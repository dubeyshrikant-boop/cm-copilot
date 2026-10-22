/* ─────────────────────────────────────────────────────────────────────
   CM Co-Pilot · Cloudflare Worker Proxy (Google Gemini)

   This worker sits between the public HTML page and Google's Gemini API.
   It holds your Gemini API key as a secret so the key is never exposed.

   Speed:
   - Streams the brief to the page as Gemini writes it (first text in ~1–2 s)
   - Thinking kept to the minimum — JSON briefs don't need long reasoning
   - JSON mode — Gemini always returns valid JSON

   Reliability:
   - "High demand" (429/500/503) → retry, then fall back to the next model

   Safety:
   - Only the CM Co-Pilot site may call it (Origin check + CORS)
   - Per-visitor rate limit, input size cap

   Secrets:  GEMINI_API_KEY   (wrangler secret put GEMINI_API_KEY)

   Quota (checked 22-10-2026): a FREE Gemini key allows only 20 gemini-2.5-flash
   requests per day; after that the backup models are used (slower, often busy).
   A paid (billing-enabled) key removes the daily cap and gets priority.
   ───────────────────────────────────────────────────────────────────── */

const ALLOWED_ORIGINS = [
  'https://dubeyshrikant-boop.github.io',
];
// Local testing (python -m http.server / wrangler dev)
const LOCAL_ORIGIN = /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/;

// Tried in order. On "high demand" / quota errors the next model is used.
// thinking: Gemini 2.5 takes thinkingBudget, Gemini 3.x takes thinkingLevel.
// Measured 22-10-2026 on a Marathi speech brief: 2.5 Flash (no thinking) first text 1–2 s,
// complete 5–6 s; 3.5/3.8 Flash were 9–26 s and often "high demand". Flash-Lite has its own quota.
const MODELS = [
  { id: 'gemini-2.5-flash', thinking: { thinkingBudget: 0 } },
  { id: 'gemini-3.1-flash-lite', thinking: { thinkingLevel: 'minimal' } },
  { id: 'gemini-3.5-flash', thinking: { thinkingLevel: 'low' } },
];

const MAX_PROMPT_CHARS = 20000;
const MAX_OUTPUT_TOKENS = 8192;   // Marathi (Devanagari) needs ~3× the tokens of English
const RETRYABLE = new Set([429, 500, 502, 503, 504]);
const ERROR_MARK = '\n[[ENGINE_ERROR]]';  // written mid-stream if Gemini fails after text has started

const json = (body, status, headers) =>
  new Response(JSON.stringify(body), { status, headers: { ...headers, 'Content-Type': 'application/json' } });

const sleep = ms => new Promise(r => setTimeout(r, ms));

function corsFor(origin) {
  const allowed = ALLOWED_ORIGINS.includes(origin) || LOCAL_ORIGIN.test(origin || '');
  return {
    allowed,
    headers: {
      'Access-Control-Allow-Origin': allowed ? origin : ALLOWED_ORIGINS[0],
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Expose-Headers': 'X-Engine-Model',
      'Access-Control-Max-Age': '86400',
      'Vary': 'Origin',
    },
  };
}

// Opens a streaming Gemini request; retries and falls back across MODELS.
// Returns { response, model } with a 200 SSE response, or { error, status }.
async function openGeminiStream(env, prompt, models) {
  let lastError = 'unknown', lastStatus = 502;
  for (const model of models) {
    for (let attempt = 0; attempt < 2; attempt++) {
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${model.id}:streamGenerateContent?alt=sse`;
      let res;
      try {
        res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-goog-api-key': env.GEMINI_API_KEY },
          body: JSON.stringify({
            contents: [{ role: 'user', parts: [{ text: prompt }] }],
            generationConfig: {
              maxOutputTokens: MAX_OUTPUT_TOKENS,
              temperature: 0.4,
              responseMimeType: 'application/json',
              thinkingConfig: model.thinking,
            },
            safetySettings: [
              { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'BLOCK_ONLY_HIGH' },
              { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'BLOCK_ONLY_HIGH' },
              { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'BLOCK_ONLY_HIGH' },
              { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'BLOCK_ONLY_HIGH' },
            ],
          }),
        });
      } catch (e) {
        lastError = e.message; lastStatus = 502;
        await sleep(400);
        continue;
      }
      if (res.ok) return { response: res, model: model.id };

      lastStatus = res.status;
      lastError = (await res.text()).slice(0, 2000);
      console.log(`gemini ${model.id} attempt ${attempt + 1}: HTTP ${res.status} ${lastError.replace(/\s+/g, " ").slice(0, 1200)}`);
      if (!RETRYABLE.has(res.status)) break;          // e.g. 400 for this model → try next model
      if (res.status === 429) break;                  // quota for this model used up → next model now
      await sleep(attempt === 0 ? 600 : 1500);
    }
  }
  return { error: lastError, status: lastStatus };
}

// Gemini SSE → plain UTF-8 text of the answer (thought parts skipped).
// If Gemini's stream ends without a finishReason (connection cut mid-answer),
// ERROR_MARK is appended so the page knows to retry instead of showing half a brief.
function geminiSseToText(body) {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = '';
  let finishReason = null;
  const emit = (line, controller) => {
    if (!line.startsWith('data:')) return;
    const payload = line.slice(5).trim();
    if (!payload) return;
    const data = JSON.parse(payload);
    const candidate = data.candidates?.[0];
    for (const part of candidate?.content?.parts || []) {
      if (part.text && !part.thought) controller.enqueue(encoder.encode(part.text));
    }
    if (candidate?.finishReason) finishReason = candidate.finishReason;
  };
  return body.pipeThrough(new TransformStream({
    transform(chunk, controller) {
      buffer += decoder.decode(chunk, { stream: true });
      let nl;
      while ((nl = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, nl).replace(/\r$/, '');
        buffer = buffer.slice(nl + 1);
        emit(line, controller);
      }
    },
    flush(controller) {
      buffer += decoder.decode();
      if (buffer.trim()) emit(buffer.trim(), controller);
      if (!finishReason) {
        console.log('gemini stream ended without finishReason');
        controller.enqueue(encoder.encode(ERROR_MARK));
      } else if (finishReason !== 'STOP' && finishReason !== 'MAX_TOKENS') {
        console.log('gemini finishReason ' + finishReason);
        controller.enqueue(encoder.encode(ERROR_MARK));
      }
    },
  }));
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin');
    const cors = corsFor(origin);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: cors.allowed ? 204 : 403, headers: cors.headers });
    }
    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405, headers: cors.headers });
    }
    if (!cors.allowed) {
      return json({ error: 'Origin not allowed' }, 403, cors.headers);
    }
    if (!env.GEMINI_API_KEY) {
      return json({ error: 'GEMINI_API_KEY not configured' }, 500, cors.headers);
    }
    if (env.LIMITER) {
      const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
      const { success } = await env.LIMITER.limit({ key: ip });
      if (!success) return json({ error: 'खूप जास्त विनंत्या — एक मिनिटाने पुन्हा प्रयत्न करा' }, 429, cors.headers);
    }

    let incoming;
    try {
      incoming = await request.json();
    } catch {
      return json({ error: 'Invalid JSON body' }, 400, cors.headers);
    }
    const prompt = incoming.messages?.[0]?.content || incoming.prompt || '';
    if (!prompt) return json({ error: 'Empty prompt' }, 400, cors.headers);
    if (prompt.length > MAX_PROMPT_CHARS) return json({ error: 'Input too long' }, 413, cors.headers);

    // Local benchmarking only: `wrangler dev --var BENCH_MODEL:<id>` pins one model.
    const models = env.BENCH_MODEL
      ? [MODELS.find(m => m.id === env.BENCH_MODEL) || { id: env.BENCH_MODEL, thinking: { thinkingLevel: 'low' } }]
      : MODELS;

    const opened = await openGeminiStream(env, prompt, models);
    if (opened.error) {
      const busy = opened.status === 429 || opened.status === 503;
      return json({ error: busy ? 'Engine busy — please retry in a moment' : 'Gemini error: ' + opened.error },
        busy ? 503 : 502, cors.headers);
    }

    const text = geminiSseToText(opened.response.body);

    // New page: stream plain text as it is written.
    if (incoming.stream) {
      const { readable, writable } = new TransformStream();
      (async () => {
        const writer = writable.getWriter();
        const reader = text.getReader();
        try {
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            await writer.write(value);
          }
        } catch (e) {
          console.log('stream error: ' + e.message);
          await writer.write(new TextEncoder().encode(ERROR_MARK));
        } finally {
          await writer.close();
        }
      })();
      return new Response(readable, {
        status: 200,
        headers: {
          ...cors.headers,
          'Content-Type': 'text/plain; charset=utf-8',
          'Cache-Control': 'no-store',
          'X-Engine-Model': opened.model,
        },
      });
    }

    // Older cached copies of the page: same response shape as before.
    try {
      const full = await new Response(text).text();
      return json({ content: [{ type: 'text', text: full }] }, 200, { ...cors.headers, 'X-Engine-Model': opened.model });
    } catch (err) {
      return json({ error: 'Proxy error: ' + err.message }, 500, cors.headers);
    }
  },
};
