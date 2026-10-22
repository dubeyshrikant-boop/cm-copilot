# CM Co-Pilot · Cloudflare Worker

The page (`../index.html`) sends each brief request here; the worker calls Google Gemini
with the `GEMINI_API_KEY` secret and streams the answer back.

Deploy after editing (run inside this `worker/` folder):

```bash
npx wrangler@4 deploy
```

The Gemini key is a Cloudflare secret — it is **not** in this repo. To change it:

```bash
npx wrangler@4 secret put GEMINI_API_KEY
```
