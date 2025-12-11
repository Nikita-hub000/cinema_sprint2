const express = require('express');
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 8000;
const MONOLITH_URL = process.env.MONOLITH_URL || 'http://monolith:8080';
const MOVIES_SERVICE_URL =
  process.env.MOVIES_SERVICE_URL || 'http://movies-service:8081';
const GRADUAL_MIGRATION =
  (process.env.GRADUAL_MIGRATION || 'false').toLowerCase() === 'true';
const MOVIES_MIGRATION_PERCENT = (() => {
  const raw = parseInt(process.env.MOVIES_MIGRATION_PERCENT || '0', 10);
  if (!Number.isFinite(raw)) return 0;
  return Math.min(Math.max(raw, 0), 100);
})();

function chooseMoviesBackend() {
  if (!GRADUAL_MIGRATION) return MONOLITH_URL;

  const pct = MOVIES_MIGRATION_PERCENT;
  const rnd = Math.random() * 100;
  return rnd < pct ? MOVIES_SERVICE_URL : MONOLITH_URL;
}

async function proxyRequest(req, res, targetBase) {
  try {
    const target = new URL(req.originalUrl, targetBase).toString();

    const headers = { ...req.headers };
    delete headers['content-length'];
    delete headers['Content-Length'];
    delete headers['host'];
    delete headers['Host'];

    const fetchOptions = {
      method: req.method,
      headers,
      body: ['GET', 'HEAD'].includes(req.method)
        ? undefined
        : JSON.stringify(req.body),
    };

    const resp = await fetch(target, fetchOptions);

    resp.headers.forEach((value, name) => {
      if (['transfer-encoding', 'content-encoding'].includes(name)) return;
      res.setHeader(name, value);
    });

    res.status(resp.status);

    const contentType = resp.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      const data = await resp.json();
      return res.json(data);
    }

    const buf = await resp.arrayBuffer();
    return res.send(Buffer.from(buf));
  } catch (err) {
    console.error('Proxy error:', err);
    return res.status(502).json({ error: 'Bad gateway', detail: String(err) });
  }
}

app.all('/api/movies', async (req, res) => {
  const target = chooseMoviesBackend();
  await proxyRequest(req, res, target);
});

app.all('/api/users', async (req, res) => {
  await proxyRequest(req, res, MONOLITH_URL);
});

app.all('/api/payments', async (req, res) => {
  await proxyRequest(req, res, MONOLITH_URL);
});

app.all('/api/subscriptions', async (req, res) => {
  await proxyRequest(req, res, MONOLITH_URL);
});

app.all('/api/movies/health', async (req, res) => {
  const target = MOVIES_SERVICE_URL;
  console.log(`Proxying ${req.method} ${req.originalUrl} -> ${target}`);
  await proxyRequest(req, res, target);
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'proxy' });
});

app.listen(PORT, () => console.log('proxy server started on port', PORT));
