const { spawn } = require('child_process');
const http    = require('http');

console.log('🚀 Starting MTN GroupShare services...');
console.log(`🌐 Public PORT: ${process.env.PORT || 'not set'}`);
console.log(`📞 OTP PORT: ${process.env.OTP_PORT || '6060'}`);

// API server runs on this internal port — never exposed directly to Render.
// All public traffic goes through the proxy below.
// Override with API_INTERNAL_PORT env var if 7070 conflicts with another service.
const PUBLIC_PORT       = parseInt(process.env.PORT || '10000');
const INTERNAL_API_PORT = (() => {
  const p = parseInt(process.env.API_INTERNAL_PORT || '7070');
  if (p === PUBLIC_PORT) {
    console.error(`❌ FATAL: API_INTERNAL_PORT (${p}) must not equal PORT (${PUBLIC_PORT}). Set API_INTERNAL_PORT to a different value.`);
    process.exit(1);
  }
  return p;
})();

// ── Health-check proxy ─────────────────────────────────────────────────────────
// Runs in start.js's own event loop — completely isolated from api-server.js and
// the Playwright bot. Even under 100% CPU in the other processes, GET /health
// is served here instantly. Everything else is transparently proxied to api-server.
const proxy = http.createServer((req, res) => {
  // Answer health probes immediately — no forwarding, no dependencies.
  if (req.method === 'GET' && req.url === '/health') {
    const body = JSON.stringify({ status: 'ok', time: new Date().toISOString() });
    res.writeHead(200, {
      'Content-Type':    'application/json',
      'Cache-Control':   'no-store',
      'Content-Length':  Buffer.byteLength(body),
    });
    res.end(body);
    return;
  }

  // Forward everything else to api-server on the internal port.
  const opts = {
    hostname: '127.0.0.1',
    port:     INTERNAL_API_PORT,
    path:     req.url,
    method:   req.method,
    headers:  req.headers,
  };

  const proxyReq = http.request(opts, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res, { end: true });
  });

  proxyReq.on('error', () => {
    if (!res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: 'API server temporarily unavailable' }));
    }
  });

  req.on('error', () => proxyReq.destroy());
  req.pipe(proxyReq, { end: true });
});

// These timeouts live on the public server (the one Render’s load balancer talks to).
proxy.keepAliveTimeout = 120000; // 120 s — exceeds Render’s proxy idle timeout
proxy.headersTimeout   = 125000; // slightly above keepAliveTimeout

proxy.listen(PUBLIC_PORT, () => {
  console.log(`🔀 Proxy on port ${PUBLIC_PORT} → API on ${INTERNAL_API_PORT} (health answered locally)`);
});

proxy.on('error', (err) => console.error('❌ Proxy error:', err.message));

// Bot uses OTP_PORT internally — not exposed to the internet
let currentBot = null;
let botRestartCount = 0;
let shuttingDown = false;

// API server gets Render's public PORT — this is what the internet hits.
// It is restarted on crash so health checks never go dark (Render restarts
// the whole container when /health stops responding).
let currentApi = null;
let apiRestartCount = 0;

function spawnApi() {
  currentApi = spawn('node', ['api-server.js'], {
    stdio: 'inherit',
    // Run on the internal port — the public proxy handles port 10000.
    env: { ...process.env, PORT: String(INTERNAL_API_PORT) }
  });

  currentApi.on('exit', (code) => {
    if (shuttingDown) return;
    apiRestartCount++;
    const delay = Math.min(3000 * apiRestartCount, 15000); // max 15 s back-off
    console.log(`⚠️  API server exited (code ${code}). Restart #${apiRestartCount} in ${delay / 1000}s...`);
    setTimeout(spawnApi, delay);
  });
}

spawnApi();

function spawnBot() {
  currentBot = spawn('node', ['index.js'], {
    stdio: 'inherit',
    env: { ...process.env, PORT: process.env.OTP_PORT || '6060' }
  });

  currentBot.on('exit', (code) => {
    if (shuttingDown) return;
    botRestartCount++;
    const delay = Math.min(5000 * botRestartCount, 30000);
    console.log(`⚠️  Bot exited (code ${code}). Restart #${botRestartCount} in ${delay / 1000}s...`);
    setTimeout(spawnBot, delay);
  });
}

spawnBot();

// Graceful shutdown
process.on('SIGTERM', () => {
  shuttingDown = true;
  console.log('🛑 SIGTERM received — shutting down...');
  if (currentApi) currentApi.kill();
  if (currentBot) currentBot.kill();
  process.exit(0);
});

process.on('SIGINT', () => {
  shuttingDown = true;
  console.log('🛑 SIGINT received — shutting down...');
  if (currentApi) currentApi.kill();
  if (currentBot) currentBot.kill();
  process.exit(0);
});