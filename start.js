const { spawn } = require('child_process');

console.log('🚀 Starting MTN GroupShare services...');
console.log(`🌐 Public PORT: ${process.env.PORT || 'not set'}`);
console.log(`📞 OTP PORT: ${process.env.OTP_PORT || '6060'}`);

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
    env: { ...process.env }
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