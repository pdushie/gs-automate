const { spawn } = require('child_process');

console.log('🚀 Starting MTN GroupShare services...');
console.log(`🌐 Public PORT: ${process.env.PORT || 'not set'}`);
console.log(`📞 OTP PORT: ${process.env.OTP_PORT || '6060'}`);

// API server gets Render's public PORT — this is what the internet hits
const api = spawn('node', ['api-server.js'], {
  stdio: 'inherit',
  env: { ...process.env }
});

api.on('exit', (code) => {
  console.log(`⚠️  API server exited with code ${code}`);
});

// Bot uses OTP_PORT internally — not exposed to the internet
let currentBot = null;
let botRestartCount = 0;
let shuttingDown = false;

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
  api.kill();
  if (currentBot) currentBot.kill();
  process.exit(0);
});

process.on('SIGINT', () => {
  shuttingDown = true;
  console.log('🛑 SIGINT received — shutting down...');
  api.kill();
  if (currentBot) currentBot.kill();
  process.exit(0);
});