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
const bot = spawn('node', ['index.js'], {
  stdio: 'inherit',
  env: { ...process.env, PORT: process.env.OTP_PORT || '6060' }
});

bot.on('exit', (code) => {
  console.log(`⚠️  Bot exited with code ${code}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received — shutting down...');
  api.kill();
  bot.kill();
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('🛑 SIGINT received — shutting down...');
  api.kill();
  bot.kill();
  process.exit(0);
});