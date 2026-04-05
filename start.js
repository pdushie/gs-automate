const { execSync, spawn } = require('child_process');

console.log('🚀 Starting MTN GroupShare services...');

// Start API server
const api = spawn('node', ['api-server.js'], { stdio: 'inherit' });
api.on('exit', (code) => console.log(`API server exited with code ${code}`));

// Start bot
const bot = spawn('node', ['index.js'], { stdio: 'inherit' });
bot.on('exit', (code) => console.log(`Bot exited with code ${code}`));

process.on('SIGTERM', () => {
  api.kill();
  bot.kill();
  process.exit(0);
});