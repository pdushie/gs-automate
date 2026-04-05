const express = require('express');
const ngrok = require('@ngrok/ngrok');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

let otpResolve = null;

// OTP receiver endpoint
app.post('/otp', (req, res) => {
  console.log('📩 Raw payload received:', JSON.stringify(req.body));

  const message =
    req.body?.message ||
    req.body?.msg ||
    req.body?.text ||
    req.body?.body ||
    req.body?.key ||
    JSON.stringify(req.body);

  if (!message) {
    console.log('⚠️  Empty payload received');
    return res.status(400).json({ error: 'Empty payload' });
  }

  const match = message.match(/\b\d{6,8}\b/);
  if (!match) {
    console.log('⚠️  No OTP found in message:', message);
    return res.status(400).json({ error: 'No OTP found in message' });
  }

  const otp = match[0];
  console.log(`✅ OTP extracted: ${otp}`);

  if (otpResolve) {
    otpResolve(otp);
    otpResolve = null;
    return res.json({ success: true, otp });
  } else {
    console.log('⚠️  OTP received but no script is currently waiting');
    return res.status(400).json({ error: 'No script waiting for OTP' });
  }
});

app.get('/', (req, res) => {
  res.json({ status: 'OTP receiver running', waiting: !!otpResolve });
});

// Exported function — called by index.js to wait for OTP
function waitForOTP(timeoutMs = 120000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      otpResolve = null;
      reject(new Error('OTP timeout — no OTP received within 2 minutes'));
    }, timeoutMs);

    otpResolve = (otp) => {
      clearTimeout(timer);
      resolve(otp);
    };
  });
}

// Start server and ngrok
async function startServer() {
  //const PORT = process.env.PORT || 6060;
  const PORT = process.env.OTP_PORT || 6060;
  await new Promise((resolve) => app.listen(PORT, resolve));
  console.log(`🚀 OTP receiver running at http://localhost:${PORT}`);

  try {
    const listener = await ngrok.forward({
      addr: PORT,
      authtoken: process.env.NGROK_AUTHTOKEN,
      domain: process.env.NGROK_DOMAIN,
    });
    console.log(`🌍 Public URL: ${listener.url()}`);
    console.log(`📡 Configure Zerogic to POST to: ${listener.url()}/otp`);
  } catch (err) {
    console.error('❌ ngrok failed to start:', err.message);
    console.log('⚠️  Continuing without ngrok — use localhost only');
  }
}

module.exports = { waitForOTP, startServer };