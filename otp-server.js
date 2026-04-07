const express = require('express');
const ngrok = require('@ngrok/ngrok');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

let otpResolve = null;
let otpTimer = null;
let otpBuffer = null; // { otp, receivedAt } — holds OTP that arrived before waitForOTP was called
const OTP_BUFFER_TTL_MS = 5 * 60 * 1000; // buffer valid for 5 minutes

// ── OTP RECEIVER ───────────────────────────────────────────
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

  // Only process messages that are actually MTN OTP messages
  const isMtnOtp = /MTN/i.test(message) && /OTP/i.test(message);
  if (!isMtnOtp) {
    console.log('⚠️  Ignored — not an MTN OTP message:', message);
    return res.status(200).json({ received: true, note: 'Not an MTN OTP message — ignored' });
  }

  // Extract OTP Code value specifically (e.g. "OTP Code: 41764126")
  let match = message.match(/OTP\s*Code[:\s]+(\d{6,8})/i);
  // Fallback: any standalone 6-8 digit number in the message
  if (!match) match = message.match(/\b(\d{6,8})\b/);

  if (!match) {
    console.log('⚠️  MTN OTP message received but no OTP digits found:', message);
    return res.status(400).json({ error: 'No OTP digits found in message' });
  }

  const otp = match[1];
  console.log(`✅ OTP extracted: ${otp}`);

  if (otpResolve) {
    otpResolve(otp);
    otpResolve = null;
    return res.json({ success: true, otp });
  } else {
    console.log('⚠️  OTP received but no script is currently waiting — buffering for next waitForOTP call');
    otpBuffer = { otp, receivedAt: Date.now() };
    // Return 200 so SMS forwarder does not keep retrying
    return res.json({ received: true, note: 'No script waiting — OTP buffered' });
  }
});

// ── HEALTH CHECK ───────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({
    status: 'OTP receiver running',
    waiting: !!otpResolve,
    time: new Date().toISOString(),
  });
});

// ── WAIT FOR OTP ───────────────────────────────────────────
function resetOtpState(clearBuffer = false) {
  if (otpTimer) {
    clearTimeout(otpTimer);
    otpTimer = null;
  }
  otpResolve = null;
  if (clearBuffer) otpBuffer = null;
}

function waitForOTP(timeoutMs = 180000) {
  // If an OTP arrived recently while no waiter was registered, use it immediately
  if (otpBuffer && (Date.now() - otpBuffer.receivedAt) < OTP_BUFFER_TTL_MS) {
    const buffered = otpBuffer.otp;
    otpBuffer = null;
    console.log(`✅ Using buffered OTP: ${buffered}`);
    resetOtpState();
    return Promise.resolve(buffered);
  }

  // Clear any lingering state from a previous timed-out or failed call
  resetOtpState();

  return new Promise((resolve, reject) => {
    otpTimer = setTimeout(() => {
      otpResolve = null;
      otpTimer = null;
      reject(new Error('OTP timeout — no OTP received within the timeout period'));
    }, timeoutMs);

    otpResolve = (otp) => {
      clearTimeout(otpTimer);
      otpTimer = null;
      resolve(otp);
    };
  });
}

// ── START SERVER + NGROK ───────────────────────────────────
async function startServer() {
  // On Render Web Service, PORT is injected by Render (usually 10000).
  // On Background Worker or locally, fall back to OTP_PORT or 6060.
  // OTP server must NOT use process.env.PORT — that belongs to api-server.js
const PORT = process.env.OTP_PORT || 6060;

  await new Promise((resolve) => app.listen(PORT, '0.0.0.0', resolve));
  console.log(`🚀 OTP receiver running at http://0.0.0.0:${PORT}`);

  // Fail loudly if NGROK_AUTHTOKEN is missing — no more silent failures
  if (!process.env.NGROK_AUTHTOKEN) {
    console.error('❌ NGROK_AUTHTOKEN is not set — ngrok will not start.');
    console.error('   → Render dashboard → Environment → add NGROK_AUTHTOKEN');
    console.log(`📡 OTP endpoint (local only): http://localhost:${PORT}/otp`);
    return;
  }

  if (!process.env.NGROK_DOMAIN) {
    console.warn('⚠️  NGROK_DOMAIN is not set — ngrok will use a random URL each restart.');
    console.warn('   → Set NGROK_DOMAIN to your static free domain for a consistent URL.');
  }

  let retries = 0;
  const maxRetries = 3;

  while (retries < maxRetries) {
    try {
      const listener = await ngrok.forward({
        addr: PORT,
        authtoken: process.env.NGROK_AUTHTOKEN,
        domain: process.env.NGROK_DOMAIN || undefined,
      });

      const publicUrl = listener.url();
      console.log(`🌍 ngrok tunnel active: ${publicUrl}`);
      console.log(`📡 Configure your SMS forwarder to POST to: ${publicUrl}/otp`);
      console.log(`🔍 Health check: ${publicUrl}/`);
      return;

    } catch (err) {
      retries++;
      console.error(`❌ ngrok failed (attempt ${retries}/${maxRetries}): ${err.message}`);

      if (err.message.includes('authtoken') || err.message.includes('auth')) {
        console.error('🔑 Check NGROK_AUTHTOKEN matches your token at dashboard.ngrok.com');
      }
      if (err.message.includes('domain') || err.message.includes('hostname')) {
        console.error('🌐 Check NGROK_DOMAIN matches a domain claimed on your ngrok account');
      }

      if (retries < maxRetries) {
        console.log(`⏳ Retrying ngrok in 5 seconds...`);
        await new Promise(r => setTimeout(r, 5000));
      } else {
        console.error('❌ ngrok failed after all retries — OTP forwarding will not work.');
        console.log('⚠️  Bot will still run but cannot receive OTP via SMS forwarder.');
      }
    }
  }
}

module.exports = { waitForOTP, startServer, resetOtpState };