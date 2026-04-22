const express = require('express');
const ngrok = require('@ngrok/ngrok');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

let otpResolve = null;
let otpTimer = null;

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
    console.log('⚠️  OTP received but no script is currently waiting — discarding to avoid stale reuse');
    return res.json({ received: true, note: 'No script waiting — OTP discarded' });
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
function resetOtpState() {
  if (otpTimer) {
    clearTimeout(otpTimer);
    otpTimer = null;
  }
  otpResolve = null;
}

function waitForOTP(timeoutMs = 180000) {
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

  // ERR_NGROK_334 = domain already online (previous container still alive during Render deploy).
  // Retry for up to ~3 minutes — Render terminates the old instance within ~60s.
  const ENDPOINT_IN_USE = 'ERR_NGROK_334';
  let retries = 0;

  while (true) {
    const isEndpointInUse = (err) => err.message.includes(ENDPOINT_IN_USE);

    try {
      const session = await new ngrok.SessionBuilder()
        .authtoken(process.env.NGROK_AUTHTOKEN)
        .connect();

      const endpoint = session.httpEndpoint();
      if (process.env.NGROK_DOMAIN) endpoint.domain(process.env.NGROK_DOMAIN);
      const listener = await endpoint.listenAndForward(`http://localhost:${PORT}`);

      const publicUrl = listener.url();
      console.log(`🌍 ngrok tunnel active: ${publicUrl}`);
      console.log(`📡 Configure your SMS forwarder to POST to: ${publicUrl}/otp`);
      console.log(`🔍 Health check: ${publicUrl}/`);
      return;

    } catch (err) {
      retries++;

      if (isEndpointInUse(err)) {
        // Previous deployment is still holding the tunnel — keep waiting
        const maxWaitRetries = 12; // 12 × 15s = 3 minutes
        if (retries <= maxWaitRetries) {
          console.warn(`⏳ ngrok: endpoint in use by previous deploy — waiting 15s (attempt ${retries}/${maxWaitRetries})...`);
          await new Promise(r => setTimeout(r, 15000));
          continue;
        }
        console.error('❌ ngrok: old deployment did not release endpoint after 3 minutes.');
      } else {
        console.error(`❌ ngrok failed (attempt ${retries}): ${err.message}`);
        if (err.message.includes('authtoken') || err.message.includes('auth')) {
          console.error('🔑 Check NGROK_AUTHTOKEN matches your token at dashboard.ngrok.com');
        }
        if (err.message.includes('domain') || err.message.includes('hostname')) {
          console.error('🌐 Check NGROK_DOMAIN matches a domain claimed on your ngrok account');
        }
        // Non-retriable error — give up after 3 attempts
        if (retries < 3) {
          console.log(`⏳ Retrying ngrok in 5 seconds...`);
          await new Promise(r => setTimeout(r, 5000));
          continue;
        }
      }

      console.error('❌ ngrok failed after all retries — OTP forwarding will not work.');
      console.log('⚠️  Bot will still run but cannot receive OTP via SMS forwarder.');
      return;
    }
  }
}

module.exports = { waitForOTP, startServer, resetOtpState };