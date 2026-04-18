const { chromium } = require('playwright');
const { waitForOTP, startServer, resetOtpState } = require('./otp-server');
const { withFileLock, atomicWrite } = require('./lock');
const TelegramBot = require('node-telegram-bot-api');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
const notifier = require('node-notifier');
require('dotenv').config();

const tgBot = process.env.TELEGRAM_BOT_TOKEN
  ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false })
  : null;

const tgBot2 = process.env.TELEGRAM_BOT_TOKEN_2
  ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN_2, { polling: false })
  : null;

function escapeHtml(str) {
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

const UPLOADED_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.uploaded.json');
const IDLE_REFRESH_INTERVAL = 1 * 60 * 1000;

const STATUS_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.status.json');
const MAX_FILE_RETRIES = 3;

function loadStatusLog() {
  try {
    if (fs.existsSync(STATUS_LOG)) return JSON.parse(fs.readFileSync(STATUS_LOG, 'utf8'));
  } catch {
    // Retry once — may have caught the file mid-rename during an atomic write
    try {
      if (fs.existsSync(STATUS_LOG)) return JSON.parse(fs.readFileSync(STATUS_LOG, 'utf8'));
    } catch {}
  }
  return {};
}

function updateStatusLog(updates) {
  withFileLock(STATUS_LOG, () => {
    const log = loadStatusLog();
    Object.assign(log, updates);
    atomicWrite(STATUS_LOG, JSON.stringify(log, null, 2));
  });
}

function sendAlert(title, message) {
  console.warn(`🔔 ALERT: ${title} — ${message}`);
  notifier.notify({ title, message, sound: true, wait: false });

  const text = `🔔 <b>${escapeHtml(title)}</b>\n${escapeHtml(message)}`;
  const recipients = [
    { bot: tgBot,  chatId: process.env.TELEGRAM_CHAT_ID },
    { bot: tgBot2, chatId: process.env.TELEGRAM_CHAT_ID_2 },
  ];

  for (const { bot, chatId } of recipients) {
    if (!bot || !chatId) continue;
    const trySend = (attempt) =>
      bot.sendMessage(chatId, text, { parse_mode: 'HTML' })
        .then(() => {
          if (attempt > 1) console.log(`✅ Telegram alert sent on retry ${attempt}`);
        })
        .catch(err => {
          const detail = err.code ? `${err.code}: ${err.message}` : err.message;
          if (attempt < 3) {
            console.warn(`⚠️  Telegram alert failed (attempt ${attempt}): ${detail} — retrying in 10s...`);
            setTimeout(() => trySend(attempt + 1), 10000);
          } else {
            console.error(`❌ Telegram alert failed after ${attempt} attempts: ${detail}`);
          }
        });
    trySend(1);
  }
}

// Strip the server-added timestamp suffix from a filename before sending callback.
// e.g. "MyFile-2026-04-07T00-32-29-843Z.xlsx" → "MyFile.xlsx"
function stripTimestamp(filename) {
  const ext = path.extname(filename);
  const base = path.basename(filename, ext);
  // Matches "-<ISO8601-like timestamp>" appended by api-server storage naming
  const stripped = base.replace(/-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d+Z$/, '');
  return stripped + ext;
}

async function sendCallback(filename, status, completedAt) {
  const orderSystemUrl = process.env.ORDERSYSTEM_URL;
  const secret = process.env.GROUPSHARE_CALLBACK_SECRET;

  if (!orderSystemUrl) {
    console.log('ℹ️  ORDERSYSTEM_URL not set — skipping callback');
    return;
  }
  if (!secret) {
    console.warn('⚠️  GROUPSHARE_CALLBACK_SECRET not set — skipping callback');
    return;
  }

  // Retrieve stored order reference(s) for this file
  const statusLog = loadStatusLog();
  const payload = { filename, status, completedAt };
  if (statusLog[`${filename}_orderIds`]) {
    payload.orderIds = statusLog[`${filename}_orderIds`];
  } else if (statusLog[`${filename}_orderId`]) {
    payload.orderId = statusLog[`${filename}_orderId`];
  }

  const url = `${orderSystemUrl.replace(/\/$/, '')}/api/groupshare/callback?secret=${encodeURIComponent(secret)}`;
  const body = JSON.stringify(payload);

  console.log(`📡 Sending callback for "${filename}" (${status}) to ${orderSystemUrl}...`);

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    });
    if (res.ok) {
      console.log(`📤 Callback sent for "${filename}" (${status}) — HTTP ${res.status}`);
    } else {
      const text = await res.text().catch(() => '');
      console.warn(`⚠️  Callback for "${filename}" returned HTTP ${res.status}: ${text}`);
    }
  } catch (err) {
    console.error(`❌ Callback failed for "${filename}": ${err.message}`);
  }
}

// Fire-and-forget: send `load_312` to ntfy.sh to kick off the airtime loading sequence.
// Respects the _airtimeEnabled flag and the configured UTC time window in the status log.
async function triggerAirtimeLoad(page, context) {
  const sl = loadStatusLog();
  if (!sl._airtimeEnabled) return;

  // Time-window check using server local time (server must be set to GMT+0 / UTC).
  // Stored as minutes since midnight; legacy whole-hour values ≤24 are auto-multiplied.
  const _now    = new Date();
  const nowMins = _now.getHours() * 60 + _now.getMinutes();
  const normWin  = v => (v == null ? null : v <= 24 ? v * 60 : v);
  const winStart = normWin(sl._airtimeWindowStart) ?? 0;
  const winEnd   = normWin(sl._airtimeWindowEnd)   ?? 1440;
  const inWindow = winStart <= winEnd
    ? (nowMins >= winStart && nowMins < winEnd)
    : (nowMins >= winStart || nowMins < winEnd);
  const fmtMins  = m => `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;

  if (!inWindow) {
    console.log(`⏰ Airtime load skipped — outside time window (${fmtMins(winStart)}–${fmtMins(winEnd)}, now ${fmtMins(nowMins)})`);
    return;
  }

  const NTFY_URL     = (process.env.NTFY_URL || 'https://ntfy.sh').replace(/\/$/, '') + '/clickyfiedloader_5';
  const TOLERANCE    = 50;   // GH¢ shortfall allowed when verifying increase
  const MAX_RETRIES  = 2;    // extra attempts per stage if balance doesn’t move
  const LOAD500_TARGET = 4500;
  const LOAD500_MAX_ROUNDS = 9;

  async function sendNtfy(cmd) {
    const r = await fetch(NTFY_URL, { method: 'PUT', body: cmd });
    if (!r.ok) throw new Error(`ntfy returned HTTP ${r.status}`);
  }

  async function getPortalBalance() {
    const { accountBalance } = await checkBalance(page, context);
    return accountBalance;
  }

  // ── load_312 ─────────────────────────────────────────────────────────
  console.log('\n💰 Airtime load starting — sending load_312…');
  updateStatusLog({
    _airtimeStage:           'load_312_sent',
    _airtimeTriggeredAt:     new Date().toISOString(),
    _airtimeLoad500Count:    0,
    _airtimeLoad500TotalAdded: 0,
    _airtimeLoad312Verified: false,
  });

  let balBefore312   = await getPortalBalance();
  let load312Success = false;

  for (let attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
    try {
      console.log(`📲 load_312 attempt ${attempt}/${MAX_RETRIES + 1}…`);
      await sendNtfy('load_312');
    } catch (err) {
      console.error(`❌ load_312 ntfy failed: ${err.message}`);
      break;
    }

    console.log('⏳ Waiting 35s for load_312 to process…');
    await new Promise(r => setTimeout(r, 35000));

    const balAfter  = await getPortalBalance();
    if (balBefore312 == null || balAfter == null) {
      console.warn(`⚠️  load_312 attempt ${attempt}: balance unavailable (before=${balBefore312}, after=${balAfter}) — assuming success`);
      updateStatusLog({ _airtimeStage: 'load_312_verified', _airtimeLoad312Verified: true });
      load312Success = true;
      break;
    }
    const credited  = balAfter - balBefore312;
    console.log(`📊 load_312 attempt ${attempt}: portal balance ${balBefore312} → ${balAfter} (credited GH¢ ${credited.toFixed(2)})`);

    if (credited >= 312 - TOLERANCE) {
      console.log(`✅ load_312 verified: GH¢ ${credited.toFixed(2)} received`);
      updateStatusLog({ _airtimeStage: 'load_312_verified', _airtimeLoad312Verified: true });
      load312Success = true;
      break;
    }

    if (attempt <= MAX_RETRIES) {
      console.warn(`⚠️  load_312 attempt ${attempt} insufficient (GH¢ ${credited.toFixed(2)}) — retrying…`);
      balBefore312 = balAfter; // use latest balance as baseline for next attempt
    }
  }

  if (!load312Success) {
    console.warn('⚠️  load_312 not confirmed after all attempts — proceeding to load_500 anyway');
    updateStatusLog({ _airtimeStage: 'load_312_verified', _airtimeLoad312Verified: false });
  }

  // Check total increase after load_312 — may already be enough
  const afterLoad312Inc = await totalIncrease();
  if (afterLoad312Inc != null && afterLoad312Inc >= TOTAL_TARGET - TOLERANCE) {
    console.log(`✅ Total target reached after load_312 (GH¢ ${afterLoad312Inc.toFixed(2)}) — skipping load_500`);
    updateStatusLog({ _airtimeStage: 'done', _airtimeLoad500TotalAdded: afterLoad312Inc });
    sendAlert('🎉 MTN GroupShare — Airtime Loaded', `GH¢ ${afterLoad312Inc.toFixed(2)} added to portal`);
    return;
  }

  // ── load_500 loop ──────────────────────────────────────────────────
  let totalAdded = 0;
  let round      = 0;

  while (round < LOAD500_MAX_ROUNDS) {
    // Check total increase from initial baseline before each round
    const incNow = await totalIncrease();
    if (incNow != null && incNow >= TOTAL_TARGET - TOLERANCE) {
      console.log(`✅ Total target reached (GH¢ ${incNow.toFixed(2)} increase) — stopping load_500`);
      totalAdded = incNow;
      break;
    }
    round++;

    console.log(`\n⏳ Waiting 30s before load_500 round ${round}…`);
    await new Promise(r => setTimeout(r, 30000));

    let balBefore500  = await getPortalBalance();
    let roundSuccess  = false;

    for (let attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
      try {
        console.log(`📲 load_500 round ${round}/${LOAD500_MAX_ROUNDS} attempt ${attempt}/${MAX_RETRIES + 1}…`);
        updateStatusLog({ _airtimeStage: 'load_500_sent', _airtimeLoad500SentCount: round });
        await sendNtfy('load_500');
      } catch (err) {
        console.error(`❌ load_500 round ${round} ntfy failed: ${err.message}`);
        break;
      }

      console.log('⏳ Waiting 35s for load_500 to process…');
      await new Promise(r => setTimeout(r, 35000));

      const balAfter = await getPortalBalance();
      if (balBefore500 == null || balAfter == null) {
        console.warn(`⚠️  load_500 round ${round} attempt ${attempt}: balance unavailable (before=${balBefore500}, after=${balAfter}) — assuming success`);
        totalAdded += 500;
        updateStatusLog({
          _airtimeStage:            'load_500_processing',
          _airtimeLoad500Count:     round,
          _airtimeLoad500TotalAdded: totalAdded,
        });
        roundSuccess = true;
        break;
      }
      const credited = balAfter - balBefore500;
      console.log(`📊 load_500 round ${round} attempt ${attempt}: ${balBefore500} → ${balAfter} (credited GH¢ ${credited.toFixed(2)})`);

      if (credited >= 500 - TOLERANCE) {
        totalAdded += credited;
        const incFromBase = await totalIncrease();
        console.log(`✅ load_500 round ${round} verified: GH¢ ${credited.toFixed(2)} received — total increase from baseline: GH¢ ${incFromBase != null ? incFromBase.toFixed(2) : totalAdded.toFixed(2)}`);
        updateStatusLog({
          _airtimeStage:             'load_500_processing',
          _airtimeLoad500Count:      round,
          _airtimeLoad500TotalAdded: totalAdded,
        });
        roundSuccess = true;
        break;
      }

      if (attempt <= MAX_RETRIES) {
        console.warn(`⚠️  load_500 round ${round} attempt ${attempt} insufficient (GH¢ ${credited.toFixed(2)}) — retrying…`);
        balBefore500 = balAfter;
      }
    }

    if (!roundSuccess) {
      console.warn(`⚠️  load_500 round ${round} not confirmed after all attempts — skipping round`);
      updateStatusLog({ _airtimeLoad500Count: round });
    }
  }

  // ── Summary ───────────────────────────────────────────────────────────
  // Use real total increase from initial baseline for final accounting
  const finalIncrease = (await totalIncrease()) ?? totalAdded;
  updateStatusLog({ _airtimeStage: 'done', _airtimeLoad500TotalAdded: finalIncrease, _airtimeLoad500Count: round });

  if (finalIncrease >= TOTAL_TARGET - TOLERANCE) {
    console.log(`🎉 Airtime load complete! GH¢ ${finalIncrease.toFixed(2)} total increase in ${round} round(s)`);
    sendAlert('🎉 MTN GroupShare — Airtime Loaded', `GH¢ ${finalIncrease.toFixed(2)} added to portal in ${round} load_500 round(s)`);
  } else {
    const msg = `GH¢ ${finalIncrease.toFixed(2)} / GH¢ ${TOTAL_TARGET} increase after ${round} round(s)`;
    console.warn(`⚠️  Airtime load incomplete: ${msg}`);
    sendAlert('⚠️ MTN GroupShare — Airtime Load Incomplete', msg);
  }
}

// Sleep for `ms` ms, but wake every `checkIntervalMs` to check for a balance refresh,
// purchase request, or newly received file. Returns true if woken early, false if full duration elapsed.
async function interruptibleSleep(ms, checkIntervalMs = 15000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const remaining = end - Date.now();
    await new Promise(r => setTimeout(r, Math.min(checkIntervalMs, remaining)));
    const log = loadStatusLog();
    if (log._balanceRefreshRequested || log._purchaseRequested || log._fileReceived) return true;
  }
  return false;
}

function loadUploadedLog() {
  try {
    if (fs.existsSync(UPLOADED_LOG)) return JSON.parse(fs.readFileSync(UPLOADED_LOG, 'utf8'));
  } catch {
    try {
      if (fs.existsSync(UPLOADED_LOG)) return JSON.parse(fs.readFileSync(UPLOADED_LOG, 'utf8'));
    } catch {}
  }
  return [];
}

function markAsUploaded(fileName) {
  withFileLock(UPLOADED_LOG, () => {
    const log = loadUploadedLog();
    if (!log.includes(fileName)) {
      log.push(fileName);
      atomicWrite(UPLOADED_LOG, JSON.stringify(log, null, 2));
      console.log(`📝 Marked as uploaded: ${fileName}`);
    }
  });
}

function getPendingFiles(folderPath) {
  if (!folderPath) throw new Error('EXCEL_FOLDER_PATH is not set in your .env file');
  if (!fs.existsSync(folderPath)) throw new Error(`Folder not found: ${folderPath}`);

  const uploaded = loadUploadedLog();
  const statusLog = loadStatusLog();
  const files = fs.readdirSync(folderPath)
    .filter(f => {
      if (!f.endsWith('.xlsx') && !f.endsWith('.xls')) return false;
      if (uploaded.includes(f)) return false;
      if (statusLog[f] === 'ABANDONED') return false;
      return true;
    })
    .map(f => ({
      name: f,
      fullPath: path.join(folderPath, f),
      mtime: fs.statSync(path.join(folderPath, f)).mtime,
    }))
    .sort((a, b) => a.mtime - b.mtime);

  return files;
}

function parseBalanceToMB(balanceText) {
  if (!balanceText || balanceText === 'Unknown') return 0;

  let totalMB = 0;

  const units = {
    TB: 1024 * 1024,
    GB: 1024,
    MB: 1,
    KB: 1 / 1024,
  };

  const regex = /([\d,]+\.?\d*)\s*(TB|GB|MB|KB)/gi;
  let match;

  while ((match = regex.exec(balanceText)) !== null) {
    const value = parseFloat(match[1].replace(/,/g, ''));
    const unit = match[2].toUpperCase();
    totalMB += value * (units[unit] || 0);
  }

  return Math.round(totalMB);
}

async function isSessionActive(page) {
  try {
    await page.goto('https://up2u.mtn.com.gh', { waitUntil: 'networkidle', timeout: 15000 });
    const currentUrl = page.url();

    if (currentUrl.includes('/account/login') || currentUrl.includes('/account/verify-otp')) {
      console.log('🔒 Session expired — redirected to:', currentUrl);
      return false;
    }

    const balanceEl = await page.$('h3[data-bind*="DataVolume"]');
    if (!balanceEl) {
      console.log('🔒 Session expired — balance element not found');
      return false;
    }

    return true;
  } catch (err) {
    console.log('🔒 Session check failed:', err.message);
    return false;
  }
}

async function login(page) {
  const maxSubmitAttempts = 3;

  try {
    // ── Phase 1: Submit credentials ONCE to get to the OTP page ──────────
    await page.goto('https://up2u.mtn.com.gh/account/login', {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });

    await page.screenshot({ path: 'login-debug.png', fullPage: true });
    console.log('📸 Screenshot saved — login-debug.png');

    if (page.url().includes('/account/verify-otp')) {
      console.log('✅ Already on OTP page — skipping credential submission');
    } else {
      await page.waitForSelector('#disclaimer-btn', { timeout: 60000 });
      await page.waitForTimeout(5000);
      await page.dispatchEvent('#disclaimer-btn', 'click');
      console.log('✅ Disclaimer accepted');

      await page.waitForSelector('input[name="Msisdn"]', { timeout: 30000 });
      await page.fill('input[name="Msisdn"]', process.env.MTN_PHONE);
      await page.fill('input[name="Pin"]', process.env.MTN_PIN);
      await page.dispatchEvent('#login-btn', 'click');
      console.log('🚀 Login clicked');

      await page.waitForURL('**/account/verify-otp', { timeout: 40000 });
    }

    // ── Phase 2+3: For each attempt, wait for OTP FIRST then submit ────────
    // Each attempt waits for its own fresh OTP so we never submit a stale code.
    // On retry the buffer is cleared so we don't reuse the same rejected OTP.
    for (let attempt = 1; attempt <= maxSubmitAttempts; attempt++) {
      if (attempt === 1) {
        console.log('\n⏳ Waiting for OTP SMS (up to 10 mins)...');
      } else {
        // Discard any stale buffered OTP so we wait for a brand-new one
        resetOtpState(true);
        console.log(`\n⏳ Waiting for fresh OTP for retry ${attempt}/${maxSubmitAttempts} (up to 5 mins)...`);
      }

      const waitMs = attempt === 1 ? 10 * 60 * 1000 : 5 * 60 * 1000;
      const otp = await waitForOTP(waitMs);
      console.log(`✅ OTP received: ${otp}`);

      console.log(`\n🔑 OTP submit attempt ${attempt}/${maxSubmitAttempts}...`);
      try {
        // If a previous submission attempt navigated away, return to the OTP
        // page WITHOUT refreshing (a refresh would trigger a new OTP request)
        if (!page.url().includes('/account/verify-otp')) {
          console.log('↩️  Navigating back to OTP page (no refresh)...');
          await page.goto('https://up2u.mtn.com.gh/account/verify-otp', {
            waitUntil: 'domcontentloaded',
            timeout: 30000
          });
        }

        await page.fill('input[name="OTPCode"]', otp);

        const navigationPromise = page.waitForURL(
          url => !url.href.includes('/account/verify-otp') && !url.href.includes('/account/login'),
          { timeout: 60000, waitUntil: 'networkidle' }
        );
        await page.dispatchEvent('#login-btn', 'click');
        await navigationPromise;

        if (await isSessionActive(page)) {
          console.log('🎉 Login successful:', page.url());
          return true;
        }
        console.warn('⚠️  Session not active after OTP submit — retrying...');
      } catch (submitErr) {
        console.error(`❌ OTP submit attempt ${attempt} failed: ${submitErr.message}`);
        if (attempt < maxSubmitAttempts) {
          console.log('⏳ Waiting 5s before retry...');
          await page.waitForTimeout(5000);
        }
      }
    }

  } catch (err) {
    console.error('❌ Login failed:', err.message);
    sendAlert('❌ MTN GroupShare — Login Failed', err.message);
    throw err;
  }

  sendAlert('❌ MTN GroupShare — Login Failed', `OTP was received but login could not be completed after ${maxSubmitAttempts} submission attempts.`);
  throw new Error(`Login failed — OTP submission unsuccessful after ${maxSubmitAttempts} attempts`);
}

async function purchaseData(page, context) {
  console.log('\n💳 Starting data purchase...');

  const REQUIRED = 4812.96;

  // ── Pre-check: fetch real-time balance via API BEFORE navigating to the purchase page ──
  // This avoids false "insufficient balance" alerts caused by stale DOM values on page load.
  const { accountBalance: apiAccountBalance, accountBalanceText: apiAccountBalanceText } = await checkBalance(page, context);
  if (apiAccountBalance != null) {
    const displayText = apiAccountBalanceText || `GH¢ ${apiAccountBalance.toLocaleString()}`;
    console.log(`💰 Account balance (API pre-check): ${displayText}`);
    if (apiAccountBalance < REQUIRED) {
      const msg = `Insufficient account balance. Required: GH¢ ${REQUIRED.toLocaleString()}, Available: ${displayText}`;
      console.warn(`⚠️  ${msg}`);
      sendAlert('⚠️ MTN GroupShare — Cannot Purchase Data', msg);
      updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: msg, _purchaseCompletedAt: new Date().toISOString() });
      return false;
    }
    console.log(`✅ Account balance sufficient (API pre-check) — proceeding to purchase page`);
  } else {
    console.log('ℹ️  Account balance not available via API — will verify on purchase page');
  }

  await page.goto('https://up2u.mtn.com.gh/business/purchase-bundles', { waitUntil: 'networkidle' });
  // Reload to flush any cached balance value the page may render on first load
  await page.reload({ waitUntil: 'networkidle' });

  // Read account balance from DOM (secondary verification)
  await page.waitForSelector('h3[data-bind*="BalanceFormatted"]', { timeout: 15000 });
  const balanceText = await page.$eval('h3[data-bind*="BalanceFormatted"]', el => el.innerText.trim());
  console.log(`💰 Account balance (purchase page): ${balanceText}`);

  // Parse "GH¢ 4,822.11" → 4822.11
  const balanceMatch = balanceText.replace(/,/g, '').match(/([\d.]+)/);
  const balance = balanceMatch ? parseFloat(balanceMatch[1]) : 0;

  if (balance < REQUIRED) {
    const msg = `Insufficient account balance. Required: GH¢ ${REQUIRED.toLocaleString()}, Available: ${balanceText}`;
    console.warn(`⚠️  ${msg}`);
    sendAlert('⚠️ MTN GroupShare — Cannot Purchase Data', msg);
    updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: msg, _purchaseCompletedAt: new Date().toISOString() });
    return false;
  }
  console.log(`✅ Balance sufficient — proceeding with purchase`);

  // Set Data bundle value to 1.5 via Kendo NumericTextBox API
  await page.waitForSelector('input[name="DataBundle"]', { state: 'attached', timeout: 10000 });
  await page.evaluate(() => {
    const input = document.querySelector('input[name="DataBundle"]');
    const widget = kendo.widgetInstance(jQuery(input));
    widget.value(1.5);
    widget.trigger('change');
  });
  console.log('✅ Data bundle set to 1.5');

  // Change unit from MB → TB by clicking the Kendo DropDownList
  await page.click('span.k-input:has-text("MB")');
  await page.waitForSelector('.k-list-container .k-item:has-text("TB"), .k-popup .k-item:has-text("TB")', { timeout: 5000 });
  await page.click('.k-list-container .k-item:has-text("TB"), .k-popup .k-item:has-text("TB")');
  console.log('✅ Unit set to TB');

  // Click Calculate Package Cost
  await page.click('button.uk-button-primary:has-text("Calculate Package Cost")');
  console.log('✅ Calculate Package Cost clicked — waiting for cost table...');

  // Wait for the cost details table to populate
  await page.waitForFunction(() => {
    const rows = document.querySelectorAll('tbody[data-template="cost-details-item-template"] tr');
    return Array.from(rows).some(r => r.innerText.includes('TB'));
  }, { timeout: 15000 });

  const tableText = await page.$eval('tbody[data-template="cost-details-item-template"]', el => el.innerText);
  console.log(`📋 Cost details:\n${tableText}`);

  // Verify expected unit and amount before confirming
  if (!tableText.includes('1 TB, 512 GB') || !tableText.replace(/,/g, '').includes('4812.96')) {
    const msg = `Unexpected cost details — aborting purchase. Got: ${tableText.trim().replace(/\n/g, ' | ')}`;
    console.error(`❌ ${msg}`);
    sendAlert('❌ MTN GroupShare — Purchase Aborted', msg);
    updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: msg, _purchaseCompletedAt: new Date().toISOString() });
    return false;
  }
  console.log('✅ Cost verified: 1.5 TB (1 TB, 512 GB) @ GH¢ 4,812.96');

  // Click Complete Purchase — opens confirmation modal
  await page.click('button.uk-button-primary:has-text("Complete Purchase")');
  console.log('✅ Complete Purchase clicked — waiting for confirmation modal...');

  // Wait for modal and click its primary confirm button
  await page.waitForSelector('#confirm-purchase-modal button:has-text("I Agree")', { timeout: 10000 });
  await page.waitForTimeout(500);
  await page.click('#confirm-purchase-modal button:has-text("I Agree")');
  console.log('✅ "I Agree" clicked in confirmation modal');

  // Wait for the modal to close and the page to settle
  await page.waitForTimeout(5000);

  // Confirm purchase by re-reading the data balance — it should have increased
  const { balanceText: newBalanceText, totalMB: newBalanceMB } = await checkBalance(page, context);
  console.log(`💰 Balance after purchase: ${newBalanceText} (${newBalanceMB.toFixed(2)} MB)`);

  console.log('🎉 Data purchase complete!');
  sendAlert('🎉 MTN GroupShare — Data Purchased', `Successfully purchased 1.5 TB (1 TB, 512 GB) data bundle for GH¢ 4,812.96. New balance: ${newBalanceText}`);
  updateStatusLog({ _purchaseStatus: 'DONE', _purchaseNote: `1.5 TB (1 TB 512 GB) @ GH¢ 4,812.96 — balance after: ${newBalanceText}`, _purchaseCompletedAt: new Date().toISOString() });
  // Kick off airtime loading sequence — awaited so it runs before the purchase flow returns
  triggerAirtimeLoad(page, context).catch(err => console.error(`❌ Airtime trigger error: ${err.message}`));
  return true;
}

async function checkBalance(page, context) {
  // ── Fast path: direct API call using session cookies from Playwright context ──
  if (context) {
    try {
      const cookies = await context.cookies('https://up2u.mtn.com.gh');
      const cookieHeader = cookies.map(c => `${c.name}=${c.value}`).join('; ');

      const res = await fetch('https://up2u.mtn.com.gh/providers/api/check-balance', {
        method: 'POST',
        headers: {
          'Accept': 'application/json, text/plain, */*',
          'Content-Type': 'application/json',
          'Cookie': cookieHeader,
          'Origin': 'https://up2u.mtn.com.gh',
          'Referer': 'https://up2u.mtn.com.gh/',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        },
        body: JSON.stringify({}),
      });

      const data = await res.json();
      if (data.success && data.body && typeof data.body.DataBalanceMB === 'number') {
        const totalMB = data.body.DataBalanceMB;
        const balanceText = data.body.DataBalanceFormatted || `${(totalMB / 1024).toFixed(2)} GB`;
        // Capture main account (GHC airtime) balance from the portal API
        const accountBalance     = typeof data.body.MainAccountBalanceCedis === 'number' ? data.body.MainAccountBalanceCedis : null;
        const accountBalanceText = accountBalance != null ? `GH¢ ${accountBalance.toFixed(2)}` : null;
        console.log(`💰 Balance (API): ${balanceText} (${totalMB.toFixed(2)} MB)${
          accountBalance != null ? ` | Main Account: GH¢ ${accountBalance.toFixed(2)}` : ''
        }`);
        const statusUpdates = {
          _lastBalance: balanceText,
          _lastBalanceMB: totalMB,
          _lastBalanceCheckedAt: new Date().toISOString(),
          _portalCookieHeader: cookieHeader,  // persisted so api-server can call check-balance directly
        };
        if (accountBalance != null) {
          statusUpdates._lastAccountBalance     = accountBalance;
          statusUpdates._lastAccountBalanceText = accountBalanceText;
        }
        updateStatusLog(statusUpdates);
        return { balanceText, totalMB, accountBalance, accountBalanceText };
      }
      console.warn('⚠️ Balance API returned unexpected response — falling back to DOM scrape');
    } catch (apiErr) {
      console.warn(`⚠️ Balance API call failed: ${apiErr.message} — falling back to DOM scrape`);
    }
  }

  // ── Fallback: DOM scrape ──
  console.log('\n💰 Checking data balance (DOM)...');
  await page.goto('https://up2u.mtn.com.gh', { waitUntil: 'networkidle' });
  await page.reload({ waitUntil: 'networkidle' });
  await page.waitForSelector('h3[data-bind*="DataVolume"]', { timeout: 15000 });
  await page.waitForTimeout(2000);

  const balanceText = await page.$eval(
    'h3[data-bind*="DataVolume"]',
    el => el.innerText.trim()
  );

  const totalMB = parseBalanceToMB(balanceText);
  console.log(`💰 Balance: ${balanceText} (${totalMB.toFixed(2)} MB)`);

  updateStatusLog({
    _lastBalance: balanceText,
    _lastBalanceMB: totalMB,
    _lastBalanceCheckedAt: new Date().toISOString(),
  });

  return { balanceText, totalMB };
}

function _parseExcelTotalMB(filePath) {
  const workbook = XLSX.readFile(filePath);
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rawRows = XLSX.utils.sheet_to_json(sheet, { header: 1 });

  const headerRow = rawRows[0];
  const dataMBColIndex = 3;

  console.log(`📊 Using column: "${headerRow[dataMBColIndex]}" (column 4)`);

  let totalMB = 0;
  let rowCount = 0;

  for (let r = 1; r < rawRows.length; r++) {
    const val = parseFloat(rawRows[r][dataMBColIndex]) || 0;
    if (val > 0) {
      totalMB += val;
      rowCount++;
    }
  }

  console.log(`📊 File: ${path.basename(filePath)}`);
  console.log(`📊 Rows: ${rowCount} | Total required: ${totalMB.toFixed(2)} MB (${(totalMB / 1024).toFixed(2)} GB)`);
  return totalMB;
}

// Returns cached totalMB for a file if the file hasn't changed (mtime match),
// otherwise parses the XLSX and caches the result for next time.
function getExcelTotalMB(file) {
  const filePath = file.fullPath || file;
  const fileName = path.basename(filePath);
  const mtime = (file.mtime || fs.statSync(filePath).mtime).toISOString();

  const log = loadStatusLog();
  const cachedMtime = log[`${fileName}_totalMB_mtime`];
  if (cachedMtime === mtime && log[`${fileName}_totalMB`] != null) {
    const cached = log[`${fileName}_totalMB`];
    console.log(`📊 File: ${fileName} — using cached total: ${cached.toFixed(2)} MB (${(cached / 1024).toFixed(2)} GB)`);
    return cached;
  }

  const totalMB = _parseExcelTotalMB(filePath);
  updateStatusLog({ [`${fileName}_totalMB`]: totalMB, [`${fileName}_totalMB_mtime`]: mtime });
  return totalMB;
}

async function uploadFile(page, excelFile) {
  const fileName = path.basename(excelFile.name, path.extname(excelFile.name));
  console.log(`\n${'='.repeat(60)}`);
  console.log(`📦 Uploading: ${excelFile.name}`);
  console.log(`${'='.repeat(60)}`);

  updateStatusLog({
    [excelFile.name]: 'IN_PROGRESS',
    [`${excelFile.name}_startedAt`]: new Date().toISOString(),
  });

  await page.goto('https://up2u.mtn.com.gh/upload/upload-beneficiaries', { waitUntil: 'networkidle' });

  // Check if a previous upload is still processing — MTN blocks new uploads in this case
  const isBlocked = await page.evaluate(() => {
    const spans = Array.from(document.querySelectorAll('span[uk-icon*="ban"], span.uk-icon'));
    if (spans.some(el => el.getAttribute('uk-icon') && el.getAttribute('uk-icon').includes('ban'))) return true;
    const allText = document.body.innerText || '';
    return allText.includes('You cannot upload file until the last upload is done processing');
  });

  if (isBlocked) {
    console.warn('⏳ Upload blocked: a previous upload is still processing on MTN\'s end. Will retry later.');
    updateStatusLog({ [excelFile.name]: 'PENDING' });
    return { blocked: true };
  }

  await page.waitForSelector('input[name="files"]', { timeout: 10000 });
  await page.setInputFiles('input[name="files"]', excelFile.fullPath);
  console.log('✅ File selected');

  await page.waitForSelector('#groupId', { timeout: 10000 });
  await page.fill('#groupId', fileName);
  console.log(`✅ Group name: ${fileName}`);

  try {
    await page.waitForSelector('button.k-upload-selected', { timeout: 10000 });
    const beneficiariesNavPromise = page.waitForURL('**/beneficiaries**', { timeout: 60000 });
    await page.click('button.k-upload-selected');
    console.log('✅ Upload clicked');

    await beneficiariesNavPromise;
    console.log('✅ Beneficiaries page loaded');

    await page.waitForSelector('#uploadList', { timeout: 10000 });
    await page.click('#uploadList');
    console.log('✅ Share clicked');

    await page.waitForSelector('.uk-button-primary:has-text("Ok")', { timeout: 10000 });
    await page.waitForTimeout(500);
    const statusNavPromise = page.waitForURL('**/upload/upload-status', { timeout: 30000 });
    await page.click('.uk-button-primary:has-text("Ok")');
    console.log('✅ Confirmation accepted');

    await statusNavPromise;
    console.log('✅ Status page loaded — polling for DONE...');
    updateStatusLog({ [`${excelFile.name}_queuedAt`]: new Date().toISOString() });
  } catch (navErr) {
    console.error(`❌ Navigation failed during upload of "${excelFile.name}": ${navErr.message}`);
    await page.screenshot({ path: `nav-error-${fileName}.png` });

    // Check if we're still on the upload page — indicates the portal rejected the upload
    const currentUrl = page.url();
    if (currentUrl.includes('upload-beneficiaries')) {
      // Read whatever error text the portal is showing
      const portalError = await page.evaluate(() => {
        const el = document.querySelector('.k-notification-error, .uk-alert-danger, [class*="error"], [class*="alert"]');
        return el ? el.innerText.trim() : document.body.innerText.trim().substring(0, 300);
      });
      console.warn(`⚠️ Portal is still on upload page. Page message: ${portalError}`);

      // Only mark as DONE if the portal error clearly indicates the group already exists.
      // Any other error is treated as a retryable failure — do NOT mark done prematurely.
      const isDuplicateGroup = /already exists|duplicate|group name.*taken|already been (uploaded|shared)/i.test(portalError);
      if (isDuplicateGroup) {
        const completedAt = new Date().toISOString();
        markAsUploaded(excelFile.name);
        updateStatusLog({
          [excelFile.name]: 'DONE',
          [`${excelFile.name}_completedAt`]: completedAt,
          [`${excelFile.name}_note`]: `Marked done — portal rejected upload (duplicate group name). Portal message: ${portalError}`,
        });
        console.log(`✅ ${excelFile.name} — marked as DONE (duplicate group name confirmed)`);
        await sendCallback(excelFile.name, 'DONE', completedAt);
        return true;
      }

      // Unknown portal error — treat as a retryable nav failure
      console.warn(`⚠️ Unknown portal error — treating as nav failure for retry.`);
      sendAlert('⚠️ MTN GroupShare — Upload Portal Error', `"${excelFile.name}" received an unexpected portal error: ${portalError}`);
    }

    // Genuine navigation failure — apply retry / abandon logic
    const currentStatus = loadStatusLog();
    const retryCount = (currentStatus[`${excelFile.name}_retryCount`] || 0) + 1;

    if (retryCount >= MAX_FILE_RETRIES) {
      updateStatusLog({
        [excelFile.name]: 'ABANDONED',
        [`${excelFile.name}_timedOutAt`]: new Date().toISOString(),
        [`${excelFile.name}_retryCount`]: retryCount,
      });
      sendAlert('🚫 MTN GroupShare — File Abandoned', `"${excelFile.name}" failed navigation ${retryCount} times and has been abandoned.`);
      console.error(`🚫 ${excelFile.name} — abandoned after ${retryCount} nav failure(s)`);
      await sendCallback(excelFile.name, 'ABANDONED', new Date().toISOString());
    } else {
      updateStatusLog({
        [excelFile.name]: 'TIMEOUT',
        [`${excelFile.name}_timedOutAt`]: new Date().toISOString(),
        [`${excelFile.name}_retryCount`]: retryCount,
      });
      sendAlert('⚠️ MTN GroupShare — Upload Navigation Failed', `"${excelFile.name}" failed to navigate (attempt ${retryCount}/${MAX_FILE_RETRIES}). Will retry automatically.`);
      console.warn(`⚠️ ${excelFile.name} — nav failure (attempt ${retryCount}/${MAX_FILE_RETRIES}), will retry`);
    }
    return { error: true };
  }

  updateStatusLog({ [excelFile.name]: 'PROCESSING' });

  const maxAttempts = 70;
  const pollInterval = 20000;
  let isDone = false;
  let isFailed = false;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    await page.reload({ waitUntil: 'networkidle' });

    if (page.url().includes('/account/login')) {
      console.warn('🔒 Session expired during polling — re-logging in...');
      sendAlert('🔒 MTN GroupShare — Session Expired', 'Session expired during upload polling. Re-logging in...');
      await login(page);
      await page.goto('https://up2u.mtn.com.gh/upload/upload-status', { waitUntil: 'networkidle' });
    }

    const status = await page.evaluate((name) => {
      const rows = document.querySelectorAll('tr.k-master-row');
      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells[1] && cells[1].textContent.trim() === name) {
          return cells[4] ? cells[4].textContent.trim() : null;
        }
      }
      return null;
    }, fileName);

    console.log(`🔄 [${new Date().toLocaleTimeString()}] Attempt ${attempt}/${maxAttempts} — ${fileName}: ${status ?? 'not found'}`);

    if (status === 'DONE') {
      isDone = true;
      const completedAt = new Date().toISOString();
      markAsUploaded(excelFile.name);

      updateStatusLog({
        [excelFile.name]: 'DONE',
        [`${excelFile.name}_completedAt`]: completedAt,
      });

      await page.screenshot({ path: `done-${fileName}.png` });
      console.log(`🎉 ${excelFile.name} — DONE!`);
      await sendCallback(excelFile.name, 'DONE', completedAt);
      break;
    }

    if (status && /fail/i.test(status)) {
      isFailed = true;
      updateStatusLog({
        [excelFile.name]: 'FAILED',
        [`${excelFile.name}_failedAt`]: new Date().toISOString(),
      });
      sendAlert(
        '❌ MTN GroupShare — Upload Failed',
        `"${excelFile.name}" was marked as FAILED by MTN. Please check the portal.`
      );
      await page.screenshot({ path: `failed-${fileName}.png` });
      console.error(`❌ ${excelFile.name} — FAILED on MTN's end`);
      break;
    }

    if (attempt < maxAttempts) await page.waitForTimeout(pollInterval);
  }

  if (!isDone && !isFailed) {
    const currentStatus = loadStatusLog();
    const retryCount = (currentStatus[`${excelFile.name}_retryCount`] || 0) + 1;

    if (retryCount >= MAX_FILE_RETRIES) {
      updateStatusLog({
        [excelFile.name]: 'ABANDONED',
        [`${excelFile.name}_timedOutAt`]: new Date().toISOString(),
        [`${excelFile.name}_retryCount`]: retryCount,
      });
      console.error(`🚫 ${excelFile.name} — abandoned after ${retryCount} timeout(s)`);
      sendAlert(
        '🚫 MTN GroupShare — File Abandoned',
        `"${excelFile.name}" timed out ${retryCount} times and has been permanently abandoned. Please check the portal manually.`
      );
      await page.screenshot({ path: `abandoned-${fileName}.png` });
      await sendCallback(excelFile.name, 'ABANDONED', new Date().toISOString());
    } else {
      updateStatusLog({
        [excelFile.name]: 'TIMEOUT',
        [`${excelFile.name}_timedOutAt`]: new Date().toISOString(),
        [`${excelFile.name}_retryCount`]: retryCount,
      });
      console.warn(`⚠️  Timed out: ${excelFile.name} (attempt ${retryCount}/${MAX_FILE_RETRIES}) — will retry next run`);
      sendAlert(
        '⚠️ MTN GroupShare — Processing Timeout',
        `"${excelFile.name}" did not reach DONE within 35 minutes (attempt ${retryCount}/${MAX_FILE_RETRIES}). Will retry automatically.`
      );
      await page.screenshot({ path: `timeout-${fileName}.png` });
    }
  }

  return isDone;
}

async function run() {
  await startServer();

  const browser = await chromium.launch({
    headless: process.env.NODE_ENV === 'production',
    slowMo: process.env.NODE_ENV === 'production' ? 0 : 300,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--disable-infobars',
      '--window-size=1280,720',
    ]
  });

  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    viewport: { width: 1280, height: 720 },
    extraHTTPHeaders: {
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Encoding': 'gzip, deflate, br',
      'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
      'Upgrade-Insecure-Requests': '1',
      'sec-fetch-dest': 'document',
      'sec-fetch-mode': 'navigate',
      'sec-fetch-site': 'none',
      'sec-fetch-user': '?1',
    }
  });

  // ── KEY FIX: page from context (not browser) — applies all headers + userAgent ──
  const page = await context.newPage();

  // Hide Playwright's automation fingerprint from WAF detection
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });

  try {
    await login(page);

    console.log('\n🔁 Entering main loop — press Ctrl+C to stop.\n');
    let idleCount = 0;

    while (true) {
      // Clear the file-received wake flag only if it was set
      if (loadStatusLog()._fileReceived) updateStatusLog({ _fileReceived: false });

      if (!await isSessionActive(page)) {
        console.log('🔒 Session lost — re-logging in...');
        sendAlert('🔒 MTN GroupShare — Session Expired', 'Session expired. Re-logging in automatically...');
        try {
          await login(page);
        } catch (loginErr) {
          console.warn(`⚠️  Login failed (portal may be down): ${loginErr.message}`);
          console.log('⏳ Waiting 5 minutes before retrying...');
          sendAlert('⚠️ MTN GroupShare — Portal Down?', 'Login failed. Will retry in 5 minutes.');
          await new Promise(r => setTimeout(r, 5 * 60 * 1000));
          continue;
        }
      }

      // Service any immediate balance refresh requested by the GET /balance API endpoint
      if (loadStatusLog()._balanceRefreshRequested) {
        console.log('💰 Balance refresh requested via API — refreshing now...');
        updateStatusLog({ _balanceRefreshRequested: false });
        await checkBalance(page, context);
      }

      // Service a data purchase requested by POST /purchase
      if (loadStatusLog()._purchaseRequested) {
        console.log('💳 Purchase requested via API — starting now...');
        updateStatusLog({ _purchaseRequested: false, _purchaseStatus: 'IN_PROGRESS' });
        try {
          await purchaseData(page, context);
        } catch (purchaseErr) {
          console.error(`❌ Purchase failed: ${purchaseErr.message}`);
          sendAlert('❌ MTN GroupShare — Purchase Error', purchaseErr.message);
          updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: purchaseErr.message, _purchaseCompletedAt: new Date().toISOString() });
        }
        continue;
      }

      // ── Balance check — always fetch real balance (direct API, no navigation overhead) ──
      const { totalMB: currentBalanceMB } = await checkBalance(page, context);
      const purchaseStatusNow = loadStatusLog()._purchaseStatus;
      if (currentBalanceMB <= 90 * 1024 && purchaseStatusNow !== 'IN_PROGRESS') {
        console.log(`💳 Balance is ≤ 90 GB (${(currentBalanceMB / 1024).toFixed(2)} GB) — triggering auto-purchase before scanning files...`);
        sendAlert('💳 MTN GroupShare — Auto-Purchase', `Balance dropped to ${(currentBalanceMB / 1024).toFixed(2)} GB. Purchasing 1.5 TB bundle.`);
        updateStatusLog({ _purchaseStatus: 'IN_PROGRESS' });
        try {
          const purchaseSucceeded = await purchaseData(page, context);
          if (purchaseSucceeded) {
            updateStatusLog({ _balanceInsufficient: false });
            console.log('🔄 Purchase complete — resuming scan...');
          }
        } catch (purchaseErr) {
          console.error(`❌ Auto-purchase failed: ${purchaseErr.message}`);
          sendAlert('❌ MTN GroupShare — Auto-Purchase Failed', purchaseErr.message);
          updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: purchaseErr.message, _purchaseCompletedAt: new Date().toISOString() });
        }
        continue;
      }

      const pendingFiles = getPendingFiles(process.env.EXCEL_FOLDER_PATH);

      if (pendingFiles.length === 0) {
        idleCount++;
        const idleLog = loadStatusLog();
        const idleBalanceMB = idleLog._lastBalanceMB || 0;
        console.log(`😴 [${new Date().toLocaleTimeString()}] Idle #${idleCount} — No pending files. Balance: ${idleLog._lastBalance || 'Unknown'} (${idleBalanceMB.toFixed(2)} MB). Next check in 1 min...`);
        await interruptibleSleep(IDLE_REFRESH_INTERVAL);
        continue;
      }

      idleCount = 0;

      // Pre-compute totalMB for every file (uses cache — only parses XLSX on first encounter)
      // then sort largest-first (First Fit Decreasing) so the biggest allocations drain the
      // balance first; smaller files fill the remaining gap when balance is low.
      for (const f of pendingFiles) f.totalMB = getExcelTotalMB(f);
      pendingFiles.sort((a, b) => b.totalMB - a.totalMB);

      console.log(`\n📂 ${pendingFiles.length} file(s) queued (largest-first):`);
      pendingFiles.forEach((f, idx) =>
        console.log(`   ${idx + 1}. ${f.name} — ${(f.totalMB / 1024).toFixed(2)} GB`)
      );

      const AUTO_PURCHASE_THRESHOLD_MB = 90 * 1024;
      let anyFileUploaded = false;
      let skippedDueToBalance = 0;
      let autoPurchaseTriggered = false;
      // Track allocations confirmed DONE but not yet reflected in the MTN API balance
      // (MTN takes time to apply deductions after a successful upload)
      let pendingDeductionMB = 0;

      for (let i = 0; i < pendingFiles.length; i++) {

        // ── Step 1: Fetch real balance before each file (API is instant, no navigation needed) ──
        // Subtract any in-flight allocations not yet reflected in the MTN API
        const { totalMB: apiBalanceMB } = await checkBalance(page, context);
        const availableMB = Math.max(0, apiBalanceMB - pendingDeductionMB);
        if (pendingDeductionMB > 0) {
          console.log(`💰 Effective balance: ${(availableMB / 1024).toFixed(2)} GB (API: ${(apiBalanceMB / 1024).toFixed(2)} GB, pending deduction: ${(pendingDeductionMB / 1024).toFixed(2)} GB — MTN processing lag)`);
        }

        // ── Step 2: Check if balance is ≤ 90 GB before attempting next file ──
        const purchaseStatusInLoop = loadStatusLog()._purchaseStatus;
        if (availableMB <= AUTO_PURCHASE_THRESHOLD_MB && purchaseStatusInLoop !== 'IN_PROGRESS') {
          console.log(`💳 Balance is ≤ 90 GB (${(availableMB / 1024).toFixed(2)} GB) — triggering auto-purchase before next file...`);
          sendAlert('💳 MTN GroupShare — Auto-Purchase', `Balance dropped to ${(availableMB / 1024).toFixed(2)} GB. Purchasing 1.5 TB bundle.`);
          updateStatusLog({ _purchaseStatus: 'IN_PROGRESS' });
          autoPurchaseTriggered = true;
          break;
        }

        console.log(`\n📌 File ${i + 1} of ${pendingFiles.length}: ${pendingFiles[i].name}`);

        if (!await isSessionActive(page)) {
          console.log('🔒 Session lost before upload — re-logging in...');
          try {
            await login(page);
          } catch (loginErr) {
            console.warn(`⚠️  Login failed before upload (portal may be down): ${loginErr.message}`);
            console.log('⏳ Waiting 5 minutes before retrying...');
            sendAlert('⚠️ MTN GroupShare — Portal Down?', 'Login failed before upload. Will retry in 5 minutes.');
            await new Promise(r => setTimeout(r, 5 * 60 * 1000));
            break;
          }
        }

        // ── Step 3: Check if this file fits the current balance ──
        const requiredMB = pendingFiles[i].totalMB; // already computed above
        console.log(`💰 Available : ${availableMB.toFixed(2)} MB (${(availableMB / 1024).toFixed(2)} GB)`);
        console.log(`📊 Required  : ${requiredMB.toFixed(2)} MB (${(requiredMB / 1024).toFixed(2)} GB)`);

        if (requiredMB > availableMB) {
          const shortfall = (requiredMB - availableMB).toFixed(2);
          console.warn(`⚠️  "${pendingFiles[i].name}" requires more data than available (shortfall: ${shortfall} MB) — skipping, checking remaining files...`);
          skippedDueToBalance++;
          continue;
        }

        // ── Step 4: Upload ──
        console.log(`✅ Balance sufficient — proceeding...`);
        const uploadResult = await uploadFile(page, pendingFiles[i]);
        if (uploadResult && uploadResult.blocked) {
          console.warn('⏳ MTN is still processing a previous upload. Stopping batch — will retry all pending files next scan.');
          break;
        }
        if (uploadResult && uploadResult.error) {
          console.warn('⚠️ Upload navigation error — skipping to next file.');
          continue;
        }

        // ── Step 5: Post-upload handling ──
        if (uploadResult !== true) {
          console.warn(`⚠️ Upload of "${pendingFiles[i].name}" did not confirm DONE — skipping to next file.`);
          continue;
        }
        anyFileUploaded = true;
        // Clear insufficient flag since we just successfully uploaded
        updateStatusLog({ _balanceInsufficient: false });
        // Accumulate the in-flight deduction — MTN API balance lags behind actual allocation
        pendingDeductionMB += requiredMB;
      }

      // Running balance hit ≤ 90 GB — purchase now then re-scan
      if (autoPurchaseTriggered) {
        let purchaseSucceeded = false;
        try {
          purchaseSucceeded = await purchaseData(page, context);
        } catch (purchaseErr) {
          console.error(`❌ Auto-purchase failed: ${purchaseErr.message}`);
          sendAlert('❌ MTN GroupShare — Auto-Purchase Failed', purchaseErr.message);
          updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: purchaseErr.message, _purchaseCompletedAt: new Date().toISOString() });
        }
        if (purchaseSucceeded) {
          updateStatusLog({ _balanceInsufficient: false });
          console.log('🔄 Purchase complete — resuming file processing immediately...');
          continue;
        }
      }

      // All pending files were too large — none fit the available balance
      if (!anyFileUploaded && skippedDueToBalance > 0) {
        const availableGB = ((loadStatusLog()._lastBalanceMB || 0) / 1024).toFixed(2);
        updateStatusLog({ _balanceInsufficient: true });

        // Cannot trigger a purchase here — MTN only allows purchase when balance < 90 GB.
        // The only way to unblock is for the sender to submit smaller files that consume
        // the balance until it drops below 90 GB, which will trigger an auto-purchase.
        const msg = `All ${skippedDueToBalance} pending file(s) exceed available balance (${availableGB} GB). `
          + `Send files with total allocation ≤ ${availableGB} GB to drain balance below 90 GB and trigger an auto-purchase.`;
        console.warn(`⚠️  ${msg}`);
        sendAlert('⚠️ MTN GroupShare — Queue Blocked', msg);
      }

      console.log(`\n⏳ Batch complete. Next scan in 1 min...`);
      await interruptibleSleep(IDLE_REFRESH_INTERVAL);
    }

  } catch (err) {
    console.error('❌ Fatal error:', err.message);
    sendAlert('❌ MTN GroupShare — Fatal Error', err.message);
    try { await page.screenshot({ path: 'error-state.png' }); } catch {}
  } finally {
    try { await page.waitForTimeout(5000); } catch {}
    await browser.close();
  }
}

run();