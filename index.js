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
const IDLE_REFRESH_INTERVAL = 25 * 1000;

// Transient network error patterns — these are portal/connection blips, not code bugs.
// Transient network / navigation error patterns — portal blips and Playwright
// navigation timeouts are both retriable; only logic/auth errors are fatal.
const TRANSIENT_NAV_ERR = /ERR_EMPTY_RESPONSE|ERR_CONNECTION_RESET|ERR_CONNECTION_REFUSED|ERR_NAME_NOT_RESOLVED|ERR_TIMED_OUT|ERR_INTERNET_DISCONNECTED|net::|Timeout.*exceeded/i;

// page.goto with automatic retry on transient network errors (up to maxRetries attempts).
async function gotoWithRetry(page, url, opts, maxRetries = 3) {
  const navOpts = opts || {};
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await page.goto(url, navOpts);
    } catch (err) {
      if (!TRANSIENT_NAV_ERR.test(err.message) || attempt === maxRetries) throw err;
      const delayMs = attempt * 5000;
      console.warn(`⚠️  goto ${url} failed (attempt ${attempt}/${maxRetries}): ${err.message} — retrying in ${delayMs / 1000}s`);
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
}

// page.reload with automatic retry on transient network errors.
async function reloadWithRetry(page, opts, maxRetries = 3) {
  const navOpts = opts || {};
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await page.reload(navOpts);
    } catch (err) {
      if (!TRANSIENT_NAV_ERR.test(err.message) || attempt === maxRetries) throw err;
      const delayMs = attempt * 5000;
      console.warn(`⚠️  page.reload failed (attempt ${attempt}/${maxRetries}): ${err.message} — retrying in ${delayMs / 1000}s`);
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
}
const KEEP_ALIVE_INTERVAL_MS = 2.5 * 60 * 1000; // reload portal page if idle > 2.5 min
let _lastPortalNavAt = 0; // updated after every real page navigation to portal

const STATUS_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.status.json');
const MAX_FILE_RETRIES = parseInt(process.env.MAX_FILE_RETRIES || '5');
const MAX_SPLIT_B_CYCLES = parseInt(process.env.MAX_SPLIT_B_CYCLES || '3');

function normalizeRelativePath(filePath, basePath) {
  return path.relative(path.resolve(basePath), path.resolve(filePath)).split(path.sep).join('/');
}

function isSpreadsheetFile(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const base = path.basename(filePath);
  return (ext === '.xlsx' || ext === '.xls') && !base.startsWith('NM-merged-');
}

function listSpreadsheetFiles(folderPath) {
  const results = [];

  function visit(dirPath) {
    const entries = fs.readdirSync(dirPath, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dirPath, entry.name);
      if (entry.isDirectory()) {
        visit(fullPath);
        continue;
      }
      if (!entry.isFile()) continue;
      if (!isSpreadsheetFile(fullPath)) continue;
      results.push({
        name: normalizeRelativePath(fullPath, folderPath),
        fullPath,
        mtime: fs.statSync(fullPath).mtime,
      });
    }
  }

  visit(folderPath);
  return results;
}

function getFileKey(filePath) {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  const resolvedFile = path.resolve(filePath);
  if (folderPath) {
    const resolvedFolder = path.resolve(folderPath);
    if (resolvedFile === resolvedFolder || resolvedFile.startsWith(resolvedFolder + path.sep)) {
      return normalizeRelativePath(resolvedFile, resolvedFolder);
    }
  }
  return path.basename(filePath);
}

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

// Per-title dedup: tracks last time each alert title was successfully sent.
// Prevents repeated triggers from the same recurring condition from flooding
// the Telegram chat and hitting rate-limit 429.
const _alertLastSentAt = new Map();
const ALERT_COOLDOWN_MS = parseInt(process.env.ALERT_COOLDOWN_MINS || '5') * 60 * 1000;

// Titles that are always sent regardless of cooldown — one-off events with
// unique outcomes (success / permanent failure) that should never be suppressed.
const ALERT_NO_COOLDOWN = new Set([
  '🎉 MTN GroupShare — Airtime Loaded',
  '🎉 MTN GroupShare — Data Purchased',
  '🚫 MTN GroupShare — Merged Batch Abandoned',
  '🚫 MTN GroupShare — File Abandoned',
  '❌ MTN GroupShare — Merged Upload Failed',
  '❌ MTN GroupShare — Upload Failed',
  '❌ MTN GroupShare — Purchase Aborted',
]);

function sendAlert(title, message) {
  console.warn(`🔔 ALERT: ${title} — ${message}`);
  notifier.notify({ title, message, sound: true, wait: false });

  // Dedup check — suppress repeated alerts for the same event type within cooldown
  if (!ALERT_NO_COOLDOWN.has(title)) {
    const lastSent = _alertLastSentAt.get(title) || 0;
    const msSince = Date.now() - lastSent;
    if (msSince < ALERT_COOLDOWN_MS) {
      const remainMins = Math.ceil((ALERT_COOLDOWN_MS - msSince) / 60000);
      console.log(`🔕 Alert suppressed (cooldown ${remainMins} min remaining): ${title}`);
      return;
    }
  }
  _alertLastSentAt.set(title, Date.now());

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
            // Respect Telegram's retry_after on 429; fall back to 15s for other errors
            const retryAfterMs = (err.response?.body?.parameters?.retry_after || 15) * 1000;
            console.warn(`⚠️  Telegram alert failed (attempt ${attempt}): ${detail} — retrying in ${Math.round(retryAfterMs/1000)}s...`);
            setTimeout(() => trySend(attempt + 1), retryAfterMs);
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

async function sendCallback(filename, status, completedAt, orderOverride = null) {
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

  const payload = { filename, status, completedAt };

  // orderOverride is passed directly from a merged batch's sourceFiles entry.
  // For non-merged (legacy) files, fall back to status log lookup.
  if (orderOverride && orderOverride.orderIds) {
    payload.orderIds = orderOverride.orderIds;
  } else if (orderOverride && orderOverride.orderId) {
    payload.orderId = orderOverride.orderId;
  } else {
    const statusLog = loadStatusLog();
    if (statusLog[`${filename}_orderIds`]) {
      payload.orderIds = statusLog[`${filename}_orderIds`];
    } else if (statusLog[`${filename}_orderId`]) {
      payload.orderId = statusLog[`${filename}_orderId`];
    }
  }

  const url = `${orderSystemUrl.replace(/\/$/, '')}/api/groupshare/callback?secret=${encodeURIComponent(secret)}`;
  const body = JSON.stringify(payload);

  const MAX_ATTEMPTS   = 5;
  const RETRY_DELAYS   = [10_000, 30_000, 60_000, 120_000]; // ms between attempts

  console.log(`📡 Sending callback for "${filename}" (${status}) to ${orderSystemUrl}...`);

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
      });
      if (res.ok) {
        console.log(`📤 Callback sent for "${filename}" (${status}) — HTTP ${res.status}${attempt > 1 ? ` (attempt ${attempt})` : ''}`);
        return; // success
      }
      const text = await res.text().catch(() => '');
      console.warn(`⚠️  Callback for "${filename}" returned HTTP ${res.status}: ${text} (attempt ${attempt}/${MAX_ATTEMPTS})`);
    } catch (err) {
      console.error(`❌ Callback failed for "${filename}" (attempt ${attempt}/${MAX_ATTEMPTS}): ${err.message}`);
    }

    if (attempt < MAX_ATTEMPTS) {
      const delay = RETRY_DELAYS[attempt - 1];
      console.log(`🔁 Retrying callback for "${filename}" in ${delay / 1000}s...`);
      await new Promise(r => setTimeout(r, delay));
    }
  }

  console.error(`🚫 Callback for "${filename}" (${status}) failed after ${MAX_ATTEMPTS} attempts — giving up`);
  sendAlert('🚫 MTN GroupShare — Callback Failed', `Callback for "${filename}" (${status}) could not be delivered after ${MAX_ATTEMPTS} attempts. Manual resolution may be required.`);
}


// Sleep for `ms` ms, but wake every `checkIntervalMs` to check for a balance refresh,
// purchase request, or newly received file. Returns true if woken early, false if full duration elapsed.
async function interruptibleSleep(ms, checkIntervalMs = 5000) {
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

  // Collect source files locked inside any batch record that is still actively
  // being processed or has successfully completed.
  //   IN_PROGRESS / PROCESSING : file was submitted to MTN — re-queuing now risks
  //     double-allocation; keep locked until the polling window expires.
  //   DONE : source files should already be in the uploaded log, but if
  //     markAsUploaded was interrupted (crash mid-loop) a source file may still be
  //     on disk — treat as locked so it is never re-queued as PENDING.
  //   TIMEOUT / FAILED / ABANDONED : file either timed out or failed; the aging
  //     mechanism (PROCESSING → TIMEOUT in startup/idle loop) is designed to
  //     re-queue source files for retry — do NOT lock TIMEOUT here or that
  //     mechanism silently deadlocks instead.
  const lockedSourceFiles = new Set();
  for (const val of Object.values(statusLog)) {
    if (val === null || typeof val !== 'object' || !val.sourceFiles || !val.status) continue;
    if (['IN_PROGRESS', 'PROCESSING', 'DONE'].includes(val.status)) {
      for (const sf of val.sourceFiles) lockedSourceFiles.add(sf.filename);
    }
  }

  const files = listSpreadsheetFiles(folderPath)
    .filter(f => {
      if (uploaded.includes(f.name)) return false;
      if (statusLog[f.name] === 'ABANDONED') return false;
      if (statusLog[f.name] === 'SPLIT') return false; // original file already split into parts — parts are pending
      if (statusLog[f.name] === 'STUCK') return false; // Part B exhausted all retry cycles — awaiting human intervention
      if (lockedSourceFiles.has(f.name)) return false; // owned by an active batch — do not re-queue
      return true;
    })
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
    // Use gotoWithRetry so a single brief blip doesn't incorrectly invalidate a
    // live session. Keep timeout short (15s) and retries low (2) so the check
    // stays fast; fall back to URL check if even retries time out.
    try {
      await gotoWithRetry(page, 'https://up2u.mtn.com.gh', { waitUntil: 'load', timeout: 15000 }, 2);
    } catch (navErr) {
      // Navigation timed out — check current URL anyway before giving up
      if (!page.url().includes('up2u.mtn.com.gh')) throw navErr;
      console.warn(`⚠️  Session check: navigation slow (${navErr.message}) — checking URL anyway`);
    }
    _lastPortalNavAt = Date.now();
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
    // Use gotoWithRetry so transient ERR_EMPTY_RESPONSE / portal blips don't
    // abort login immediately — they are retried a few times before giving up.
    await gotoWithRetry(page, 'https://up2u.mtn.com.gh/account/login', {
      waitUntil: 'domcontentloaded',
      timeout: 240000
    });

    await page.screenshot({ path: 'login-debug.png', fullPage: true, timeout: 180000 });
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
        resetOtpState();
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
          await gotoWithRetry(page, 'https://up2u.mtn.com.gh/account/verify-otp', {
            waitUntil: 'domcontentloaded',
            timeout: 240000
          });
        }

        await page.fill('input[name="OTPCode"]', otp);

        const navigationPromise = page.waitForURL(
          url => !url.href.includes('/account/verify-otp') && !url.href.includes('/account/login'),
          { timeout: 240000, waitUntil: 'domcontentloaded' }
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
    // Suppress alert for transient network errors — the caller's retry loop handles those
    // and sends its own "Portal Down?" alert. Only alert for genuine login failures
    // (bad credentials, OTP timeout, unexpected page state, etc.).
    if (!TRANSIENT_NAV_ERR.test(err.message)) {
      sendAlert('❌ MTN GroupShare — Login Failed', err.message);
    }
    throw err;
  }

  sendAlert('❌ MTN GroupShare — Login Failed', `OTP was received but login could not be completed after ${maxSubmitAttempts} submission attempts.`);
  throw new Error(`Login failed — OTP submission unsuccessful after ${maxSubmitAttempts} attempts`);
}

// Send an immediate EVD top-up request via the api-server's /evd/trigger-now endpoint.
// Called when purchaseData detects insufficient GH¢ balance so the bot doesn't have to
// wait up to EVD_AUTO_POLL_MINS (3 min) for the scheduled auto-loader to fire.
async function triggerEvdTopUp(neededGhc) {
  const EVD_PURCHASE_TARGET_GHC = parseFloat(process.env.EVD_PURCHASE_TARGET_GHC || '4813');
  const amount = Math.max(1, Math.ceil(neededGhc > 0 ? neededGhc : EVD_PURCHASE_TARGET_GHC));
  const port   = process.env.API_INTERNAL_PORT || 7070;
  try {
    const res  = await fetch(`http://127.0.0.1:${port}/evd/trigger-now`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-Internal-Dashboard': '1' },
      body:    JSON.stringify({ amount }),
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok && data.success) {
      console.log(`⚡ EVD top-up triggered — GH¢ ${amount} requested`);
    } else {
      console.warn(`⚠️  EVD trigger-now failed: ${JSON.stringify(data)}`);
    }
  } catch (err) {
    console.warn(`⚠️  EVD trigger-now call failed: ${err.message}`);
  }
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
      updateStatusLog({ _purchaseStatus: 'WAITING_FUNDS', _purchaseNote: msg, _purchaseCompletedAt: new Date().toISOString() });
      await triggerEvdTopUp(REQUIRED - apiAccountBalance);
      return false;
    }
    console.log(`✅ Account balance sufficient (API pre-check) — proceeding to purchase page`);
  } else {
    console.log('ℹ️  Account balance not available via API — will verify on purchase page');
  }

  await gotoWithRetry(page, 'https://up2u.mtn.com.gh/business/purchase-bundles', { waitUntil: 'networkidle' });
  // Reload to flush any cached balance value the page may render on first load
  await reloadWithRetry(page, { waitUntil: 'networkidle' });

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
    updateStatusLog({ _purchaseStatus: 'WAITING_FUNDS', _purchaseNote: msg, _purchaseCompletedAt: new Date().toISOString() });
    await triggerEvdTopUp(REQUIRED - balance);
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
  }, null, { timeout: 15000 });

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

  // ── Daily batch count ─────────────────────────────────────────────────────
  // Tracks how many 1.5 TB bundles have been purchased today (UTC date).
  // Resets automatically when the date changes.
  const todayUTC = new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
  const batchCountLog = loadStatusLog();
  const prevDate    = batchCountLog._batchCountDate  || '';
  const prevCount   = batchCountLog._batchCountToday || 0;
  const prevTotal   = batchCountLog._batchCountTotal || 0;
  const newCount    = prevDate === todayUTC ? prevCount + 1 : 1;
  const newTotal    = prevTotal + 1;
  console.log(`📦 Daily batch count: ${newCount} purchase(s) on ${todayUTC} (${newTotal} all-time)`);

  sendAlert('🎉 MTN GroupShare — Data Purchased', `Successfully purchased 1.5 TB (1 TB, 512 GB) data bundle for GH¢ 4,812.96. New balance: ${newBalanceText} | Batch count today: ${newCount} · All-time: ${newTotal}`);
  updateStatusLog({
    _purchaseStatus: 'DONE',
    _purchaseNote: `1.5 TB (1 TB 512 GB) @ GH¢ 4,812.96 — balance after: ${newBalanceText}`,
    _purchaseCompletedAt: new Date().toISOString(),
    _batchCountDate: todayUTC,
    _batchCountToday: newCount,
    _batchCountTotal: newTotal,
  });
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
  // Wrapped in try/catch — if the portal is temporarily unreachable (ERR_EMPTY_RESPONSE etc.)
  // we return the last cached balance instead of propagating a fatal crash.
  console.log('\n💰 Checking data balance (DOM)...');
  try {
    await gotoWithRetry(page, 'https://up2u.mtn.com.gh', { waitUntil: 'networkidle' });
    await reloadWithRetry(page, { waitUntil: 'networkidle' });
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
  } catch (domErr) {
    // Both API and DOM paths failed — portal is temporarily unreachable.
    // Return cached balance so the main loop can continue without crashing.
    const cached = loadStatusLog();
    const totalMB = cached._lastBalanceMB || 0;
    const balanceText = cached._lastBalance || 'Unknown';
    console.warn(`⚠️  DOM balance scrape failed: ${domErr.message}`);
    console.warn(`⚠️  Returning cached balance: ${balanceText} (${totalMB.toFixed(2)} MB)`);
    sendAlert('⚠️ MTN GroupShare — Balance Check Failed', `Both API and DOM balance checks failed. Using cached value: ${balanceText}. Portal may be temporarily unreachable.`);
    return { balanceText, totalMB };
  }
}

function _parseExcelTotalMB(filePath) {
  try {
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rawRows = XLSX.utils.sheet_to_json(sheet, { header: 1 });

    const headerRow = rawRows[0];
    const dataMBColIndex = 3;

    console.log(`📊 Using column: "${headerRow ? headerRow[dataMBColIndex] : 'unknown'}" (column 4)`);

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
  } catch (err) {
    console.error(`❌ Failed to parse XLSX "${path.basename(filePath)}": ${err.message} — treating as 0 MB`);
    return 0;
  }
}

// Returns cached totalMB for a file if the file hasn't changed (mtime match),
// otherwise parses the XLSX and caches the result for next time.
function getExcelTotalMB(file) {
  const filePath = file.fullPath || file;
  const fileName = file.name || getFileKey(filePath);
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

// ── MERGED FILE BUILDER ───────────────────────────────────────────────────────
// Accepts an array of file objects (each with .name, .fullPath, .totalMB).
// Reads each XLSX, concatenates all data rows under a shared header, writes to
// a temp NM-merged-* file, and records the batch metadata in the status log.
// Returns the merged file object (same shape as a pendingFiles entry) plus
// a sourceFiles array for callback tracking.
function buildMergedFile(files) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const mergedName = `NM-merged-${timestamp}.xlsx`;
  const mergedPath = path.join(process.env.EXCEL_FOLDER_PATH, mergedName);

  let header = null;
  let allDataRows = [];

  for (const file of files) {
    const workbook = XLSX.readFile(file.fullPath);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
    if (rows.length === 0) continue;

    if (!header) {
      header = rows[0]; // capture header from first file
    }
    // Append data rows (skip header row of each file)
    for (let r = 1; r < rows.length; r++) {
      if (rows[r] && rows[r].length > 0) allDataRows.push(rows[r]);
    }
  }

  if (!header) throw new Error('buildMergedFile: no data rows found across selected files');

  const mergedSheet = XLSX.utils.aoa_to_sheet([header, ...allDataRows]);
  const mergedWorkbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(mergedWorkbook, mergedSheet, 'Sheet1');
  XLSX.writeFile(mergedWorkbook, mergedPath);

  const totalAllocationMB = files.reduce((sum, f) => sum + f.totalMB, 0);
  console.log(`📎 Merged ${files.length} file(s) → ${mergedName} (${(totalAllocationMB / 1024).toFixed(2)} GB, ${allDataRows.length} rows)`);

  // Load order IDs for each source file from status log.
  // Flat keys (_orderId/_orderIds) are set at upload time. If a file was
  // previously merged and its order IDs only exist inside an older batch
  // record's sourceFiles[], scan those records as a fallback so callbacks
  // on retry attempts always include the correct order references.
  const log = loadStatusLog();
  const sourceFiles = files.map(f => {
    const entry = { filename: f.name, allocationMB: f.totalMB, callbackSentAt: null };
    if (log[`${f.name}_orderIds`]) {
      entry.orderIds = log[`${f.name}_orderIds`];
    } else if (log[`${f.name}_orderId`]) {
      entry.orderId = log[`${f.name}_orderId`];
    } else {
      // Fallback: scan all previous merged batch records for this source file
      let bestCreatedAt = null;
      for (const [, val] of Object.entries(log)) {
        if (val === null || typeof val !== 'object' || !val.sourceFiles || !val.createdAt) continue;
        const prev = val.sourceFiles.find(s => s.filename === f.name);
        if (!prev) continue;
        if (!bestCreatedAt || val.createdAt > bestCreatedAt) {
          bestCreatedAt = val.createdAt;
          if (prev.orderIds) { entry.orderIds = prev.orderIds; delete entry.orderId; }
          else if (prev.orderId) { entry.orderId = prev.orderId; delete entry.orderIds; }
        }
      }
    }
    return entry;
  });

  // Record merged batch metadata in status log
  updateStatusLog({
    [mergedName]: {
      status: 'PENDING',
      createdAt: new Date().toISOString(),
      totalAllocationMB,
      sourceFiles,
      retryCount: 0,
    },
  });

  const mergedFile = {
    name: mergedName,
    fullPath: mergedPath,
    totalMB: totalAllocationMB,
    mtime: fs.statSync(mergedPath).mtime,
    isMerged: true,
  };

  return { mergedFile, sourceFiles };
}

// ── OPTIMAL BIN-PACKER ───────────────────────────────────────────────────────
// Finds the subset of files whose combined allocation is as large as possible
// without exceeding availableMB.  This maximises balance consumption so the
// leftover after each cycle is as small as possible — ideally < 90 GB so the
// auto-purchase threshold is crossed and a new bundle is purchased immediately.
//
// Algorithm: branch-and-bound over files sorted largest-first, with a suffix-sum
// upper bound that lets whole subtrees be pruned.  Terminates early when a
// solution within 90 GB of availableMB is found, or after a 500 ms deadline.
// Falls back gracefully to the best result found so far.
function findOptimalBatch(files, availableMB) {
  if (files.length === 0) return [];

  const TARGET_LEFTOVER_MB = 90 * 1024; // stop searching once within 90 GB
  const DEADLINE_MS = 500;
  const deadline = Date.now() + DEADLINE_MS;

  // Sort largest-first — gives a good greedy baseline on the first branch
  // AND enables the tightest upper-bound pruning.
  const sorted = [...files].sort((a, b) => b.totalMB - a.totalMB);

  // Suffix sums — suffixSum[i] = sum of sorted[i..n-1]
  const suffixSum = new Array(sorted.length + 1).fill(0);
  for (let i = sorted.length - 1; i >= 0; i--) {
    suffixSum[i] = suffixSum[i + 1] + sorted[i].totalMB;
  }

  let bestTotal = 0;
  let bestIndices = [];
  let timedOut = false;

  const stack = [{ idx: 0, total: 0, indices: [] }];

  while (stack.length > 0 && !timedOut) {
    if (Date.now() > deadline) { timedOut = true; break; }

    const { idx, total, indices } = stack.pop();

    if (total > bestTotal) {
      bestTotal = total;
      bestIndices = indices;
    }

    // Early-exit: already within the target leftover
    if (availableMB - total < TARGET_LEFTOVER_MB) break;

    // Pruning: adding all remaining files can't beat bestTotal
    if (total + suffixSum[idx] <= bestTotal) continue;

    for (let i = sorted.length - 1; i >= idx; i--) {
      const next = total + sorted[i].totalMB;
      if (next <= availableMB) {
        // Upper-bound check before pushing
        if (next + suffixSum[i + 1] > bestTotal) {
          stack.push({ idx: i + 1, total: next, indices: [...indices, i] });
        }
      }
    }
  }

  const result = bestIndices.map(i => sorted[i]);
  const leftoverGB = ((availableMB - bestTotal) / 1024).toFixed(2);
  console.log(`📊 Batch optimizer: using ${(bestTotal / 1024).toFixed(2)} GB of ${(availableMB / 1024).toFixed(2)} GB — leftover ${leftoverGB} GB`
    + (timedOut ? ' (time-limited)' : ''));
  return result;
}

// ── ROW-LEVEL FILE SPLITTER ───────────────────────────────────────────────────
// When no pending file fits the available balance and balance > 90 GB (so
// auto-purchase won't fire), the bot is deadlocked.  This function breaks the
// deadlock by splitting the smallest pending file into two parts:
//   Part A — rows whose cumulative MB fits within availableMB — uploaded now.
//   Part B — remaining rows — uploaded after balance is replenished.
//
// Both parts carry the original orderId/orderIds for manual traceability.
// Only Part B sends a callback to the order system (signalling the full order
// is complete); Part A is treated as an intermediate chunk with no callback.
//
// Original file is deleted from disk; status log records it as 'SPLIT'.
// Returns { partAFile, partBFile } on success.  Throws on unrecoverable errors.
function splitFileToFitBalance(file, availableMB) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const partAName = `NM-split-${timestamp}-a-${file.name}`;
  const partBName = `NM-split-${timestamp}-b-${file.name}`;
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  const partAPath  = path.join(folderPath, partAName);
  const partBPath  = path.join(folderPath, partBName);
  const dataMBColIndex = 3;

  const workbook = XLSX.readFile(file.fullPath);
  const sheet    = workbook.Sheets[workbook.SheetNames[0]];
  const rawRows  = XLSX.utils.sheet_to_json(sheet, { header: 1 });

  if (rawRows.length < 2) throw new Error(`splitFileToFitBalance: "${file.name}" has no data rows`);

  const header   = rawRows[0];
  const partARows = [];
  const partBRows = [];
  let runningMB   = 0;

  for (let r = 1; r < rawRows.length; r++) {
    const row = rawRows[r];
    if (!row || row.length === 0) continue;
    const mb = parseFloat(row[dataMBColIndex]) || 0;
    if (runningMB + mb <= availableMB) {
      runningMB += mb;
      partARows.push(row);
    } else {
      partBRows.push(row);
    }
  }

  if (partARows.length === 0) {
    throw new Error(`splitFileToFitBalance: first row alone exceeds available balance (${(availableMB / 1024).toFixed(2)} GB) — cannot split`);
  }
  if (partBRows.length === 0) {
    throw new Error(`splitFileToFitBalance: all rows fit — split is unnecessary`);
  }

  // Write Part A
  const wbA = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wbA, XLSX.utils.aoa_to_sheet([header, ...partARows]), 'Sheet1');
  XLSX.writeFile(wbA, partAPath);

  // Write Part B
  const wbB = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wbB, XLSX.utils.aoa_to_sheet([header, ...partBRows]), 'Sheet1');
  XLSX.writeFile(wbB, partBPath);

  const partAMB = partARows.reduce((s, r) => s + (parseFloat(r[dataMBColIndex]) || 0), 0);
  const partBMB = partBRows.reduce((s, r) => s + (parseFloat(r[dataMBColIndex]) || 0), 0);

  // Carry order IDs from the original file to both parts (for manual traceability)
  const log = loadStatusLog();
  const orderUpdates = {};
  if (log[`${file.name}_orderIds`]) {
    orderUpdates[`${partAName}_orderIds`] = log[`${file.name}_orderIds`];
    orderUpdates[`${partBName}_orderIds`] = log[`${file.name}_orderIds`];
  } else if (log[`${file.name}_orderId`]) {
    orderUpdates[`${partAName}_orderId`] = log[`${file.name}_orderId`];
    orderUpdates[`${partBName}_orderId`] = log[`${file.name}_orderId`];
  }

  updateStatusLog({
    // Original — mark as SPLIT so it is not re-queued
    [file.name]: 'SPLIT',
    [`${file.name}_splitAt`]:   new Date().toISOString(),
    [`${file.name}_splitPartA`]: partAName,
    [`${file.name}_splitPartB`]: partBName,
    // Part A — intermediate, no callback on completion
    [partAName]: 'PENDING',
    [`${partAName}_isSplitIntermediate`]: true,
    [`${partAName}_originalFile`]:        file.name,
    [`${partAName}_partnerPart`]:         partBName,
    // Part B — final, sends callback on completion
    [partBName]: 'PENDING',
    [`${partBName}_isSplitFinal`]:   true,
    [`${partBName}_originalFile`]:   file.name,
    ...orderUpdates,
  });

  // Delete the original file from disk — both parts are now pending
  try { fs.unlinkSync(file.fullPath); } catch {}

  const msg = `"${file.name}" split to drain balance.\n`
    + `Part A: ${(partAMB / 1024).toFixed(2)} GB (${partARows.length} rows) — uploading now\n`
    + `Part B: ${(partBMB / 1024).toFixed(2)} GB (${partBRows.length} rows) — uploads after replenishment\n`
    + `Callback held until Part B completes.`;
  console.log(`✂️  ${msg}`);
  sendAlert('✂️ MTN GroupShare — File Split', msg);

  return {
    partAFile: { name: partAName, fullPath: partAPath, totalMB: partAMB, mtime: fs.statSync(partAPath).mtime },
    partBFile: { name: partBName, fullPath: partBPath, totalMB: partBMB, mtime: fs.statSync(partBPath).mtime },
  };
}

// Parse the data allocation from a stripped filename.
// Supports patterns like "NM-25GB_...", "NM-1024MB_...", "NM-1.5TB_...".
function parseAllocationFromFilename(strippedBase) {
  const m = strippedBase.match(/(\d+(?:\.\d+)?)\s*(MB|GB|TB)/i);
  return m ? { value: m[1], unit: m[2].toUpperCase() } : null;
}

async function uploadFile(page, excelFile) {
  const fullBaseName  = path.basename(excelFile.name, path.extname(excelFile.name));
  const strippedBase  = path.basename(stripTimestamp(excelFile.name), path.extname(excelFile.name));
  const groupName     = strippedBase; // group name = original filename without extension
  const allocation    = parseAllocationFromFilename(strippedBase);

  console.log(`\n${'='.repeat(60)}`);
  console.log(`📦 Uploading: ${excelFile.name}`);
  console.log(`   Group name  : ${groupName}`);
  console.log(`   Data volume : ${allocation ? `${allocation.value} ${allocation.unit}` : '⚠️  not found in filename'}`);
  console.log(`${'='.repeat(60)}`);

  if (!allocation) {
    const errMsg = `Cannot upload "${excelFile.name}" — data allocation could not be parsed from the filename. ` +
      `Rename the file to include the allocation, e.g. NM-25GB_... or NM-1024MB_...`;
    console.error(`❌ ${errMsg}`);
    sendAlert('❌ MTN GroupShare — Missing Allocation', errMsg);
    updateStatusLog({
      [excelFile.name]: 'ABANDONED',
      [`${excelFile.name}_timedOutAt`]: new Date().toISOString(),
      [`${excelFile.name}_note`]: errMsg,
    });
    return { error: true };
  }

  if (excelFile.isMerged) {
    withFileLock(STATUS_LOG, () => {
      const l = loadStatusLog();
      const rec = l[excelFile.name] || {};
      rec.status = 'IN_PROGRESS';
      rec.startedAt = new Date().toISOString();
      l[excelFile.name] = rec;
      atomicWrite(STATUS_LOG, JSON.stringify(l, null, 2));
    });
  } else {
    updateStatusLog({
      [excelFile.name]: 'IN_PROGRESS',
      [`${excelFile.name}_startedAt`]: new Date().toISOString(),
    });
  }

  // ── Navigate to Manage Groups ─────────────────────────────────────────────
  await gotoWithRetry(page, 'https://up2u.mtn.com.gh/beneficiaries/manage-groups', { waitUntil: 'networkidle' });
  _lastPortalNavAt = Date.now();

  // ── Recovery: check if group already exists from a prior failed attempt ────
  let groupCreatedSuccessfully = false;
  try {
    const alreadyExists = await page.evaluate((name) => {
      for (const row of document.querySelectorAll('tr.k-master-row, tbody tr')) {
        if (row.textContent.includes(name)) return true;
      }
      return false;
    }, groupName);
    if (alreadyExists) {
      console.log(`ℹ️  Group "${groupName}" already exists on Manage Groups — treating as DONE (recovery)`);
      groupCreatedSuccessfully = true;
    }
  } catch (checkErr) {
    console.warn(`⚠️  Pre-flight group existence check failed: ${checkErr.message}`);
  }

  if (!groupCreatedSuccessfully) {
    try {
      // ── 1. Open Create Group modal ───────────────────────────────────────
      await page.waitForSelector('button[onclick*="OpenCreateGroupModal"]', { timeout: 15000 });
      await page.click('button[onclick*="OpenCreateGroupModal"]');
      await page.waitForSelector('#create-group-name', { timeout: 15000 });
      console.log('✅ Create Group modal opened');

      // ── 2. Group Name ────────────────────────────────────────────────────
      await page.fill('#create-group-name', groupName);
      console.log(`✅ Group name: ${groupName}`);

      // ── 3. Data Volume + Unit ────────────────────────────────────────────
      await page.fill('#create-group-data', allocation.value);
      await page.selectOption('#create-group-data-unit', allocation.unit);
      console.log(`✅ Data volume: ${allocation.value} ${allocation.unit}`);

      // ── 4. Switch to Upload Beneficiaries tab ────────────────────────────
      await page.click('a[data-mode="upload"]');
      console.log('✅ Upload Beneficiaries tab selected');

      // ── 5. Attach Excel file ─────────────────────────────────────────────
      await page.waitForSelector('#group-beneficiaries-file', { timeout: 10000 });
      await page.setInputFiles('#group-beneficiaries-file', excelFile.fullPath);
      console.log('✅ File attached');

      // ── 6. Submit ────────────────────────────────────────────────────────
      await page.waitForSelector('button.submit-btn', { timeout: 10000 });
      await page.click('button.submit-btn');
      console.log('✅ Create Group submitted — waiting for response...');

      // ── 7. Detect success or failure notification ────────────────────────
      const outcome = await Promise.race([
        page.waitForSelector(
          '.uk-notification-message-success, .uk-notification-message[class*="success"]',
          { timeout: 120000 }
        ).then(() => 'success'),
        page.waitForSelector(
          '.uk-notification-message-danger, .uk-notification-message[class*="danger"], .uk-alert-danger',
          { timeout: 120000 }
        ).then(async (el) => {
          const msg = await el.evaluate(e => e.textContent.trim()).catch(() => '');
          return { type: 'error', msg };
        }),
      ]);

      if (outcome === 'success') {
        console.log('✅ Portal confirmed group created');
        groupCreatedSuccessfully = true;
      } else {
        const errMsg = typeof outcome === 'object' ? outcome.msg : '';
        const isDuplicate = /already exists|duplicate|group name.*taken/i.test(errMsg);
        if (isDuplicate) {
          console.log(`ℹ️  Duplicate group name confirmed by portal — treating as DONE`);
          groupCreatedSuccessfully = true;
        } else {
          throw new Error(`Portal error after submit: ${errMsg}`);
        }
      }
    } catch (navErr) {
      console.error(`❌ Create Group failed for "${excelFile.name}": ${navErr.message}`);
      try { await page.screenshot({ path: `nav-error-${fullBaseName}.png`, timeout: 5000 }); } catch {}

      // ── Retry / abandon logic ────────────────────────────────────────────
      const currentStatus = loadStatusLog();
      const existingNavRetry = excelFile.isMerged
        ? (currentStatus[excelFile.name]?.retryCount || 0)
        : (currentStatus[`${excelFile.name}_retryCount`] || 0);
      const retryCount = existingNavRetry + 1;
      const timedOutAt = new Date().toISOString();

      if (retryCount >= MAX_FILE_RETRIES) {
        if (excelFile.isMerged) {
          withFileLock(STATUS_LOG, () => {
            const l = loadStatusLog();
            const rec = l[excelFile.name] || {};
            rec.status = 'ABANDONED'; rec.timedOutAt = timedOutAt; rec.retryCount = retryCount;
            l[excelFile.name] = rec;
            atomicWrite(STATUS_LOG, JSON.stringify(l, null, 2));
          });
          try { fs.unlinkSync(excelFile.fullPath); } catch {}
          const srcNames = (loadStatusLog()[excelFile.name]?.sourceFiles || []).map(s => s.filename).join(', ');
          sendAlert('🚫 MTN GroupShare — Merged Batch Abandoned', `Batch "${excelFile.name}" failed ${retryCount} times. Source files re-queued: ${srcNames}`);
        } else {
          updateStatusLog({
            [excelFile.name]: 'ABANDONED',
            [`${excelFile.name}_timedOutAt`]: timedOutAt,
            [`${excelFile.name}_retryCount`]: retryCount,
          });
          const navAbandonLog = loadStatusLog();
          if (navAbandonLog[`${excelFile.name}_isSplitIntermediate`]) {
            const partBName = navAbandonLog[`${excelFile.name}_partnerPart`];
            if (partBName) {
              updateStatusLog({ [partBName]: 'ABANDONED', [`${partBName}_timedOutAt`]: timedOutAt, [`${partBName}_abandonedReason`]: `Part A "${excelFile.name}" was abandoned` });
              try { fs.unlinkSync(path.join(process.env.EXCEL_FOLDER_PATH, partBName)); } catch {}
              sendAlert('🚫 MTN GroupShare — Split Part A Abandoned', `"${excelFile.name}" (split Part A) abandoned after ${retryCount} failures.\nPart B "${partBName}" also abandoned.`);
              await sendCallback(partBName, 'ABANDONED', timedOutAt);
            } else {
              sendAlert('🚫 MTN GroupShare — File Abandoned', `"${excelFile.name}" failed ${retryCount} times and has been abandoned.`);
              await sendCallback(excelFile.name, 'ABANDONED', timedOutAt);
            }
          } else if (navAbandonLog[`${excelFile.name}_isSplitFinal`]) {
            const originalFile = navAbandonLog[`${excelFile.name}_originalFile`] || excelFile.name;
            const splitAttempt = (navAbandonLog[`${excelFile.name}_splitFinalAttempt`] || 0) + 1;
            if (splitAttempt < MAX_SPLIT_B_CYCLES) {
              updateStatusLog({ [excelFile.name]: null, [`${excelFile.name}_timedOutAt`]: null, [`${excelFile.name}_retryCount`]: 0, [`${excelFile.name}_splitFinalAttempt`]: splitAttempt });
              sendAlert('⚠️ MTN GroupShare — Split Part B Retry', `"${excelFile.name}" (split Part B, original: "${originalFile}") failed (cycle ${splitAttempt}/${MAX_SPLIT_B_CYCLES}). Re-queuing.`);
            } else {
              updateStatusLog({ [excelFile.name]: 'STUCK', [`${excelFile.name}_splitFinalAttempt`]: splitAttempt });
              sendAlert('🚨 MTN GroupShare — Split Part B Needs Intervention', `"${excelFile.name}" (split Part B, original: "${originalFile}") exhausted ${MAX_SPLIT_B_CYCLES} cycles. Please process manually.`);
            }
          } else {
            sendAlert('🚫 MTN GroupShare — File Abandoned', `"${excelFile.name}" failed ${retryCount} times and has been abandoned.`);
            await sendCallback(excelFile.name, 'ABANDONED', timedOutAt);
          }
        }
        console.error(`🚫 ${excelFile.name} — abandoned after ${retryCount} failure(s)`);
        try { await page.screenshot({ path: `abandoned-${fullBaseName}.png`, timeout: 5000 }); } catch {}
      } else {
        if (excelFile.isMerged) {
          withFileLock(STATUS_LOG, () => {
            const l = loadStatusLog();
            const rec = l[excelFile.name] || {};
            rec.status = 'TIMEOUT'; rec.timedOutAt = timedOutAt; rec.retryCount = retryCount;
            l[excelFile.name] = rec;
            atomicWrite(STATUS_LOG, JSON.stringify(l, null, 2));
          });
          try { fs.unlinkSync(excelFile.fullPath); } catch {}
          sendAlert('⚠️ MTN GroupShare — Merged Batch Nav Failed', `Batch "${excelFile.name}" failed to create (attempt ${retryCount}/${MAX_FILE_RETRIES}). Source files will be re-queued.`);
        } else {
          updateStatusLog({
            [excelFile.name]: 'TIMEOUT',
            [`${excelFile.name}_timedOutAt`]: timedOutAt,
            [`${excelFile.name}_retryCount`]: retryCount,
          });
          sendAlert('⚠️ MTN GroupShare — Upload Failed', `"${excelFile.name}" failed (attempt ${retryCount}/${MAX_FILE_RETRIES}). Will retry.`);
        }
        console.warn(`⚠️ ${excelFile.name} — failure (attempt ${retryCount}/${MAX_FILE_RETRIES}), TIMEOUT`);
        try { await page.screenshot({ path: `timeout-${fullBaseName}.png`, timeout: 5000 }); } catch {}
      }
      return { error: true };
    }
  }

  // ── Mark DONE ─────────────────────────────────────────────────────────────
  const completedAt = new Date().toISOString();

  if (excelFile.isMerged) {
    withFileLock(STATUS_LOG, () => {
      const l = loadStatusLog();
      const rec = l[excelFile.name] || {};
      const queuedMs = rec.queuedAt ? new Date(rec.queuedAt).getTime() : null;
      rec.status = 'DONE';
      rec.completedAt = completedAt;
      if (queuedMs) rec.processingDurationMs = Date.now() - queuedMs;
      l[excelFile.name] = rec;
      atomicWrite(STATUS_LOG, JSON.stringify(l, null, 2));
    });

    const batchRecord = loadStatusLog()[excelFile.name] || {};
    const sourceFiles = batchRecord.sourceFiles || [];
    for (let si = 0; si < sourceFiles.length; si++) {
      const src = sourceFiles[si];
      if (src.callbackSentAt) {
        console.log(`ℹ️  Callback already sent for "${src.filename}" — skipping`);
      } else {
        await sendCallback(src.filename, 'DONE', completedAt, src);
        withFileLock(STATUS_LOG, () => {
          const l = loadStatusLog();
          const rec = l[excelFile.name] || {};
          if (rec.sourceFiles && rec.sourceFiles[si]) rec.sourceFiles[si].callbackSentAt = new Date().toISOString();
          l[excelFile.name] = rec;
          atomicWrite(STATUS_LOG, JSON.stringify(l, null, 2));
        });
      }
      updateStatusLog({ [`${src.filename}_completedAt`]: completedAt });
      markAsUploaded(src.filename);
    }
    try { fs.unlinkSync(excelFile.fullPath); } catch {}
    markAsUploaded(excelFile.name);
  } else {
    markAsUploaded(excelFile.name);
    const singleQueuedAt = loadStatusLog()[`${excelFile.name}_queuedAt`];
    const singleDurationMs = singleQueuedAt ? Date.now() - new Date(singleQueuedAt).getTime() : null;
    updateStatusLog({
      [excelFile.name]: 'DONE',
      [`${excelFile.name}_completedAt`]: completedAt,
      ...(singleDurationMs != null ? { [`${excelFile.name}_processingDurationMs`]: singleDurationMs } : {}),
    });
    const splitLog = loadStatusLog();
    if (splitLog[`${excelFile.name}_isSplitIntermediate`]) {
      console.log(`✂️  Split Part A "${excelFile.name}" DONE — holding callback until Part B completes`);
      updateStatusLog({ _fileReceived: true });
    } else {
      await sendCallback(excelFile.name, 'DONE', completedAt);
    }
  }

  try { await page.screenshot({ path: `done-${fullBaseName}.png`, timeout: 5000 }); } catch (ssErr) { console.warn(`⚠️  Screenshot failed: ${ssErr.message}`); }
  console.log(`🎉 ${excelFile.name} — DONE!`);
  sendAlert('🎉 MTN GroupShare — Data Purchased', `"${groupName}" group created and beneficiaries uploaded successfully.`);
  return true;
}

async function run() {
  await startServer();

  // Reset any purchase status that got stuck from a previous session:
  //  • IN_PROGRESS  — bot was killed mid-purchase; no active purchase on fresh start
  //  • WAITING_FUNDS — GH¢ was low last session; EVD may have topped up in the meantime,
  //                    so re-attempt immediately rather than waiting for a callback that
  //                    may never arrive (e.g. if it already came while bot was down)
  const stuckPurchaseStatus = loadStatusLog()._purchaseStatus;
  if (stuckPurchaseStatus === 'IN_PROGRESS') {
    console.warn('⚠️  Resetting stale _purchaseStatus IN_PROGRESS → FAILED on startup');
    updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: 'Reset on restart — previous purchase session interrupted' });
  } else if (stuckPurchaseStatus === 'WAITING_FUNDS') {
    console.warn('⚠️  Resetting stale _purchaseStatus WAITING_FUNDS → "" on startup (will re-check GH¢ balance)');
    updateStatusLog({ _purchaseStatus: '', _purchaseNote: 'Reset on restart — will re-attempt purchase if GH¢ is now sufficient' });
  }

  // ── Crash-recovery for in-flight batches ────────────────────────────────
  // If the bot was killed while a merged batch was IN_PROGRESS (browser was
  // interacting with MTN UI) or PROCESSING (file submitted, waiting for MTN),
  // those batches are now orphaned — the polling loop that sets DONE/TIMEOUT
  // died with the process.  Without this recovery they stay locked forever.
  //
  // IN_PROGRESS → PENDING : upload never confirmed; MTN's own "still processing"
  //   banner will block a double-submit if MTN did receive the file.
  // PROCESSING (> 40 min) → TIMEOUT : submission was confirmed; it has been long
  //   enough that MTN would have finished or failed; normal retry path picks up.
  // PROCESSING (≤ 40 min) : leave as-is; getPendingFiles() holds source files
  //   locked; idle loop will age it out once the window passes.
  {
    const BATCH_PROCESSING_TIMEOUT_MS = 50 * 60 * 1000; // 50 min
    const startupLog = loadStatusLog();
    const startupUpdates = {};
    for (const [key, val] of Object.entries(startupLog)) {
      if (val === null || typeof val !== 'object' || !val.sourceFiles || !val.status) continue;
      if (val.status === 'IN_PROGRESS') {
        startupUpdates[key] = { ...val, status: 'PENDING' };
        console.warn(`⚠️  Startup: batch "${key}" was IN_PROGRESS — reset to PENDING (source files re-queued)`);
      } else if (val.status === 'PROCESSING') {
        const queuedMs = new Date(val.queuedAt || val.startedAt || val.createdAt).getTime();
        const ageMin = Math.round((Date.now() - queuedMs) / 60000);
        if (Date.now() - queuedMs > BATCH_PROCESSING_TIMEOUT_MS) {
          startupUpdates[key] = { ...val, status: 'TIMEOUT' };
          console.warn(`⚠️  Startup: batch "${key}" was PROCESSING for ${ageMin} min — marked TIMEOUT`);
        } else {
          console.log(`ℹ️  Startup: batch "${key}" is PROCESSING (${ageMin} min) — source files locked until 40-min window passes`);
        }
      }
    }
    if (Object.keys(startupUpdates).length) updateStatusLog(startupUpdates);
  }

  // ── Startup recovery: incomplete DONE batch source-file marking ──────────
  // If the bot was killed while iterating through a DONE batch's source files
  // (calling markAsUploaded per file), some files may still be on disk and not
  // in the uploaded log.  Scan all DONE batch records and ensure every source
  // file is marked uploaded so they are never re-queued as PENDING.
  {
    const doneLog = loadStatusLog();
    const completionUpdates = {};
    for (const [key, val] of Object.entries(doneLog)) {
      if (val === null || typeof val !== 'object' || !val.sourceFiles || val.status !== 'DONE') continue;
      for (const src of val.sourceFiles) {
        if (!src.filename) continue;
        const alreadyUploaded = loadUploadedLog().includes(src.filename);
        if (!alreadyUploaded) {
          markAsUploaded(src.filename);
          completionUpdates[`${src.filename}_completedAt`] = val.completedAt || doneLog[`${src.filename}_completedAt`] || new Date().toISOString();
          console.warn(`⚠️  Startup: source file "${src.filename}" from DONE batch "${key}" was not in uploaded log — marked uploaded now`);
        }
      }
    }
    if (Object.keys(completionUpdates).length) updateStatusLog(completionUpdates);
  }

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

  // Browser context options — extracted so they can be reused when recreating
  // the context after repeated login failures (stale TCP/DNS state recovery).
  const contextOptions = {
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
  };

  // Helper: tear down the current browser context and build a brand-new one.
  // Called after repeated login failures to clear Chromium's stale TCP/DNS cache.
  async function recreateContext() {
    try { await context.close(); } catch {}
    context = await browser.newContext(contextOptions);
    await context.addInitScript(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    });
    page = await context.newPage();
  }

  // context.addInitScript applies to every page opened from this context,
  // including pages we create fresh on login retries.
  let context = await browser.newContext(contextOptions);
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
  let page = await context.newPage();

  try {
    // Retry initial login indefinitely when the portal is unreachable (service down).
    // Uses increasing backoff: 1 → 2 → 4 → 8 → 16 → 30 min (capped), then holds at 30 min.
    // Non-transient errors (bad credentials, OTP timeout, etc.) are still fatal.
    // On each retry the stale page is closed and a fresh one opened — this clears any
    // accumulated Playwright navigation state from the previous failed attempt.
    let _serviceDownAttempt = 0;
    while (true) {
      try {
        await login(page);
        break; // success — proceed to main loop
      } catch (loginErr) {
        if (TRANSIENT_NAV_ERR.test(loginErr.message)) {
          _serviceDownAttempt++;
          const backoffMins = Math.min(30, Math.pow(2, _serviceDownAttempt - 1));
          console.warn(`⚠️  Initial login failed (attempt ${_serviceDownAttempt}) — portal appears to be down: ${loginErr.message}`);
          console.log(`⏳ Retrying in ${backoffMins} minute(s)...`);
          sendAlert('⚠️ MTN GroupShare — Portal Down?', `Service unreachable (attempt ${_serviceDownAttempt}). Retrying in ${backoffMins} min.`);
          await new Promise(r => setTimeout(r, backoffMins * 60 * 1000));
          // Recreate the entire browser context (not just the page) to clear any
          // stale Chromium TCP/DNS state before the next login attempt.
          await recreateContext();
          // loop and retry
        } else {
          throw loginErr; // non-transient — let fatal handler deal with it
        }
      }
    }

    console.log('\n🔁 Entering main loop — press Ctrl+C to stop.\n');
    let idleCount = 0;

    while (true) {
      // Clear the file-received wake flag only if it was set
      if (loadStatusLog()._fileReceived) updateStatusLog({ _fileReceived: false });

      if (!await isSessionActive(page)) {
        console.log('🔒 Session lost — re-logging in...');
        sendAlert('🔒 MTN GroupShare — Session Expired', 'Session expired. Re-logging in automatically...');
        let reloginOk = false;
        let _reloginAttempt = 0;
        while (!reloginOk) {
          _reloginAttempt++;
          // Every 3rd consecutive failure, recreate the entire browser context to clear
          // any stale Chromium TCP/DNS state that may be causing ERR_EMPTY_RESPONSE
          // even when the portal is actually accessible.
          if (_reloginAttempt > 1 && (_reloginAttempt % 3 === 1)) {
            console.warn(`⚠️  Recreating browser context after ${_reloginAttempt - 1} failed re-login attempt(s)...`);
            await recreateContext();
          } else {
            try { await page.close(); } catch {}
            page = await context.newPage();
          }
          try {
            await login(page);
            reloginOk = true;
          } catch (loginErr) {
            if (!TRANSIENT_NAV_ERR.test(loginErr.message)) throw loginErr; // non-transient — fatal
            const backoffMins = Math.min(30, Math.pow(2, _reloginAttempt - 1));
            console.warn(`⚠️  Re-login failed (attempt ${_reloginAttempt}) — portal may be down: ${loginErr.message}`);
            console.log(`⏳ Retrying in ${backoffMins} minute(s)...`);
            sendAlert('⚠️ MTN GroupShare — Portal Down?', `Re-login failed (attempt ${_reloginAttempt}). Retrying in ${backoffMins} min.`);
            await new Promise(r => setTimeout(r, backoffMins * 60 * 1000));
          }
        }
      }

      // Service any immediate balance refresh requested by the GET /balance API endpoint
      if (loadStatusLog()._balanceRefreshRequested) {
        console.log('💰 Balance refresh requested via API — refreshing now...');
        updateStatusLog({ _balanceRefreshRequested: false });
        await checkBalance(page, context);
      }

      // DEBUG: Navigate to manage-groups → View Beneficiaries (triggered from dashboard button)
      if (loadStatusLog()._debugNavBeneficiaries) {
        const debugFileName = loadStatusLog()._debugNavFileName || null;
        updateStatusLog({ _debugNavBeneficiaries: false });
        console.log(`🐛 [DEBUG] Navigating to manage-groups for View Beneficiaries test${debugFileName ? ` (file: "${debugFileName}")` : ' (first row)'}...`);
        try {
          await gotoWithRetry(page, 'https://up2u.mtn.com.gh/beneficiaries/manage-groups', { waitUntil: 'networkidle', timeout: 900000 });
          console.log('🐛 [DEBUG] manage-groups loaded — saving screenshot...');
          await page.screenshot({ path: 'debug-manage-groups.png', fullPage: true, timeout: 180000 });
          console.log('📸 Screenshot saved — debug-manage-groups.png');

          // If a file name was provided, filter the grid first
          if (debugFileName) {
            console.log(`🐛 [DEBUG] Filtering grid by "${debugFileName}"...`);
            await page.waitForTimeout(2000);
            await page.click('th[data-field="GroupName"] a.k-grid-filter');
            await page.waitForSelector('.k-filter-menu input.k-textbox, .k-filter-menu input[type="text"]', { timeout: 5000 });
            await page.screenshot({ path: 'debug-manage-groups-filter-open.png', fullPage: true, timeout: 180000 });
            console.log('📸 Screenshot saved — debug-manage-groups-filter-open.png');
            await page.fill('.k-filter-menu input.k-textbox, .k-filter-menu input[type="text"]', debugFileName);
            await page.screenshot({ path: 'debug-manage-groups-filter-typed.png', fullPage: true, timeout: 180000 });
            console.log('📸 Screenshot saved — debug-manage-groups-filter-typed.png');
            await page.press('.k-filter-menu input.k-textbox, .k-filter-menu input[type="text"]', 'Enter');
            await page.screenshot({ path: 'debug-manage-groups-filter-clicked.png', fullPage: true, timeout: 180000 });
            console.log('📸 Screenshot saved — debug-manage-groups-filter-clicked.png');
            console.log('🐛 [DEBUG] Filter applied — waiting for grid to refresh (timeout: 10 min)...');
            await page.waitForLoadState('networkidle', { timeout: 600000 });
            await page.screenshot({ path: 'debug-manage-groups-filtered.png', fullPage: true, timeout: 180000 });
            console.log('📸 Screenshot saved — debug-manage-groups-filtered.png');
          }

          // Click the Manage group dropdown button (matching row if filtered, else first row)
          console.log('🐛 [DEBUG] Clicking Manage group dropdown button...');
          const debugManageBtn = page.locator('button[aria-haspopup="true"]', { hasText: 'Manage group' }).first();
          await debugManageBtn.waitFor({ state: 'visible', timeout: 30000 });
          await debugManageBtn.click();
          await page.waitForTimeout(1000);
          await page.screenshot({ path: 'debug-manage-group-dropdown.png', fullPage: true, timeout: 180000 });
          console.log('📸 Screenshot saved — debug-manage-group-dropdown.png');

          console.log('🐛 [DEBUG] Waiting for View Beneficiaries link in dropdown...');
          await page.waitForSelector('a[href*="/beneficiaries/groups/"]', { timeout: 15000 });
          await page.screenshot({ path: 'debug-view-benef-before-click.png', fullPage: true, timeout: 180000 });
          console.log('📸 Screenshot saved — debug-view-benef-before-click.png');

          const debugBenefNavPromise = page.waitForURL('**/beneficiaries/groups/**', { timeout: 900000 });
          await page.click('a[href*="/beneficiaries/groups/"]', { timeout: 600000 });
          await debugBenefNavPromise;
          console.log('🐛 [DEBUG] View Beneficiaries page loaded ✅ — waiting for first row...');
          // Wait for the first row to appear in the grid
          await page.waitForSelector('table.k-grid-table tbody tr, .k-grid tbody tr', { timeout: 900000 });
          await page.screenshot({ path: 'debug-view-benef-loaded.png', fullPage: true, timeout: 180000 });
          console.log('📸 Screenshot saved — debug-view-benef-loaded.png');

          updateStatusLog({ _debugNavResult: 'OK', _debugNavAt: new Date().toISOString() });
        } catch (debugErr) {
          console.error(`🐛 [DEBUG] View Beneficiaries nav failed: ${debugErr.message}`);
          try { await page.screenshot({ path: 'debug-view-benef-error.png', fullPage: true, timeout: 180000 }); } catch {}
          console.log('📸 Screenshot saved — debug-view-benef-error.png');
          updateStatusLog({ _debugNavResult: 'FAILED', _debugNavError: debugErr.message, _debugNavAt: new Date().toISOString() });
        }
        continue;
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
      if (currentBalanceMB <= 90 * 1024 && purchaseStatusNow !== 'IN_PROGRESS' && purchaseStatusNow !== 'WAITING_FUNDS') {
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
        // Age out any PROCESSING batches whose polling window has now passed.
        // This handles restarts where the batch was fresh (< 40 min) and couldn't
        // be aged out at startup — once the window passes the source files unlock.
        {
          const BATCH_PROCESSING_TIMEOUT_MS = 50 * 60 * 1000;
          const idleStatusLog = loadStatusLog();
          const idleUpdates = {};
          for (const [key, val] of Object.entries(idleStatusLog)) {
            if (val === null || typeof val !== 'object' || !val.sourceFiles || val.status !== 'PROCESSING') continue;
            const queuedMs = new Date(val.queuedAt || val.startedAt || val.createdAt).getTime();
            if (Date.now() - queuedMs > BATCH_PROCESSING_TIMEOUT_MS) {
              idleUpdates[key] = { ...val, status: 'TIMEOUT' };
              console.warn(`⚠️  Idle: batch "${key}" was PROCESSING for ${Math.round((Date.now() - queuedMs)/60000)} min — marked TIMEOUT; source files unlocked for retry`);
            }
          }
          if (Object.keys(idleUpdates).length) updateStatusLog(idleUpdates);
        }

        idleCount++;
        const idleLog = loadStatusLog();
        const idleBalanceMB = idleLog._lastBalanceMB || 0;

        // Even with no pending files, trigger auto-purchase if balance is below threshold.
        // This ensures stock is replenished before the next batch of files arrives.
        {
          const idlePurchaseStatus = idleLog._purchaseStatus;
          if (idleBalanceMB > 0 && idleBalanceMB <= 90 * 1024
              && idlePurchaseStatus !== 'IN_PROGRESS'
              && idlePurchaseStatus !== 'WAITING_FUNDS') {
            console.log(`💳 [Idle] Balance is ≤ 90 GB (${(idleBalanceMB / 1024).toFixed(2)} GB) — triggering auto-purchase while idle...`);
            sendAlert('💳 MTN GroupShare — Auto-Purchase (Idle)', `Balance is ${(idleBalanceMB / 1024).toFixed(2)} GB. Purchasing 1.5 TB bundle.`);
            updateStatusLog({ _purchaseStatus: 'IN_PROGRESS' });
            try {
              await purchaseData(page, context);
            } catch (purchaseErr) {
              console.error(`❌ Idle auto-purchase failed: ${purchaseErr.message}`);
              sendAlert('❌ MTN GroupShare — Idle Auto-Purchase Failed', purchaseErr.message);
              updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: purchaseErr.message, _purchaseCompletedAt: new Date().toISOString() });
            }
            continue;
          }
        }

        // Keep-alive: reload the portal page if we haven't navigated there recently.
        // Prevents the MTN portal from killing the browser session due to inactivity.
        if (Date.now() - _lastPortalNavAt >= KEEP_ALIVE_INTERVAL_MS) {
          try {
            console.log(`🫀 Keep-alive: reloading portal page (last nav ${Math.round((Date.now() - _lastPortalNavAt) / 1000)}s ago)...`);
            await page.goto('https://up2u.mtn.com.gh', { waitUntil: 'load', timeout: 30000 });
            _lastPortalNavAt = Date.now();
          } catch (kaErr) {
            console.warn(`⚠️  Keep-alive reload failed: ${kaErr.message}`);
          }
        }

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

      // ── Fetch effective balance once for this scan cycle ─────────────────
      const { totalMB: apiBalanceMB } = await checkBalance(page, context);
      const availableMB = apiBalanceMB;

      // ── Check balance threshold before building batch ─────────────────────
      const purchaseStatusInLoop = loadStatusLog()._purchaseStatus;
      if (availableMB <= AUTO_PURCHASE_THRESHOLD_MB && purchaseStatusInLoop !== 'IN_PROGRESS' && purchaseStatusInLoop !== 'WAITING_FUNDS') {
        console.log(`💳 Balance is ≤ 90 GB (${(availableMB / 1024).toFixed(2)} GB) — triggering auto-purchase before next batch...`);
        sendAlert('💳 MTN GroupShare — Auto-Purchase', `Balance dropped to ${(availableMB / 1024).toFixed(2)} GB. Purchasing 1.5 TB bundle.`);
        updateStatusLog({ _purchaseStatus: 'IN_PROGRESS' });
        autoPurchaseTriggered = true;
      }

      if (!autoPurchaseTriggered) {
        // ── Separate split parts from normal files ────────────────────────────
        // Split intermediates (Part A) and split finals (Part B/B-A/etc.) must
        // NEVER be merged with other files.  They were sized to fit the balance
        // at split-time and carry their own callback logic.  Process the oldest
        // split part solo first; only bin-pack regular files when none exist.
        const cycleLog   = loadStatusLog();
        const splitParts = pendingFiles.filter(f =>
          cycleLog[`${f.name}_isSplitIntermediate`] || cycleLog[`${f.name}_isSplitFinal`]
        );
        // Zip-extracted files carry a fixed allocation in their filename and must
        // each create their own group — never merge them with anything.
        const zipParts = pendingFiles.filter(f =>
          !cycleLog[`${f.name}_isSplitIntermediate`] && !cycleLog[`${f.name}_isSplitFinal`] &&
          cycleLog[`${f.name}_isZipExtracted`]
        );
        const regularFiles = pendingFiles.filter(f =>
          !cycleLog[`${f.name}_isSplitIntermediate`] && !cycleLog[`${f.name}_isSplitFinal`] &&
          !cycleLog[`${f.name}_isZipExtracted`]
        );

        // Priority: split parts first, then zip-extracted files (oldest first),
        // then bin-pack regular files when neither category has waiting files.
        // Always pick Part A (isSplitIntermediate) before Part B (isSplitFinal).
        const filesToPack = (splitParts.length > 0 || zipParts.length > 0) ? [] : regularFiles;
        const forceSingleFile = splitParts.length > 0
          ? (splitParts.find(f => cycleLog[`${f.name}_isSplitIntermediate`]) || splitParts[0])
          : zipParts.length > 0
            ? zipParts[0] // oldest zip file first (already sorted by mtime)
            : null;

        // ── Optimal bin-pack: maximise balance consumption ────────────────────
        // findOptimalBatch picks the combination of files that uses as much of
        // availableMB as possible so the post-upload leftover is < 90 GB and
        // auto-purchase triggers automatically.
        const batch = forceSingleFile ? [forceSingleFile] : findOptimalBatch(filesToPack, availableMB);
        const batchMB = batch.reduce((s, f) => s + f.totalMB, 0);
        skippedDueToBalance = pendingFiles.filter(f => !batch.includes(f)).length;

        if (batch.length === 0) {
          skippedDueToBalance = pendingFiles.length;

          // ── Balance-drain deadlock breaker ────────────────────────────────
          // No file fits AND balance is above the auto-purchase threshold, so
          // the system would loop idle forever.  Split the smallest pending
          // file into Part A (fits now) + Part B (uploads after replenishment).
          // Only runs when explicitly enabled from the dashboard (_splitEnabled).
          if (availableMB > AUTO_PURCHASE_THRESHOLD_MB && loadStatusLog()._splitEnabled === true) {
            const splitLog = loadStatusLog();
            const splitCandidate = [...pendingFiles]
              .filter(f => f.totalMB > 0
                && !splitLog[`${f.name}_isSplitIntermediate`]  // don't re-split Part A (still in-flight)
                && !splitLog[`${f.name}_startedAt`])            // never split a file that was ever submitted to MTN — the upload may have landed even if the bot lost the redirect; splitting would cause double-processing
              .sort((a, b) => a.totalMB - b.totalMB)[0]; // smallest file = least overshoot
            if (splitCandidate) {
              try {
                splitFileToFitBalance(splitCandidate, availableMB);
                console.log(`✂️  Split triggered on "${splitCandidate.name}" — restarting scan to process Part A...`);
                continue; // re-scan: Part A is now a valid pending file
              } catch (splitErr) {
                console.warn(`⚠️  File split failed for "${splitCandidate.name}": ${splitErr.message}`);
                sendAlert('⚠️ MTN GroupShare — Split Failed', `Could not split "${splitCandidate.name}": ${splitErr.message}`);
              }
            } else {
              // All oversized pending files were previously submitted to MTN — splitting them
              // risks double-processing.  Alert for manual intervention.
              const blockedNames = pendingFiles
                .filter(f => f.totalMB > 0 && splitLog[`${f.name}_startedAt`])
                .map(f => f.name)
                .join(', ');
              if (blockedNames) {
                const msg = `Balance (${(availableMB / 1024).toFixed(2)} GB) is too low for pending file(s) but they were previously submitted — cannot split safely. Manual check required: ${blockedNames}`;
                console.warn(`⚠️  ${msg}`);
                sendAlert('⚠️ MTN GroupShare — Previously-Submitted File Blocked', msg);
              }
            }
          }
        } else {
          if (!await isSessionActive(page)) {
            console.log('🔒 Session lost before upload — re-logging in...');
            let reloginOk = false;
            let _preUploadReloginAttempt = 0;
            while (!reloginOk) {
              _preUploadReloginAttempt++;
              if (_preUploadReloginAttempt > 1 && (_preUploadReloginAttempt % 3 === 1)) {
                console.warn(`⚠️  Recreating browser context after ${_preUploadReloginAttempt - 1} failed re-login attempt(s)...`);
                await recreateContext();
              } else {
                try { await page.close(); } catch {}
                page = await context.newPage();
              }
              try {
                await login(page);
                reloginOk = true;
              } catch (loginErr) {
                if (!TRANSIENT_NAV_ERR.test(loginErr.message)) throw loginErr;
                const backoffMins = Math.min(30, Math.pow(2, _preUploadReloginAttempt - 1));
                console.warn(`⚠️  Re-login failed before upload (attempt ${_preUploadReloginAttempt}): ${loginErr.message}`);
                sendAlert('⚠️ MTN GroupShare — Portal Down?', `Re-login failed before upload (attempt ${_preUploadReloginAttempt}). Retrying in ${backoffMins} min.`);
                await new Promise(r => setTimeout(r, backoffMins * 60 * 1000));
              }
            }
          }

          // ── Fresh balance check right before upload ───────────────────────
          const { totalMB: freshBalanceMB } = await checkBalance(page, context);
          console.log(`💰 Pre-upload balance: ${(freshBalanceMB / 1024).toFixed(2)} GB (batch needs ${(batchMB / 1024).toFixed(2)} GB)`);
          if (batchMB > freshBalanceMB) {
            console.warn(`⚠️  Balance dropped since scan — batch (${(batchMB / 1024).toFixed(2)} GB) exceeds fresh balance (${(freshBalanceMB / 1024).toFixed(2)} GB). Skipping upload this cycle.`);
            sendAlert('⚠️ MTN GroupShare — Balance Changed', `Batch of ${(batchMB / 1024).toFixed(2)} GB skipped — fresh balance is only ${(freshBalanceMB / 1024).toFixed(2)} GB.`);
            await interruptibleSleep(IDLE_REFRESH_INTERVAL);
            continue;
          }

          // ── Build merged file (or use single file directly if only one fits) ─
          let fileToUpload;
          const mergeLog = loadStatusLog();
          const batchHasSplitPart = batch.some(f =>
            mergeLog[`${f.name}_isSplitIntermediate`] || mergeLog[`${f.name}_isSplitFinal`]
          );
          if (batch.length === 1 || batchHasSplitPart) {
            // Always upload split parts standalone — never merge them
            if (batchHasSplitPart && batch.length > 1) {
              console.warn(`⚠️  Split part found in multi-file batch — isolating to standalone upload to prevent double-processing`);
            }
            fileToUpload = batchHasSplitPart
              ? batch.find(f => mergeLog[`${f.name}_isSplitIntermediate`] || mergeLog[`${f.name}_isSplitFinal`])
              : batch[0];
            console.log(`\n📌 Single file batch — uploading directly: ${fileToUpload.name}`);
          } else {
            console.log(`\n📎 Merging ${batch.length} file(s) into one batch (${(batchMB / 1024).toFixed(2)} GB total):`);
            batch.forEach((f, idx) => console.log(`   ${idx + 1}. ${f.name} — ${(f.totalMB / 1024).toFixed(2)} GB`));
            const { mergedFile } = buildMergedFile(batch);
            fileToUpload = mergedFile;
          }

          const uploadResult = await uploadFile(page, fileToUpload);

          if (uploadResult && uploadResult.blocked) {
            console.warn('⏳ MTN is still processing a previous upload. Stopping batch — will retry all pending files next scan.');
          } else if (uploadResult && uploadResult.error) {
            console.warn('⚠️ Upload navigation error — will retry next scan.');
          } else if (uploadResult === true) {
            anyFileUploaded = true;
            updateStatusLog({ _balanceInsufficient: false });
          } else {
            console.warn(`⚠️ Upload did not confirm DONE — will retry next scan.`);
          }
        }
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
        const latestBalanceMB = loadStatusLog()._lastBalanceMB || 0;
        const availableGB = (latestBalanceMB / 1024).toFixed(2);
        updateStatusLog({ _balanceInsufficient: true });

        if (latestBalanceMB <= AUTO_PURCHASE_THRESHOLD_MB) {
          // Balance is below purchase threshold but purchase didn't trigger — status log
          // may be stuck. Force a reset so next cycle re-triggers the purchase.
          console.warn(`⚠️  Balance (${availableGB} GB) is below 90 GB but auto-purchase did not trigger — resetting purchase status for next cycle.`);
          updateStatusLog({ _purchaseStatus: 'FAILED', _purchaseNote: 'Force-reset — balance below threshold but purchase blocked by stale status' });
        } else {
          const msg = `All ${skippedDueToBalance} pending file(s) exceed available balance (${availableGB} GB). `
            + `Send files with total allocation ≤ ${availableGB} GB to drain balance below 90 GB and trigger an auto-purchase.`;
          console.warn(`⚠️  ${msg}`);
          sendAlert('⚠️ MTN GroupShare — Queue Blocked', msg);
        }
      }

      console.log(`\n⏳ Batch complete. Next scan in 25 sec...`);
      await interruptibleSleep(IDLE_REFRESH_INTERVAL);
    }

  } catch (err) {
    console.error('❌ Fatal error:', err.message);
    sendAlert('❌ MTN GroupShare — Fatal Error', err.message);
    try { await page.screenshot({ path: 'error-state.png' }); } catch {}
    // Signal the outer watchdog that a fatal crash occurred
    lastFatalCrashAt = Date.now();
  } finally {
    try { await page.waitForTimeout(5000); } catch {}
    await browser.close();
  }
}

// ── Crash watchdog ───────────────────────────────────────────────────────────
// If run() crashes and the error has not self-resolved within 10 minutes,
// exit the process so the host (Render) restarts the service automatically.
let lastFatalCrashAt = null;
const CRASH_RECOVERY_WINDOW_MS = 10 * 60 * 1000; // 10 minutes

async function runWithWatchdog() {
  while (true) {
    lastFatalCrashAt = null;
    await run();

    if (lastFatalCrashAt === null) {
      // run() returned without a fatal crash (e.g. clean shutdown) — stop
      break;
    }

    // Fatal crash occurred — wait out the remaining recovery window then restart
    const waitMs = Math.max(0, CRASH_RECOVERY_WINDOW_MS - (Date.now() - lastFatalCrashAt));
    if (waitMs > 0) {
      console.log(`⏳ Fatal error detected — waiting ${Math.round(waitMs / 1000)}s before forcing restart...`);
      await new Promise(r => setTimeout(r, waitMs));
    }

    console.error('🔴 Fatal error unrecovered after 10 minutes — exiting for process restart');
    sendAlert('🔴 MTN GroupShare — Restarting', 'Fatal error unrecovered after 10 minutes. Process exiting for automatic restart.');
    process.exit(1);
  }
}

runWithWatchdog();
