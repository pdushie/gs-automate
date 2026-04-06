const { chromium } = require('playwright');
const { waitForOTP, startServer, resetOtpState } = require('./otp-server');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
const notifier = require('node-notifier');
require('dotenv').config();

const UPLOADED_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.uploaded.json');
const IDLE_REFRESH_INTERVAL = 3 * 60 * 1000;
const STATUS_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.status.json');
const MAX_FILE_RETRIES = 3;

function loadStatusLog() {
  if (fs.existsSync(STATUS_LOG)) return JSON.parse(fs.readFileSync(STATUS_LOG, 'utf8'));
  return {};
}

function updateStatusLog(updates) {
  const log = loadStatusLog();
  Object.assign(log, updates);
  fs.writeFileSync(STATUS_LOG, JSON.stringify(log, null, 2));
}

function sendAlert(title, message) {
  console.warn(`🔔 ALERT: ${title} — ${message}`);
  notifier.notify({ title, message, sound: true, wait: false });
}

// Sleep for `ms` ms, but wake every `checkIntervalMs` to check for a balance refresh request.
// Returns true if woken early by the flag, false if the full duration elapsed.
async function interruptibleSleep(ms, checkIntervalMs = 15000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const remaining = end - Date.now();
    await new Promise(r => setTimeout(r, Math.min(checkIntervalMs, remaining)));
    if (loadStatusLog()._balanceRefreshRequested) return true;
  }
  return false;
}

function loadUploadedLog() {
  if (fs.existsSync(UPLOADED_LOG)) {
    return JSON.parse(fs.readFileSync(UPLOADED_LOG, 'utf8'));
  }
  return [];
}

function markAsUploaded(fileName) {
  const log = loadUploadedLog();
  if (!log.includes(fileName)) {
    log.push(fileName);
    fs.writeFileSync(UPLOADED_LOG, JSON.stringify(log, null, 2));
    console.log(`📝 Marked as uploaded: ${fileName}`);
  }
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
  let attempts = 0;
  const maxLoginAttempts = 3;

  while (attempts < maxLoginAttempts) {
    attempts++;
    console.log(`\n🔐 Login attempt ${attempts}/${maxLoginAttempts}...`);

    try {
      await page.goto('https://up2u.mtn.com.gh/account/login', {
        waitUntil: 'domcontentloaded',
        timeout: 60000
      });

      //await page.screenshot({ path: 'login-debug.png', fullPage: true });
      //console.log('📸 Screenshot saved — login-debug.png');

      await page.waitForSelector('#disclaimer-btn', { timeout: 30000 });
      await page.waitForTimeout(500);
      await page.dispatchEvent('#disclaimer-btn', 'click');
      console.log('✅ Disclaimer accepted');

      await page.waitForSelector('input[name="Msisdn"]', { timeout: 10000 });
      await page.fill('input[name="Msisdn"]', process.env.MTN_PHONE);
      await page.fill('input[name="Pin"]', process.env.MTN_PIN);
      await page.dispatchEvent('#login-btn', 'click');
      console.log('🚀 Login clicked');

      await page.waitForURL('**/account/verify-otp', { timeout: 15000 });
      console.log('✅ OTP page — waiting for SMS...');

      resetOtpState(); // clear any lingering state from a previous attempt
      const otp = await waitForOTP(180000); // 3 minutes
      console.log(`✅ OTP received: ${otp}`);
      await page.fill('input[name="OTPCode"]', otp);
      await page.dispatchEvent('#login-btn', 'click');

      await page.waitForNavigation({ waitUntil: 'networkidle', timeout: 15000 });

      if (await isSessionActive(page)) {
        console.log('🎉 Login successful:', page.url());
        return true;
      } else {
        console.warn('⚠️  Login seemed to succeed but session not active — retrying...');
      }

    } catch (err) {
      console.error(`❌ Login attempt ${attempts} failed:`, err.message);
      if (attempts < maxLoginAttempts) {
        console.log('⏳ Waiting 10s before retry...');
        await page.waitForTimeout(10000);
      }
    }
  }

  sendAlert('❌ MTN GroupShare — Login Failed', `Failed to log in after ${maxLoginAttempts} attempts. Please check credentials.`);
  throw new Error(`Login failed after ${maxLoginAttempts} attempts`);
}

async function checkBalance(page) {
  console.log('\n💰 Checking data balance...');
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

function getExcelTotalMB(filePath) {
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

  await page.waitForSelector('button.k-upload-selected', { timeout: 10000 });
  await page.click('button.k-upload-selected');
  console.log('✅ Upload clicked');

  await page.waitForURL('**/beneficiaries**', { timeout: 30000 });
  console.log('✅ Beneficiaries page loaded');

  await page.waitForSelector('#uploadList', { timeout: 10000 });
  await page.click('#uploadList');
  console.log('✅ Share clicked');

  await page.waitForSelector('.uk-button-primary:has-text("Ok")', { timeout: 10000 });
  await page.waitForTimeout(500);
  await page.click('.uk-button-primary:has-text("Ok")');
  console.log('✅ Confirmation accepted');

  await page.waitForURL('**/upload/upload-status', { timeout: 15000 });
  console.log('✅ Status page loaded — polling for DONE...');

  updateStatusLog({ [excelFile.name]: 'PROCESSING' });

  const maxAttempts = 70;
  const pollInterval = 30000;
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
      markAsUploaded(excelFile.name);

      updateStatusLog({
        [excelFile.name]: 'DONE',
        [`${excelFile.name}_completedAt`]: new Date().toISOString(),
      });

      await page.screenshot({ path: `done-${fileName}.png` });
      console.log(`🎉 ${excelFile.name} — DONE!`);
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
      if (!await isSessionActive(page)) {
        console.log('🔒 Session lost — re-logging in...');
        sendAlert('🔒 MTN GroupShare — Session Expired', 'Session expired. Re-logging in automatically...');
        await login(page);
      }

      // Service any immediate balance refresh requested by the GET /balance API endpoint
      let freshBalanceJustFetched = false;
      if (loadStatusLog()._balanceRefreshRequested) {
        console.log('💰 Balance refresh requested via API — refreshing now...');
        updateStatusLog({ _balanceRefreshRequested: false });
        await checkBalance(page);
        freshBalanceJustFetched = true;
      }

      const pendingFiles = getPendingFiles(process.env.EXCEL_FOLDER_PATH);

      if (pendingFiles.length === 0) {
        idleCount++;
        if (!freshBalanceJustFetched) {
          const { balanceText, totalMB } = await checkBalance(page);
          console.log(`😴 [${new Date().toLocaleTimeString()}] Idle #${idleCount} — No pending files. Balance: ${balanceText} (${totalMB.toFixed(2)} MB). Next check in 3 mins...`);
        } else {
          console.log(`😴 [${new Date().toLocaleTimeString()}] Idle #${idleCount} — No pending files. Next check in 3 mins...`);
        }
        await interruptibleSleep(IDLE_REFRESH_INTERVAL);
        continue;
      }

      idleCount = 0;
      console.log(`\n📂 ${pendingFiles.length} new file(s) detected!`);

      for (let i = 0; i < pendingFiles.length; i++) {
        console.log(`\n📌 File ${i + 1} of ${pendingFiles.length}: ${pendingFiles[i].name}`);

        if (!await isSessionActive(page)) {
          console.log('🔒 Session lost before upload — re-logging in...');
          await login(page);
        }

        const { balanceText, totalMB: availableMB } = await checkBalance(page);
        const requiredMB = getExcelTotalMB(pendingFiles[i].fullPath);

        console.log(`💰 Available : ${availableMB.toFixed(2)} MB (${(availableMB / 1024).toFixed(2)} GB)`);
        console.log(`📊 Required  : ${requiredMB.toFixed(2)} MB (${(requiredMB / 1024).toFixed(2)} GB)`);

        if (requiredMB > availableMB) {
          const shortfall = (requiredMB - availableMB).toFixed(2);
          sendAlert(
            '⚠️ MTN GroupShare — Insufficient Balance',
            `Cannot upload "${pendingFiles[i].name}".\nRequired: ${requiredMB.toFixed(2)} MB\nAvailable: ${availableMB.toFixed(2)} MB\nShortfall: ${shortfall} MB\nPlease top up and try again.`
          );
          console.warn(`⚠️  Skipping — will recheck in 3 mins.`);
          continue;
        }

        console.log(`✅ Balance sufficient — proceeding...`);
        const uploadResult = await uploadFile(page, pendingFiles[i]);
        if (uploadResult && uploadResult.blocked) {
          console.warn('⏳ MTN is still processing a previous upload. Stopping batch — will retry all pending files next scan.');
          break;
        }
      }

      console.log(`\n⏳ Batch complete. Next scan in 3 mins...`);
      await interruptibleSleep(IDLE_REFRESH_INTERVAL);
    }

  } catch (err) {
    console.error('❌ Fatal error:', err.message);
    sendAlert('❌ MTN GroupShare — Fatal Error', err.message);
    await page.screenshot({ path: 'error-state.png' });
  } finally {
    await page.waitForTimeout(5000);
    await browser.close();
  }
}

run();