const { chromium } = require('playwright');
const { waitForOTP, startServer } = require('./otp-server');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
const notifier = require('node-notifier');
require('dotenv').config();

const UPLOADED_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.uploaded.json');
const IDLE_REFRESH_INTERVAL = 3 * 60 * 1000;
const STATUS_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.status.json');

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
  const files = fs.readdirSync(folderPath)
    .filter(f => (f.endsWith('.xlsx') || f.endsWith('.xls')) && !uploaded.includes(f))
    .map(f => ({
      name: f,
      fullPath: path.join(folderPath, f),
      mtime: fs.statSync(path.join(folderPath, f)).mtime,
    }))
    .sort((a, b) => a.mtime - b.mtime);

  return files;
}

function parseBalanceToMB(balanceText) {
  let totalMB = 0;
  const gbMatch = balanceText.match(/([\d.]+)\s*GB/i);
  const mbMatch = balanceText.match(/([\d.]+)\s*MB/i);
  if (gbMatch) totalMB += parseFloat(gbMatch[1]) * 1024;
  if (mbMatch) totalMB += parseFloat(mbMatch[1]);
  return totalMB;
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

      await page.screenshot({ path: 'login-debug.png', fullPage: true });
      console.log('📸 Screenshot saved — login-debug.png');

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

    if (attempt < maxAttempts) await page.waitForTimeout(pollInterval);
  }

  if (!isDone) {
    updateStatusLog({
      [excelFile.name]: 'TIMEOUT',
      [`${excelFile.name}_timedOutAt`]: new Date().toISOString(),
    });

    console.warn(`⚠️  Timed out: ${excelFile.name} — will retry next run`);
    sendAlert(
      '⚠️ MTN GroupShare — Processing Timeout',
      `"${excelFile.name}" did not reach DONE within 35 minutes. Please check the portal manually.`
    );
    await page.screenshot({ path: `timeout-${fileName}.png` });
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

      const pendingFiles = getPendingFiles(process.env.EXCEL_FOLDER_PATH);

      if (pendingFiles.length === 0) {
        idleCount++;
        const { balanceText, totalMB } = await checkBalance(page);
        console.log(`😴 [${new Date().toLocaleTimeString()}] Idle #${idleCount} — No pending files. Balance: ${balanceText} (${totalMB.toFixed(2)} MB). Next check in 3 mins...`);
        await page.waitForTimeout(IDLE_REFRESH_INTERVAL);
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
        await uploadFile(page, pendingFiles[i]);
      }

      console.log(`\n⏳ Batch complete. Next scan in 3 mins...`);
      await page.waitForTimeout(IDLE_REFRESH_INTERVAL);
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