const express = require('express');
const multer = require('multer');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
require('dotenv').config();
const { withFileLock, atomicWrite } = require('./lock');
const crypto      = require('crypto');
const TelegramBot = require('node-telegram-bot-api');


const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));


const UPLOADED_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.uploaded.json');
const STATUS_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.status.json');
const RETENTION_HOURS = parseInt(process.env.FILE_RETENTION_HOURS || '24');


// ── AUTH ─────────────────────────────────────────────────
// Dashboard access is protected by a Telegram-delivered OTP.
// If TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set, auth is bypassed (dev mode).
const tgAuthBot    = process.env.TELEGRAM_BOT_TOKEN   ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN,   { polling: false }) : null;
const tgAuthChatId = process.env.TELEGRAM_CHAT_ID     || null;
const tgAuthBot2   = process.env.TELEGRAM_BOT_TOKEN_2 ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN_2, { polling: false }) : null;
const tgAuthChatId2 = process.env.TELEGRAM_CHAT_ID_2  || null;

// In-memory stores — reset on process restart (forces re-login, no db dependency)
const _otpStore     = new Map(); // 'global' -> { code, expiresAt, used }
const _sessionStore = new Map(); // token     -> expiresAt
const _rateStore    = new Map(); // ip        -> timestamps[]

const OTP_TTL_MS     =  5 * 60 * 1000;      // 5 minutes
const SESSION_TTL_MS =  8 * 60 * 60 * 1000; // 8 hours
const RATE_MAX       = 3;
const RATE_WIN_MS    = 15 * 60 * 1000;      // 15 minutes

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of _otpStore)     if (v.expiresAt < now) _otpStore.delete(k);
  for (const [k, v] of _sessionStore) if (v < now)           _sessionStore.delete(k);
  for (const [k, ts] of _rateStore) {
    const fresh = ts.filter(t => now - t < RATE_WIN_MS);
    if (!fresh.length) _rateStore.delete(k); else _rateStore.set(k, fresh);
  }
}, 60_000);

function getClientIp(req) {
  return (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || 'unknown';
}

function parseCookies(req) {
  const result = {};
  for (const part of (req.headers.cookie || '').split(';')) {
    const idx = part.indexOf('=');
    if (idx < 0) continue;
    const k = part.slice(0, idx).trim();
    const v = part.slice(idx + 1).trim();
    try { result[k] = decodeURIComponent(v); } catch { result[k] = v; }
  }
  return result;
}

// Returns true when the browser session cookie is valid (set after OTP login).
function isAuthenticated(req) {
  if (!tgAuthBot || !tgAuthChatId) return true; // auth disabled — no bot configured
  const token  = parseCookies(req)['gs_session'];
  if (!token) return false;
  const expiry = _sessionStore.get(token);
  if (!expiry || Date.now() > expiry) { _sessionStore.delete(token); return false; }
  return true;
}

// Only used to protect the dashboard HTML page — NOT API endpoints.
function requireAuth(req, res, next) {
  if (isAuthenticated(req)) return next();
  res.redirect('/login');
}


// Auto-create upload folder if it doesn't exist
const uploadFolder = process.env.EXCEL_FOLDER_PATH;
if (uploadFolder && !fs.existsSync(uploadFolder)) {
  fs.mkdirSync(uploadFolder, { recursive: true });
  console.log(`📁 Created upload folder: ${uploadFolder}`);
}


// Save uploaded files directly into the watch folder
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, process.env.EXCEL_FOLDER_PATH),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    const base = path.basename(file.originalname, ext);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    cb(null, `${base}-${timestamp}${ext}`);
  }
});


const upload = multer({
  storage,
  fileFilter: (req, file, cb) => {
    if (file.originalname.endsWith('.xlsx') || file.originalname.endsWith('.xls')) {
      cb(null, true);
    } else {
      cb(new Error('Only .xlsx and .xls files are allowed'));
    }
  }
});


// ── HELPERS ───────────────────────────────────────────────


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


function loadStatusLog() {
  try {
    if (fs.existsSync(STATUS_LOG)) return JSON.parse(fs.readFileSync(STATUS_LOG, 'utf8'));
  } catch {
    try {
      if (fs.existsSync(STATUS_LOG)) return JSON.parse(fs.readFileSync(STATUS_LOG, 'utf8'));
    } catch {}
  }
  return {};
}


// Atomic write — always called from within a withFileLock block
function saveStatusLog(statusLog) {
  atomicWrite(STATUS_LOG, JSON.stringify(statusLog, null, 2));
}

function updateStatusLog(updates) {
  withFileLock(STATUS_LOG, () => {
    const log = loadStatusLog();
    saveStatusLog({ ...log, ...updates });
  });
}


function getDiskUsage() {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  try {
    const files = fs.readdirSync(folderPath);
    let totalBytes = 0;
    let fileCount = 0;

    for (const file of files) {
      if (file.startsWith('.')) continue; // skip hidden log files
      const filePath = path.join(folderPath, file);
      const stat = fs.statSync(filePath);
      if (stat.isFile()) {
        totalBytes += stat.size;
        fileCount++;
      }
    }

    return {
      totalBytes,
      totalKB: Math.round(totalBytes / 1024),
      totalMB: parseFloat((totalBytes / (1024 * 1024)).toFixed(2)),
      fileCount,
    };
  } catch (err) {
    return { totalBytes: 0, totalKB: 0, totalMB: 0, fileCount: 0 };
  }
}


function cleanupOldFiles() {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  const uploaded = loadUploadedLog();
  const statusLog = loadStatusLog();
  const now = Date.now();
  const retentionMs = RETENTION_HOURS * 60 * 60 * 1000;

  let deleted = 0;
  let skipped = 0;
  const deletedFiles = [];

  try {
    const files = fs.readdirSync(folderPath)
      .filter(f => f.endsWith('.xlsx') || f.endsWith('.xls'));

    for (const file of files) {
      const isDone = uploaded.includes(file) || statusLog[file] === 'DONE';

      // Never delete files that are still PENDING or being processed
      if (!isDone) {
        skipped++;
        continue;
      }

      const completedAt = statusLog[file + '_completedAt'];
      const completedTime = completedAt ? new Date(completedAt).getTime() : null;

      // Fall back to file modified time if completedAt is missing
      const filePath = path.join(folderPath, file);
      const fileMtime = fs.statSync(filePath).mtimeMs;
      const ageMs = now - (completedTime || fileMtime);
      const ageHours = Math.round(ageMs / 3600000);

      if (ageMs >= retentionMs) {
        fs.unlinkSync(filePath);
        deletedFiles.push({ filename: file, ageHours });
        deleted++;
        console.log(`🗑️  Cleaned up: ${file} (age: ${ageHours}h)`);
      }
    }

    // Persist updated status log if anything was deleted.
    // Re-read inside the lock to pick up any bot writes that happened during the loop.
    if (deleted > 0) {
      withFileLock(STATUS_LOG, () => {
        const freshLog = loadStatusLog();
        for (const { filename } of deletedFiles) {
          delete freshLog[filename];
          delete freshLog[filename + '_queuedAt'];
          delete freshLog[filename + '_completedAt'];
          delete freshLog[filename + '_startedAt'];
          delete freshLog[filename + '_timedOutAt'];
          delete freshLog[filename + '_retryCount'];
          delete freshLog[filename + '_failedAt'];
        }
        saveStatusLog(freshLog);
      });
      console.log(`✅ Cleanup complete — deleted: ${deleted}, skipped (pending): ${skipped}`);
    } else {
      console.log(`🧹 Cleanup ran — nothing to delete, skipped (pending): ${skipped}`);
    }

    return { deleted, skipped, deletedFiles };
  } catch (err) {
    console.error('❌ Cleanup error:', err.message);
    return { deleted: 0, skipped: 0, deletedFiles: [], error: err.message };
  }
}


// ── ROUTES ────────────────────────────────────────────────


// Returns the sum of data (MB) across all pending (not-yet-processed) files in the queue.
// Uses the bot's cached _totalMB values from the status log to avoid re-parsing XLSX.
// If availableMB is provided, files that individually exceed it are excluded from the total —
// they are stuck until a purchase happens and should not block incoming files that DO fit.
function getPendingQueueTotalMB(statusLog, uploadedLog, availableMB = 0) {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  if (!folderPath || !fs.existsSync(folderPath)) return 0;

  const DONE_STATES = new Set(['DONE', 'ABANDONED', 'FAILED']);
  let totalMB = 0;

  const files = fs.readdirSync(folderPath)
    .filter(f => (f.endsWith('.xlsx') || f.endsWith('.xls')) && !f.startsWith('NM-merged-'));

  for (const file of files) {
    if (uploadedLog.includes(file)) continue;
    if (DONE_STATES.has(statusLog[file])) continue;

    let fileMB = 0;
    if (statusLog[`${file}_totalMB`] != null) {
      fileMB = statusLog[`${file}_totalMB`];
    } else {
      // Cache miss — parse the file directly
      const { totalDataGB } = getExcelStats(path.join(folderPath, file));
      if (totalDataGB != null) fileMB = totalDataGB * 1024;
    }

    // Skip stuck files — they exceed available balance and cannot be processed
    // until a bundle purchase happens. Don't let them block new incoming files.
    if (availableMB > 0 && fileMB > availableMB) continue;

    totalMB += fileMB;
  }

  return totalMB;
}

// Parse a file buffer (base64-decoded) to get its data total from column 4 (DATA_MB).
function getFileTotalMBFromBuffer(buffer) {
  try {
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rawRows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
    let totalMB = 0;
    for (let r = 1; r < rawRows.length; r++) {
      const val = parseFloat(rawRows[r][3]) || 0;
      if (val > 0) totalMB += val;
    }
    return totalMB;
  } catch {
    return 0;
  }
}





// ── ROUTES ────────────────────────────────────────────────

// GET / — dashboard (requires authentication)
app.get('/', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// GET /login — login page (public)
app.get('/login', (req, res) => {
  if (isAuthenticated(req)) return res.redirect('/');
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});


// ── AUTH ROUTES ───────────────────────────────────────────

// POST /auth/request-otp — generate a 6-digit OTP and send it to Telegram
app.post('/auth/request-otp', (req, res) => {
  if (!tgAuthBot || !tgAuthChatId) {
    return res.json({ success: true, note: 'Auth not configured — access is open' });
  }

  const ip  = getClientIp(req);
  const now = Date.now();
  const hits = (_rateStore.get(ip) || []).filter(t => now - t < RATE_WIN_MS);
  if (hits.length >= RATE_MAX) {
    const retryAfterSecs = Math.ceil((hits[0] + RATE_WIN_MS - now) / 1000);
    return res.status(429)
      .set('Retry-After', String(retryAfterSecs))
      .json({ success: false, error: 'TOO_MANY_REQUESTS', retryAfterSeconds: retryAfterSecs });
  }
  hits.push(now);
  _rateStore.set(ip, hits);

  const code      = String(crypto.randomInt(100_000, 999_999));
  const expiresAt = now + OTP_TTL_MS;
  _otpStore.set('global', { code, expiresAt, used: false });

  const text = `🔐 *MTN GroupShare Dashboard*\n\nYour sign-in code:\n\`${code}\`\n\n_Valid for 5 minutes. Do not share this code._`;

  // Send to all configured Telegram recipients
  const tgRecipients = [
    { bot: tgAuthBot,  chatId: tgAuthChatId },
    { bot: tgAuthBot2, chatId: tgAuthChatId2 },
  ].filter(r => r.bot && r.chatId);

  if (tgRecipients.length === 0) {
    console.error('❌ OTP Telegram send failed: no bots configured');
    _otpStore.delete('global');
    return res.status(500).json({ success: false, error: 'Failed to deliver OTP. Check bot configuration.' });
  }

  Promise.all(tgRecipients.map(r => r.bot.sendMessage(r.chatId, text, { parse_mode: 'Markdown' })))
    .then(() => {
      console.log(`🔑 OTP sent via Telegram (ip: ${ip})`);
      res.json({ success: true });
    })
    .catch(err => {
      console.error(`❌ OTP Telegram send failed: ${err.message}`);
      _otpStore.delete('global');
      res.status(500).json({ success: false, error: 'Failed to deliver OTP. Check bot configuration.' });
    });
});

// POST /auth/verify — validate OTP, issue httpOnly session cookie
app.post('/auth/verify', (req, res) => {
  if (!tgAuthBot || !tgAuthChatId) {
    const token = crypto.randomBytes(32).toString('hex');
    _sessionStore.set(token, Date.now() + SESSION_TTL_MS);
    res.cookie('gs_session', token, { httpOnly: true, sameSite: 'Lax', maxAge: SESSION_TTL_MS });
    return res.json({ success: true });
  }

  const submitted = String(req.body.otp || '').replace(/\D/g, '').slice(0, 6);
  if (!submitted) return res.status(400).json({ success: false, error: 'OTP is required' });

  const record = _otpStore.get('global');
  if (!record || record.used || Date.now() > record.expiresAt) {
    return res.status(401).json({ success: false, error: 'Code expired or not found. Request a new one.' });
  }

  // Constant-time comparison — prevents timing side-channel attacks
  const a = Buffer.from(submitted.padEnd(10, '0'));
  const b = Buffer.from(record.code.padEnd(10, '0'));
  const valid = a.length === b.length && crypto.timingSafeEqual(a, b);

  if (!valid) {
    console.warn(`⚠️  Invalid OTP attempt from ${getClientIp(req)}`);
    return res.status(401).json({ success: false, error: 'Incorrect code. Please try again.' });
  }

  record.used = true; // single-use
  const token = crypto.randomBytes(32).toString('hex');
  _sessionStore.set(token, Date.now() + SESSION_TTL_MS);
  res.cookie('gs_session', token, { httpOnly: true, sameSite: 'Lax', maxAge: SESSION_TTL_MS });
  console.log(`✅ Dashboard login — session issued (ip: ${getClientIp(req)})`);
  res.json({ success: true });
});

// POST /auth/logout — invalidate session cookie
app.post('/auth/logout', (req, res) => {
  const token = parseCookies(req)['gs_session'];
  if (token) _sessionStore.delete(token);
  res.clearCookie('gs_session');
  res.redirect('/login');
});

// GET /auth/status — lets the UI detect session expiry without a full page reload
app.get('/auth/status', (req, res) => {
  res.json({ authenticated: isAuthenticated(req), authRequired: !!(tgAuthBot && tgAuthChatId) });
});


// POST /upload — accept an Excel file from external app
app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ success: false, error: 'No file provided' });
  }

  const { totalDataGB } = getExcelStats(req.file.path);
  const fileMB = totalDataGB != null ? totalDataGB * 1024 : 0;
  const statusLogNow = loadStatusLog();
  const availableMBNow = statusLogNow._lastBalanceMB || 0;
  if (fileMB > 0 && availableMBNow > 0 && fileMB > availableMBNow) {
    const requiredGB = parseFloat((fileMB / 1024).toFixed(2));
    const availableGB = parseFloat((availableMBNow / 1024).toFixed(2));
    console.warn(`⚠️ File queued but allocation (${requiredGB} GB) exceeds current balance (${availableGB} GB) — will process when balance is topped up`);
  }

  withFileLock(STATUS_LOG, () => {
    const log = loadStatusLog();
    saveStatusLog({ ...log, _fileReceived: true });
  });
  console.log(`📥 API received file: ${req.file.filename}`);
  res.json({
    success: true,
    message: 'File queued for processing',
    filename: req.file.filename,
    queuedAt: new Date().toISOString(),
  });
});


// POST /upload-base64 — accept Excel as base64 string
app.post('/upload-base64', (req, res) => {
  const { filename, data, orderId, orderIds } = req.body;

  if (!filename || !data) {
    return res.status(400).json({ success: false, error: 'filename and data are required' });
  }

  if (!filename.endsWith('.xlsx') && !filename.endsWith('.xls')) {
    return res.status(400).json({ success: false, error: 'Only .xlsx and .xls files are allowed' });
  }

  try {
    const buffer = Buffer.from(data, 'base64');

    const newFileMB = getFileTotalMBFromBuffer(buffer);
    const statusLog = loadStatusLog();
    const availableMB = statusLog._lastBalanceMB || 0;
    if (newFileMB > 0 && availableMB > 0 && newFileMB > availableMB) {
      const requiredGB = parseFloat((newFileMB / 1024).toFixed(2));
      const availableGB = parseFloat((availableMB / 1024).toFixed(2));
      console.warn(`⚠️ File queued but allocation (${requiredGB} GB) exceeds current balance (${availableGB} GB) — will process when balance is topped up`);
    }

    const ext = path.extname(filename);
    const base = path.basename(filename, ext);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const savedName = `${base}-${timestamp}${ext}`;
    const savePath = path.join(process.env.EXCEL_FOLDER_PATH, savedName);

    fs.writeFileSync(savePath, buffer);
    console.log(`📥 API received base64 file: ${savedName}`);

    // Persist order reference(s) and wake the idle bot
    const orderMeta = { _fileReceived: true };
    if (Array.isArray(orderIds) && orderIds.length > 0) {
      orderMeta[`${savedName}_orderIds`] = orderIds;
    } else if (orderId) {
      orderMeta[`${savedName}_orderId`] = orderId;
    }
    if (Object.keys(orderMeta).length > 0) {
      withFileLock(STATUS_LOG, () => {
        const log = loadStatusLog();
        saveStatusLog({ ...log, ...orderMeta });
      });
    }

    res.json({
      success: true,
      message: 'File queued for processing',
      filename: savedName,
      queuedAt: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});


// Parse an Excel file and return total data allocation (GB) and row count
function getExcelStats(filePath) {
  try {
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rawRows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
    const DATA_MB_COL = 3; // column 4 (0-indexed)
    let totalMB = 0;
    let rowCount = 0;
    for (let r = 1; r < rawRows.length; r++) {
      const val = parseFloat(rawRows[r][DATA_MB_COL]) || 0;
      if (val > 0) { totalMB += val; rowCount++; }
    }
    return { totalDataGB: parseFloat((totalMB / 1024).toFixed(4)), rowCount };
  } catch {
    return { totalDataGB: null, rowCount: null };
  }
}

// Resolves the status of a source filename by looking it up in the status log.
// For files that were merged into an NM-merged-* batch, status is derived from
// the batch record's sourceFiles array. Returns a status-log-style object.
function resolveFileStatus(filename, uploaded, statusLog) {
  // Scan ALL merged batch records that contain this source file.
  // A file can appear in multiple batch records when a previous batch timed out
  // and the source files were re-merged into a new batch. Always use the most
  // recently created batch record so stale TIMEOUT/FAILED records from earlier
  // attempts don't shadow the current IN_PROGRESS/DONE status.
  let mergedBatch = null;
  let mergedSrcEntry = null;
  let mergedBatchVal = null;
  for (const [key, val] of Object.entries(statusLog)) {
    if (!key.startsWith('NM-merged-') || typeof val !== 'object' || !val.sourceFiles) continue;
    const srcEntry = val.sourceFiles.find(s => s.filename === filename);
    if (!srcEntry) continue;
    // Prefer the most recently created batch record
    if (!mergedBatch || (val.createdAt && (!mergedBatchVal.createdAt || val.createdAt > mergedBatchVal.createdAt))) {
      mergedBatch = key;
      mergedSrcEntry = srcEntry;
      mergedBatchVal = val;
    }
  }

  // 1. File already fully processed — in uploaded log
  if (uploaded.includes(filename)) {
    return {
      status: 'DONE',
      completedAt: statusLog[filename + '_completedAt'] || null,
      queuedAt: statusLog[filename + '_queuedAt'] || null,
      orderId: (mergedSrcEntry?.orderId) || statusLog[filename + '_orderId'] || null,
      orderIds: (mergedSrcEntry?.orderIds) || statusLog[filename + '_orderIds'] || null,
      mergedBatch,
    };
  }

  // 2. Flat string status key (legacy single-file uploads still in progress)
  if (statusLog[filename] && typeof statusLog[filename] === 'string') {
    return {
      status: statusLog[filename],
      completedAt: statusLog[filename + '_completedAt'] || null,
      queuedAt: statusLog[filename + '_queuedAt'] || null,
      orderId: statusLog[filename + '_orderId'] || null,
      orderIds: statusLog[filename + '_orderIds'] || null,
      mergedBatch,
    };
  }

  // 3. File is inside an active/pending merged batch
  if (mergedBatch) {
    return {
      status: mergedBatchVal.status || 'PROCESSING',
      completedAt: mergedBatchVal.completedAt || null,
      queuedAt: mergedBatchVal.createdAt || null,
      orderId: mergedSrcEntry.orderId || null,
      orderIds: mergedSrcEntry.orderIds || null,
      mergedBatch,
    };
  }

  // 4. Default — file received but not yet processed
  return { status: 'PENDING', completedAt: null, queuedAt: null, orderId: null, orderIds: null, mergedBatch: null };
}

// GET /status — get status of all files
app.get('/status', (req, res) => {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  const uploaded = loadUploadedLog();
  const statusLog = loadStatusLog();

  const allFiles = fs.readdirSync(folderPath)
    .filter(f => (f.endsWith('.xlsx') || f.endsWith('.xls')) && !f.startsWith('NM-merged-'))
    .map(f => {
      const stats = getExcelStats(path.join(folderPath, f));
      const resolved = resolveFileStatus(f, uploaded, statusLog);
      const entry = {
        filename: f,
        status: resolved.status,
        queuedAt: resolved.queuedAt,
        completedAt: resolved.completedAt,
        totalDataGB: stats.totalDataGB,
        rowCount: stats.rowCount,
      };
      if (resolved.orderIds) entry.orderIds = resolved.orderIds;
      else if (resolved.orderId) entry.orderId = resolved.orderId;
      if (resolved.mergedBatch) entry.mergedBatch = resolved.mergedBatch;
      return entry;
    });

  // Sort: most recently completed/failed first, pending/active at the bottom
  const DONE_STATES = new Set(['DONE', 'FAILED', 'ABANDONED']);
  allFiles.sort((a, b) => {
    const aDone = DONE_STATES.has(a.status);
    const bDone = DONE_STATES.has(b.status);
    if (aDone && bDone) {
      // Both finished — most recently completed first, then filename A→Z
      const aTime = a.completedAt || 0;
      const bTime = b.completedAt || 0;
      if (bTime > aTime) return  1;
      if (bTime < aTime) return -1;
      return a.filename.localeCompare(b.filename);
    }
    if (aDone) return -1; // finished before not-finished
    if (bDone) return  1;
    return a.filename.localeCompare(b.filename); // pending/active: filename A→Z
  });

  res.json({ success: true, files: allFiles });
});


// GET /status/:filename — get status of a specific file
app.get('/status/:filename', (req, res) => {
  const { filename } = req.params;
  const uploaded = loadUploadedLog();
  const statusLog = loadStatusLog();

  const filePath = path.join(process.env.EXCEL_FOLDER_PATH, filename);
  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ success: false, error: 'File not found' });
  }

  const stats = getExcelStats(filePath);
  const resolved = resolveFileStatus(filename, uploaded, statusLog);
  const entry = {
    success: true,
    filename,
    status: resolved.status,
    queuedAt: resolved.queuedAt,
    completedAt: resolved.completedAt,
    totalDataGB: stats.totalDataGB,
    rowCount: stats.rowCount,
  };
  if (resolved.orderIds) entry.orderIds = resolved.orderIds;
  else if (resolved.orderId) entry.orderId = resolved.orderId;
  if (resolved.mergedBatch) entry.mergedBatch = resolved.mergedBatch;
  res.json(entry);
});


// GET /balance — return current balance from status log (refreshed each bot iteration).
// Add ?refresh=true to force an immediate balance refresh (waits up to 25s for bot to respond).
app.get('/balance', async (req, res) => {
  const wantRefresh = req.query.refresh === 'true';

  if (!wantRefresh) {
    // Fast path — return current balance immediately from status log
    const log = loadStatusLog();
    let cacheAge = null;
    if (log._lastBalanceCheckedAt) {
      const ageMs = Date.now() - new Date(log._lastBalanceCheckedAt).getTime();
      const ageMins = Math.round(ageMs / 60000);
      cacheAge = ageMins < 1 ? 'less than a minute ago' : `${ageMins} minute${ageMins === 1 ? '' : 's'} ago`;
    }
    return res.json({
      success: true,
      balance: log._lastBalance || 'Unknown',
      balanceMB: log._lastBalanceMB || 0,
      checkedAt: log._lastBalanceCheckedAt || null,
      cacheAge,
      fresh: true,
    });
  }

  // Slow path (?refresh=true) — signal the bot to do a real portal read
  const requestedAt = Date.now();
  withFileLock(STATUS_LOG, () => {
    const statusLog = loadStatusLog();
    saveStatusLog({ ...statusLog, _balanceRefreshRequested: true });
  });

  // Wait up to 25s for the bot to clear the flag and write a fresh reading
  const deadline = Date.now() + 25000;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 1000));
    const updated = loadStatusLog();
    const checkedAt = updated._lastBalanceCheckedAt;
    if (!updated._balanceRefreshRequested && checkedAt && new Date(checkedAt).getTime() >= requestedAt) {
      return res.json({
        success: true,
        balance: updated._lastBalance || 'Unknown',
        balanceMB: updated._lastBalanceMB || 0,
        checkedAt: updated._lastBalanceCheckedAt,
        fresh: true,
      });
    }
  }

  // Bot did not respond in time — clear the stale flag and return current estimate
  withFileLock(STATUS_LOG, () => {
    const s = loadStatusLog();
    if (s._balanceRefreshRequested) {
      saveStatusLog({ ...s, _balanceRefreshRequested: false });
    }
  });

  const final = loadStatusLog();

  // Work out what the bot is currently doing
  const uploaded = loadUploadedLog();
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  let busyFile = null;
  let busyStatus = null;
  try {
    const files = fs.readdirSync(folderPath).filter(f => f.endsWith('.xlsx') || f.endsWith('.xls'));
    for (const f of files) {
      const st = uploaded.includes(f) ? 'DONE' : (final[f] || 'PENDING');
      if (st === 'IN_PROGRESS' || st === 'PROCESSING') {
        busyFile = f;
        busyStatus = st;
        break;
      }
    }
  } catch {}

  let cacheAge = null;
  if (final._lastBalanceCheckedAt) {
    const ageMs = Date.now() - new Date(final._lastBalanceCheckedAt).getTime();
    const ageMins = Math.round(ageMs / 60000);
    cacheAge = ageMins < 1 ? 'less than a minute ago' : `${ageMins} minute${ageMins === 1 ? '' : 's'} ago`;
  }

  const note = busyFile
    ? `Bot is busy processing "${busyFile}" (${busyStatus}) and cannot navigate away to refresh balance. Last known value is from ${cacheAge || 'an earlier check'}.`
    : `Bot did not respond within 25 s. Last known value is from ${cacheAge || 'an earlier check'}. Try again shortly.`;

  return res.json({
    success: true,
    balance: final._lastBalance || 'Unknown',
    balanceMB: final._lastBalanceMB || 0,
    checkedAt: final._lastBalanceCheckedAt || null,
    cacheAge,
    fresh: false,
    note,
  });
});


// POST /purchase — trigger a data bundle purchase on the bot
app.post('/purchase', (req, res) => {
  withFileLock(STATUS_LOG, () => {
    const statusLog = loadStatusLog();
    saveStatusLog({ ...statusLog, _purchaseRequested: true, _purchaseStatus: 'PENDING', _purchaseRequestedAt: new Date().toISOString() });
  });
  console.log('📲 Purchase requested via API');
  return res.status(202).json({ success: true, note: 'Purchase request queued. Poll GET /purchase-status for result.' });
});

// GET /purchase-status — check the current state of a purchase request
app.get('/purchase-status', (req, res) => {
  const log = loadStatusLog();
  return res.json({
    status: log._purchaseStatus || 'IDLE',
    note: log._purchaseNote || null,
    requestedAt: log._purchaseRequestedAt || null,
    completedAt: log._purchaseCompletedAt || null,
  });
});


// GET /disk — get current disk usage of the upload folder
app.get('/disk', (req, res) => {
  const usage = getDiskUsage();
  const diskLimitMB = parseFloat(process.env.DISK_LIMIT_MB || '900'); // safe limit under 1GB
  const usedPercent = parseFloat(((usage.totalMB / diskLimitMB) * 100).toFixed(1));

  res.json({
    success: true,
    used: {
      bytes: usage.totalBytes,
      kb: usage.totalKB,
      mb: usage.totalMB,
    },
    limit: {
      mb: diskLimitMB,
      gb: parseFloat((diskLimitMB / 1024).toFixed(2)),
    },
    usedPercent,
    fileCount: usage.fileCount,
    retentionHours: RETENTION_HOURS,
    checkedAt: new Date().toISOString(),
  });
});


// POST /cleanup — manually trigger file cleanup
app.post('/cleanup', (req, res) => {
  console.log('🧹 Manual cleanup triggered via API');
  const result = cleanupOldFiles();
  res.json({
    success: true,
    message: 'Cleanup complete',
    retentionHours: RETENTION_HOURS,
    ...result,
  });
});


// GET /health — simple health check
app.get('/health', (req, res) => {
  const usage = getDiskUsage();
  res.json({
    success: true,
    status: 'running',
    time: new Date().toISOString(),
    disk: {
      usedMB: usage.totalMB,
      fileCount: usage.fileCount,
    },
  });
});


// GET /summary — single lightweight call for the dashboard UI.
// Uses cached _totalMB values from the status log — never re-parses XLSX.
app.get('/summary', requireAuth, (req, res) => {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  if (!folderPath || !fs.existsSync(folderPath)) {
    return res.status(500).json({ success: false, error: 'EXCEL_FOLDER_PATH not configured' });
  }

  const uploaded  = loadUploadedLog();
  const statusLog = loadStatusLog();
  const disk      = getDiskUsage();

  let files;
  try {
    files = fs.readdirSync(folderPath)
      .filter(f => (f.endsWith('.xlsx') || f.endsWith('.xls')) && !f.startsWith('NM-merged-'))
      .map(f => {
        const resolved   = resolveFileStatus(f, uploaded, statusLog);
        const cachedMB   = statusLog[`${f}_totalMB`];
        const totalDataGB = cachedMB != null
          ? parseFloat((cachedMB / 1024).toFixed(4))
          : null;

        const entry = {
          filename:    f,
          status:      resolved.status,
          totalDataGB,
          queuedAt:    resolved.queuedAt   || null,
          startedAt:   statusLog[`${f}_startedAt`]  || null,
          completedAt: resolved.completedAt || null,
          failedAt:    statusLog[`${f}_failedAt`]   || null,
          timedOutAt:  statusLog[`${f}_timedOutAt`] || null,
          retryCount:  statusLog[`${f}_retryCount`] || 0,
          mergedBatch: resolved.mergedBatch || null,
        };

        if (resolved.orderIds)     entry.orderIds = resolved.orderIds;
        else if (resolved.orderId) entry.orderId  = resolved.orderId;

        return entry;
      });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }

  const DONE_SET   = new Set(['DONE']);
  const FAILED_SET = new Set(['FAILED', 'ABANDONED']);
  const ACTIVE_SET = new Set(['IN_PROGRESS', 'PROCESSING', 'TIMEOUT']);
  const diskLimitMB = parseFloat(process.env.DISK_LIMIT_MB || '900');

  // Assign FFD queue positions — mirrors the largest-first sort in index.js
  // PENDING files get positions 1, 2, 3… sorted by totalDataGB descending.
  // IN_PROGRESS/PROCESSING get position 0 (currently active).
  const QUEUEABLE = new Set(['PENDING', 'TIMEOUT']);
  const pendingOrdered = files
    .filter(f => QUEUEABLE.has(f.status))
    .sort((a, b) => (b.totalDataGB || 0) - (a.totalDataGB || 0));
  pendingOrdered.forEach((f, i) => { f.queuePosition = i + 1; });
  files.filter(f => f.status === 'IN_PROGRESS' || f.status === 'PROCESSING')
       .forEach(f => { f.queuePosition = 0; });

  // Sort: most recently completed/failed first, then active (queuePosition 0), then pending by queue order
  files.sort((a, b) => {
    const FINISHED = new Set(['DONE', 'FAILED', 'ABANDONED']);
    const aFinished = FINISHED.has(a.status);
    const bFinished = FINISHED.has(b.status);

    // Finished files: sort by completedAt descending (most recent first), then filename A→Z
    if (aFinished && bFinished) {
      const aTime = a.completedAt || a.failedAt || 0;
      const bTime = b.completedAt || b.failedAt || 0;
      if (bTime > aTime) return  1;
      if (bTime < aTime) return -1;
      return a.filename.localeCompare(b.filename);
    }
    if (aFinished) return -1;
    if (bFinished) return  1;

    // Non-finished: sort by queuePosition ascending (0 = active, then 1, 2, 3…)
    const aPos = a.queuePosition ?? Infinity;
    const bPos = b.queuePosition ?? Infinity;
    if (aPos !== bPos) return aPos - bPos;
    return a.filename.localeCompare(b.filename);
  });

  const queue = {
    total:       files.length,
    pending:     files.filter(f => !f.status || f.status === 'PENDING').length,
    active:      files.filter(f => ACTIVE_SET.has(f.status)).length,
    done:        files.filter(f => DONE_SET.has(f.status)).length,
    failed:      files.filter(f => FAILED_SET.has(f.status)).length,
    nextInLine:  pendingOrdered.length > 0 ? pendingOrdered[0].filename : null,
    pendingMB: Math.round(
      files
        .filter(f => !DONE_SET.has(f.status) && !FAILED_SET.has(f.status))
        .reduce((s, f) => s + (f.totalDataGB ? f.totalDataGB * 1024 : 0), 0)
    ),
  };

  res.json({
    success: true,
    balance: {
      text:         statusLog._lastBalance             || null,
      mb:           statusLog._lastBalanceMB           || 0,
      checkedAt:    statusLog._lastBalanceCheckedAt    || null,
      accountGhc:   statusLog._lastAccountBalance      ?? null,
      accountText:  statusLog._lastAccountBalanceText  || null,
    },
    purchase: {
      status:      statusLog._purchaseStatus      || 'IDLE',
      note:        statusLog._purchaseNote        || null,
      requestedAt: statusLog._purchaseRequestedAt || null,
      completedAt: statusLog._purchaseCompletedAt || null,
    },
    disk: {
      usedMB:      disk.totalMB,
      fileCount:   disk.fileCount,
      limitMB:     diskLimitMB,
      usedPercent: parseFloat(((disk.totalMB / diskLimitMB) * 100).toFixed(1)),
    },
    queue,
    files,
    airtime: {
      enabled:            statusLog._airtimeEnabled           || false,
      windowStart:        statusLog._airtimeWindowStart       ?? 0,
      windowEnd:          statusLog._airtimeWindowEnd         ?? 24,
      stage:              statusLog._airtimeStage             || 'idle',
      triggeredAt:        statusLog._airtimeTriggeredAt       || null,
      lastCallbackAt:     statusLog._airtimeLastCallbackAt    || null,
      lastError:          statusLog._airtimeLastError         || null,
      load312Verified:    statusLog._airtimeLoad312Verified   ?? null,
      load500Count:       statusLog._airtimeLoad500Count      ?? 0,
      load500TotalAdded:  statusLog._airtimeLoad500TotalAdded ?? 0,
      load500MaxRounds:   AIRTIME_LOAD500_MAX_ROUNDS,
      load500TargetGhc:   AIRTIME_LOAD500_TARGET_GHC,
    },
  });
});


// ── AIRTIME LOADING ────────────────────────────────────────

const AIRTIME_LOAD500_MAX_ROUNDS = 9;
const AIRTIME_LOAD500_TARGET_GHC = 4500;
const AIRTIME_BALANCE_TOLERANCE  = 50; // GHC — acceptable shortfall when verifying balance increase
const AIRTIME_MAX_RETRIES        = 2;  // max retry attempts per stage before proceeding anyway

// Fetch the portal account balance directly from the MTN up2u API using the
// session cookies persisted by the bot. Falls back to the signalling mechanism
// (requesting a fresh read from the Playwright bot) if no cookies are available.
async function waitForFreshAccountBalance(timeoutMs = 20000) {
  // ── Fast path: call the API directly with stored session cookies ──────────
  const sl = loadStatusLog();
  const cookieHeader = sl._portalCookieHeader;
  if (cookieHeader) {
    try {
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
      if (data.success && data.body && typeof data.body.MainAccountBalanceCedis === 'number') {
        const accountBalance     = data.body.MainAccountBalanceCedis;
        const accountBalanceText = `GH¢ ${accountBalance.toFixed(2)}`;
        console.log(`✅ Portal balance fetched directly: ${accountBalanceText}`);
        // Write the fresh value back so the status log stays current
        updateStatusLog({
          _lastAccountBalance:     accountBalance,
          _lastAccountBalanceText: accountBalanceText,
          _lastBalanceCheckedAt:   new Date().toISOString(),
        });
        return loadStatusLog();
      }
      console.warn('⚠️  check-balance API returned unexpected response — falling back to bot signal');
    } catch (err) {
      console.warn(`⚠️  check-balance API call failed: ${err.message} — falling back to bot signal`);
    }
  }

  // ── Fallback: signal bot and wait for it to do a Playwright balance read ──
  console.log('🔄 No stored cookies — requesting balance refresh from bot…');
  const requestedAt = Date.now();
  withFileLock(STATUS_LOG, () => {
    const s = loadStatusLog();
    saveStatusLog({ ...s, _balanceRefreshRequested: true });
  });

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 1500));
    const s = loadStatusLog();
    const checkedAt = s._lastBalanceCheckedAt;
    if (!s._balanceRefreshRequested && checkedAt && new Date(checkedAt).getTime() >= requestedAt) {
      console.log('✅ Portal balance refreshed via bot');
      return s;
    }
  }
  // Timed out
  withFileLock(STATUS_LOG, () => {
    const s = loadStatusLog();
    if (s._balanceRefreshRequested) saveStatusLog({ ...s, _balanceRefreshRequested: false });
  });
  console.warn('⚠️  Portal balance refresh timed out — using last known value');
  return loadStatusLog();
}

// Retry load_312 via ntfy (used when USSD failure is confirmed by portal).
async function dispatchLoad312Ntfy() {
  try {
    const ntfyUrl = (process.env.NTFY_URL || 'https://ntfy.sh').replace(/\/$/, '') + '/clickyfiedloader_5';
    const r = await fetch(ntfyUrl, { method: 'PUT', body: 'load_312' });
    if (r.ok) {
      console.log('📤 Airtime load_312 retried via ntfy');
      updateStatusLog({ _airtimeStage: 'load_312_sent' });
    } else {
      const msg = `ntfy HTTP ${r.status}`;
      console.warn(`⚠️  load_312 retry ntfy returned ${r.status}`);
      updateStatusLog({ _airtimeStage: 'error', _airtimeLastError: msg });
    }
  } catch (err) {
    console.error(`❌ load_312 retry failed: ${err.message}`);
    updateStatusLog({ _airtimeStage: 'error', _airtimeLastError: err.message });
  }
}

// Send load_500 to ntfy and record the sent round number in the status log.
// Snapshots the current portal account balance before dispatching so the credit
// verifier can compare before/after to confirm the phone's transfer arrived.
async function dispatchLoad500Ntfy(roundNumber) {
  const slNow = loadStatusLog();
  try {
    const ntfyUrl = (process.env.NTFY_URL || 'https://ntfy.sh').replace(/\/$/, '') + '/clickyfiedloader_5';
    const r = await fetch(ntfyUrl, { method: 'PUT', body: 'load_500' });
    if (r.ok) {
      console.log(`📤 Airtime load_500 round ${roundNumber}/${AIRTIME_LOAD500_MAX_ROUNDS} dispatched`);
      updateStatusLog({
        _airtimeStage: 'load_500_sent',
        _airtimeLoad500SentCount: roundNumber,
        // NOTE: _airtimeLoad500RetryCount is intentionally NOT reset here —
        // the caller owns the retry counter (fresh starts reset it explicitly).
        _airtimeAcctBalBeforeLoad500: slNow._lastAccountBalance ?? null,
      });
    } else {
      const msg = `ntfy HTTP ${r.status}`;
      console.warn(`⚠️  Airtime load_500 round ${roundNumber} ntfy returned ${r.status}`);
      updateStatusLog({ _airtimeStage: 'error', _airtimeLastError: msg });
    }
  } catch (err) {
    console.error(`❌ Airtime load_500 round ${roundNumber} dispatch failed: ${err.message}`);
    updateStatusLog({ _airtimeStage: 'error', _airtimeLastError: err.message });
  }
}

// GET /airtime/status — current airtime feature state (open endpoint)
app.get('/airtime/status', (req, res) => {
  const sl = loadStatusLog();
  res.json({
    success: true,
    airtime: {
      enabled:            sl._airtimeEnabled           || false,
      windowStart:        sl._airtimeWindowStart       ?? 0,
      windowEnd:          sl._airtimeWindowEnd         ?? 1440,
      stage:              sl._airtimeStage             || 'idle',
      triggeredAt:        sl._airtimeTriggeredAt       || null,
      lastCallbackAt:     sl._airtimeLastCallbackAt    || null,
      lastError:          sl._airtimeLastError         || null,
      load312Verified:    sl._airtimeLoad312Verified   ?? null,
      load500Count:       sl._airtimeLoad500Count      ?? 0,
      load500TotalAdded:  sl._airtimeLoad500TotalAdded ?? 0,
      load500MaxRounds:   AIRTIME_LOAD500_MAX_ROUNDS,
      load500TargetGhc:   AIRTIME_LOAD500_TARGET_GHC,
    },
  });
});

// POST /airtime/settings — update airtime config (dashboard session required)
app.post('/airtime/settings', (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ success: false, error: 'Unauthorized' });

  const { enabled, windowStart, windowEnd } = req.body || {};
  const updates = {};
  if (typeof enabled === 'boolean') updates._airtimeEnabled = enabled;
  if (typeof windowStart === 'number' && windowStart >= 0 && windowStart <= 1439) updates._airtimeWindowStart = Math.floor(windowStart);
  if (typeof windowEnd   === 'number' && windowEnd   >= 1 && windowEnd   <= 1440) updates._airtimeWindowEnd   = Math.floor(windowEnd);

  updateStatusLog(updates);
  const sl = loadStatusLog();
  return res.json({
    success: true,
    airtime: {
      enabled:     sl._airtimeEnabled     || false,
      windowStart: sl._airtimeWindowStart ?? 0,
      windowEnd:   sl._airtimeWindowEnd   ?? 1440,
    },
  });
});

// POST /airtime/callback — USSD process signals a stage has completed.
// Secured with x-airtime-secret header (or ?secret= query param) matching GROUPSHARE_CALLBACK_SECRET.
// Body: { "stage": "load_312"|"load_500" }
// The callback fires regardless of whether the USSD succeeded or failed, so the server
// ALWAYS verifies success by checking the portal GHC balance for an increase after the callback.
// If no increase is detected the load command is retried (up to AIRTIME_MAX_RETRIES times).
app.post('/airtime/callback', async (req, res) => {
  const expected = process.env.GROUPSHARE_CALLBACK_SECRET;
  if (expected) {
    const given = String(req.headers['x-airtime-secret'] || req.query.secret || '');
    const expBuf   = Buffer.from(expected, 'utf8');
    const givenBuf = Buffer.alloc(expBuf.length, 0);
    Buffer.from(given, 'utf8').copy(givenBuf);
    if (!crypto.timingSafeEqual(expBuf, givenBuf)) {
      return res.status(401).json({ success: false, error: 'Unauthorized' });
    }
  }

  const { stage } = req.body || {};
  if (!stage) return res.status(400).json({ success: false, error: 'Missing stage' });

  // ── Shared portal-credit verifier ───────────────────────────────────────────
  // The phone sends airtime to the portal via USSD, so the portal GHC cash balance
  // INCREASES after a successful transfer. Fetches a fresh portal balance and compares
  // against the snapshot taken before the USSD was sent. Returns { verified, credited, note }.
  async function verifyPortalCredit(acctBefore, expectedGhc, label) {
    if (acctBefore == null) {
      return {
        verified: true,
        credited: null,
        note: `No portal baseline before ${label} — assuming success`,
      };
    }
    console.log(`🔍 ${label}: checking portal account balance increase (expected ~GH¢ ${expectedGhc})…`);
    const freshSl   = await waitForFreshAccountBalance(20000);
    const acctAfter = freshSl._lastAccountBalance;
    if (acctAfter == null) {
      return {
        verified: true,
        credited: null,
        note: `Portal balance unavailable after ${label} — assuming success`,
      };
    }
    const credited = acctAfter - acctBefore;
    if (credited >= expectedGhc - AIRTIME_BALANCE_TOLERANCE) {
      return {
        verified: true,
        credited,
        acctAfter,
        note: `Portal confirmed GH¢ ${credited.toFixed(2)} received — USSD succeeded`,
      };
    }
    return {
      verified: false,
      credited,
      acctAfter,
      note: `Portal: GH¢ ${credited.toFixed(2)} received (expected ~${expectedGhc}) — USSD failed`,
    };
  }

  // ── load_312 callback ──────────────────────────────────────────────────────
  if (stage === 'load_312') {
    const sl = loadStatusLog();
    if (sl._airtimeStage !== 'load_312_sent') {
      console.log(`⚡ load_312 callback ignored (current stage: ${sl._airtimeStage})`);
      return res.json({ success: true, action: 'ignored', currentStage: sl._airtimeStage });
    }

    console.log('📲 Airtime load_312 callback received — verifying via portal…');
    updateStatusLog({ _airtimeStage: 'load_312_processing' }); // block re-entrant callbacks

    const { verified, credited, acctAfter, note: verificationNote } =
      await verifyPortalCredit(sl._airtimeAcctBalBeforeLoad312, 312, 'load_312');

    if (!verified) {
      const retryCount = (sl._airtimeLoad312RetryCount ?? 0) + 1;
      console.warn(`🔄 load_312 USSD failed — retry ${retryCount}/${AIRTIME_MAX_RETRIES}: ${verificationNote}`);

      if (retryCount <= AIRTIME_MAX_RETRIES) {
        updateStatusLog({
          _airtimeStage: 'load_312_sent',
          _airtimeLastCallbackAt: new Date().toISOString(),
          _airtimeAcctBalBeforeLoad312: acctAfter ?? sl._airtimeAcctBalBeforeLoad312,
          _airtimeLoad312RetryCount: retryCount,
          _airtimeLastError: verificationNote,
        });
        res.json({ success: true, action: 'load_312_retry', verified: false, note: verificationNote });
        await new Promise(r => setTimeout(r, 5000));
        await dispatchLoad312Ntfy();
        return;
      }
      console.warn(`⚠️  load_312 max retries (${AIRTIME_MAX_RETRIES}) reached — proceeding to load_500 anyway`);
    } else {
      console.log(`✅ load_312 verified: ${verificationNote}`);
    }

    updateStatusLog({
      _airtimeStage: 'load_312_verified',
      _airtimeLastCallbackAt: new Date().toISOString(),
      _airtimeLoad312Verified: verified,
      _airtimeLoad312RetryCount: 0,
      _airtimeLoad500Count: 0,
      _airtimeLoad500TotalAdded: 0,
      _airtimeLoad500SentCount: 0,
    });

    res.json({ success: true, action: 'load_312_received', verified, note: verificationNote });
    await new Promise(r => setTimeout(r, 5000));
    await dispatchLoad500Ntfy(1);
    return;
  }

  // ── load_500 callback ──────────────────────────────────────────────────────
  if (stage === 'load_500') {
    const sl = loadStatusLog();

    if (sl._airtimeStage === 'done') {
      console.log('⚡ load_500 callback ignored (already done)');
      return res.json({ success: true, action: 'already_done' });
    }
    if (sl._airtimeStage !== 'load_500_sent') {
      console.log(`⚡ load_500 callback ignored (current stage: ${sl._airtimeStage})`);
      return res.json({ success: true, action: 'ignored', currentStage: sl._airtimeStage });
    }

    const prevCount  = sl._airtimeLoad500Count ?? 0;
    const newCount   = prevCount + 1;
    let   totalAdded = sl._airtimeLoad500TotalAdded ?? 0;

    console.log(`📲 Airtime load_500 round ${newCount} callback received — verifying via portal…`);
    updateStatusLog({ _airtimeStage: 'load_500_processing' }); // block re-entrant callbacks

    const { verified, credited, acctAfter, note: verificationNote } =
      await verifyPortalCredit(sl._airtimeAcctBalBeforeLoad500, 500, `load_500 round ${newCount}`);

    if (!verified) {
      const retryCount = (sl._airtimeLoad500RetryCount ?? 0) + 1;
      console.warn(`🔄 load_500 round ${newCount} USSD failed — retry ${retryCount}/${AIRTIME_MAX_RETRIES}: ${verificationNote}`);

      if (retryCount <= AIRTIME_MAX_RETRIES) {
        updateStatusLog({
          _airtimeStage: 'load_500_sent',
          _airtimeLastCallbackAt: new Date().toISOString(),
          _airtimeAcctBalBeforeLoad500: acctAfter ?? sl._airtimeAcctBalBeforeLoad500,
          _airtimeLoad500RetryCount: retryCount,
          _airtimeLastError: verificationNote,
        });
        res.json({ success: true, action: `load_500_round_${newCount}_retry`, verified: false, note: verificationNote });
        await new Promise(r => setTimeout(r, 5000));
        await dispatchLoad500Ntfy(newCount); // retry same round
        return;
      }
      console.warn(`⚠️  load_500 round ${newCount} max retries (${AIRTIME_MAX_RETRIES}) reached — skipping round`);
      // Skipped round: credited amount (0 or partial) is not counted toward target
    } else {
      console.log(`✅ load_500 round ${newCount} verified: ${verificationNote}`);
      totalAdded += credited ?? 500; // credited is always set when verified=true with a baseline
    }

    const isDone = newCount >= AIRTIME_LOAD500_MAX_ROUNDS || totalAdded >= AIRTIME_LOAD500_TARGET_GHC;

    console.log(`📊 load_500 round ${newCount}/${AIRTIME_LOAD500_MAX_ROUNDS} — total added: GH¢ ${totalAdded.toFixed(2)}/${AIRTIME_LOAD500_TARGET_GHC} — done: ${isDone}`);

    updateStatusLog({
      _airtimeStage: isDone ? 'done' : 'load_500_processing',
      _airtimeLastCallbackAt: new Date().toISOString(),
      _airtimeLoad500Count: newCount,
      _airtimeLoad500TotalAdded: totalAdded,
      _airtimeLoad500RetryCount: 0,
    });

    if (isDone) {
      console.log(`🎉 Airtime load complete! ${newCount} round(s), GH¢ ${totalAdded.toFixed(2)} total added`);
      return res.json({ success: true, action: 'done', rounds: newCount, totalAdded });
    }

    res.json({
      success: true,
      action: `load_500_round_${newCount}_complete`,
      verified,
      note: verificationNote,
      roundsCompleted: newCount,
      roundsRemaining: AIRTIME_LOAD500_MAX_ROUNDS - newCount,
      totalAdded,
    });

    await new Promise(r => setTimeout(r, 5000));
    await dispatchLoad500Ntfy(newCount + 1);
    return;
  }

  return res.status(400).json({ success: false, error: `Unknown stage: ${stage}` });
});


// ── SCHEDULED CLEANUP ─────────────────────────────────────
// Run on startup, then every hour
console.log(`🧹 File retention set to ${RETENTION_HOURS} hours`);
cleanupOldFiles();
setInterval(cleanupOldFiles, 60 * 60 * 1000);


// ── PORT BINDING ───────────────────────────────────────────
const API_PORT = process.env.PORT || process.env.API_PORT || 7070;
const PUBLIC_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${API_PORT}`;


app.listen(API_PORT, () => {
  console.log(`🚀 API server running on port ${API_PORT}`);
  console.log(`📡 Endpoints:`);
  console.log(`   POST ${PUBLIC_URL}/upload         — upload .xlsx file (multipart)`);
  console.log(`   POST ${PUBLIC_URL}/upload-base64  — upload .xlsx file (base64)`);
  console.log(`   GET  ${PUBLIC_URL}/status         — list all file statuses`);
  console.log(`   GET  ${PUBLIC_URL}/status/:file   — get specific file status`);
  console.log(`   GET  ${PUBLIC_URL}/balance        — get current data balance`);
  console.log(`   GET  ${PUBLIC_URL}/disk           — get disk usage`);
  console.log(`   POST ${PUBLIC_URL}/cleanup        — trigger manual cleanup`);
  console.log(`   GET  ${PUBLIC_URL}/health         — health check`);
});


module.exports = app;