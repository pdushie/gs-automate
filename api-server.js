const express = require('express');
const multer = require('multer');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
require('dotenv').config();
const { withFileLock, atomicWrite } = require('./lock');
const crypto      = require('crypto');
const compression = require('compression');
const TelegramBot = require('node-telegram-bot-api');


const app = express();
app.use(compression()); // gzip all JSON + HTML responses
app.use(cors());
// Capture raw body buffer for routes that need it (e.g. HMAC verification on /evd/callback).
// Must be set on express.json() before the body is parsed — route-level express.raw() does NOT
// work because the global parser consumes the stream first.
app.use(express.json({ limit: '50mb', verify: (req, _res, buf) => { req.rawBody = buf; } }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));


const UPLOADED_LOG  = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.uploaded.json');
const STATUS_LOG    = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.status.json');
const SESSION_LOG   = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.sessions.json');
const RETENTION_HOURS = parseInt(process.env.FILE_RETENTION_HOURS || '24');


// ── AUTH ─────────────────────────────────────────────────
// Dashboard access is protected by a Telegram-delivered OTP.
// If TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set, auth is bypassed (dev mode).
const tgAuthBot    = process.env.TELEGRAM_BOT_TOKEN   ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN,   { polling: false }) : null;
const tgAuthChatId = process.env.TELEGRAM_CHAT_ID     || null;
const tgAuthBot2   = process.env.TELEGRAM_BOT_TOKEN_2 ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN_2, { polling: false }) : null;
const tgAuthChatId2 = process.env.TELEGRAM_CHAT_ID_2  || null;

// In-memory stores
const _otpStore   = new Map(); // 'global' -> { code, expiresAt, used }
const _rateStore  = new Map(); // ip        -> timestamps[]

// Session store — backed by disk so sessions survive process restarts / redeploys.
const _sessionStore = new Map(); // token -> expiresAt

function _persistSessions() {
  try {
    const obj = {};
    const now = Date.now();
    for (const [k, v] of _sessionStore) if (v > now) obj[k] = v; // only save valid sessions
    fs.writeFileSync(SESSION_LOG, JSON.stringify(obj), 'utf8');
  } catch { /* non-fatal */ }
}

// Load persisted sessions on startup, discarding any that have already expired
try {
  if (fs.existsSync(SESSION_LOG)) {
    const raw = JSON.parse(fs.readFileSync(SESSION_LOG, 'utf8'));
    const now = Date.now();
    for (const [k, v] of Object.entries(raw)) {
      if (v > now) _sessionStore.set(k, v);
    }
    console.log(`🔑 Loaded ${_sessionStore.size} active session(s) from disk`);
  }
} catch { /* ignore corrupt file */ }

const OTP_TTL_MS     =  5 * 60 * 1000;      // 5 minutes
const SESSION_TTL_MS = 30 * 60 * 1000;      // 30 minutes
const RATE_MAX       = 3;
const RATE_WIN_MS    = 15 * 60 * 1000;      // 15 minutes

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of _otpStore)     if (v.expiresAt < now) _otpStore.delete(k);
  let sessionsPruned = 0;
  for (const [k, v] of _sessionStore) if (v < now) { _sessionStore.delete(k); sessionsPruned++; }
  if (sessionsPruned > 0) _persistSessions();
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
  const token = parseCookies(req)['gs_session'];
  if (token) {
    const expiry = _sessionStore.get(token);
    if (expiry && Date.now() <= expiry) {
      // Sliding window: extend the session on every authenticated request
      _sessionStore.set(token, Date.now() + SESSION_TTL_MS);
      _persistSessions();
      res.cookie('gs_session', token, { httpOnly: true, sameSite: 'Lax', maxAge: SESSION_TTL_MS });
      return next();
    }
    _sessionStore.delete(token);
  }
  if (!tgAuthBot || !tgAuthChatId) return next(); // auth disabled
  // API/JSON callers get 401; browser navigation gets a redirect
  const wantsJson = (req.headers.accept || '').includes('application/json') || req.xhr;
  if (wantsJson) return res.status(401).json({ success: false, error: 'Session expired — please log in again' });
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

// In-process read caches — reduce disk I/O for hot read paths.
// The bot writes directly to disk via atomicWrite; these caches use a short
// TTL so API reads stay fresh without hitting the filesystem on every request.
const _cache = {
  statusLog:   { data: null, at: 0 },
  uploadedLog: { data: null, at: 0 },
  diskUsage:   { data: null, at: 0 },
};
const STATUS_LOG_TTL  = 500;   // ms — status log can lag by up to 500 ms
const UPLOADED_LOG_TTL = 1000; // ms — uploaded list changes less often
const DISK_USAGE_TTL  = 60000; // ms — disk usage is expensive, cache for 1 min

function _readJson(filePath, fallback) {
  try {
    if (fs.existsSync(filePath)) return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    try {
      if (fs.existsSync(filePath)) return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch {}
  }
  return fallback;
}

function loadUploadedLog() {
  const c = _cache.uploadedLog;
  if (c.data !== null && Date.now() - c.at < UPLOADED_LOG_TTL) return c.data;
  c.data = _readJson(UPLOADED_LOG, []);
  c.at   = Date.now();
  return c.data;
}

function loadStatusLog() {
  const c = _cache.statusLog;
  if (c.data !== null && Date.now() - c.at < STATUS_LOG_TTL) return c.data;
  c.data = _readJson(STATUS_LOG, {});
  c.at   = Date.now();
  return c.data;
}


// Atomic write — always called from within a withFileLock block
function saveStatusLog(statusLog) {
  atomicWrite(STATUS_LOG, JSON.stringify(statusLog, null, 2));
  _cache.statusLog.at = 0; // invalidate read cache
}

function updateStatusLog(updates) {
  withFileLock(STATUS_LOG, () => {
    const log = loadStatusLog();
    saveStatusLog({ ...log, ...updates });
  });
}


function getDiskUsage() {
  const c = _cache.diskUsage;
  if (c.data !== null && Date.now() - c.at < DISK_USAGE_TTL) return c.data;

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

    const result = {
      totalBytes,
      totalKB: Math.round(totalBytes / 1024),
      totalMB: parseFloat((totalBytes / (1024 * 1024)).toFixed(2)),
      fileCount,
    };
    _cache.diskUsage = { data: result, at: Date.now() };
    return result;
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
    _persistSessions();
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
  _persistSessions();
  res.cookie('gs_session', token, { httpOnly: true, sameSite: 'Lax', maxAge: SESSION_TTL_MS });
  console.log(`✅ Dashboard login — session issued (ip: ${getClientIp(req)})`);
  res.json({ success: true });
});

// POST /auth/logout — invalidate session cookie
app.post('/auth/logout', (req, res) => {
  const token = parseCookies(req)['gs_session'];
  if (token) { _sessionStore.delete(token); _persistSessions(); }
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
    saveStatusLog({ ...log, _fileReceived: true, [`${req.file.filename}_queuedAt`]: new Date().toISOString() });
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
    const orderMeta = { _fileReceived: true, [`${savedName}_queuedAt`]: new Date().toISOString() };
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


// Direct portal balance fetch — used by /balance?refresh=true and /summary auto-refresh.
// Calls the MTN portal check-balance API using the saved session cookie from the bot.
// Returns { balanceMB, balanceText, accountGhc, accountText, checkedAt } on success, null on failure.
async function fetchFreshBalanceFromPortal() {
  const log = loadStatusLog();
  if (!log._portalCookieHeader) return null;
  try {
    const res = await fetch('https://up2u.mtn.com.gh/providers/api/check-balance', {
      method: 'POST',
      headers: {
        'Accept':       'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'Cookie':       log._portalCookieHeader,
        'Origin':       'https://up2u.mtn.com.gh',
        'Referer':      'https://up2u.mtn.com.gh/',
        'User-Agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
      body: JSON.stringify({}),
    });
    const data = await res.json().catch(() => ({}));
    if (!data.success || typeof data.body?.DataBalanceMB !== 'number') return null;
    const balanceMB    = data.body.DataBalanceMB;
    const balanceText  = data.body.DataBalanceFormatted || `${(balanceMB / 1024).toFixed(2)} GB`;
    const accountGhc   = typeof data.body.MainAccountBalanceCedis === 'number' ? data.body.MainAccountBalanceCedis : null;
    const accountText  = accountGhc != null ? `GH\u00a2 ${accountGhc.toFixed(2)}` : null;
    const checkedAt    = new Date().toISOString();
    const updates = { _lastBalance: balanceText, _lastBalanceMB: balanceMB, _lastBalanceCheckedAt: checkedAt };
    if (accountGhc != null) { updates._lastAccountBalance = accountGhc; updates._lastAccountBalanceText = accountText; }
    updateStatusLog(updates);
    _cache.statusLog.at = 0; // bust read cache so /summary picks up new values immediately
    return { balanceMB, balanceText, accountGhc, accountText, checkedAt };
  } catch {
    return null;
  }
}

// GET /balance — return current balance from status log (refreshed each bot iteration).
// Add ?refresh=true to force an immediate balance refresh via direct portal API call.
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
      fresh: false,
    });
  }

  // Refresh path — hit the MTN portal API directly (fast, no bot dependency)
  const fresh = await fetchFreshBalanceFromPortal();
  if (fresh) {
    return res.json({
      success: true,
      balance: fresh.balanceText,
      balanceMB: fresh.balanceMB,
      checkedAt: fresh.checkedAt,
      fresh: true,
    });
  }

  // Portal API unavailable — fall back to cached value
  const final = loadStatusLog();
  let cacheAge2 = null;
  if (final._lastBalanceCheckedAt) {
    const ageMs2 = Date.now() - new Date(final._lastBalanceCheckedAt).getTime();
    const ageMins2 = Math.round(ageMs2 / 60000);
    cacheAge2 = ageMins2 < 1 ? 'less than a minute ago' : `${ageMins2} minute${ageMins2 === 1 ? '' : 's'} ago`;
  }
  const noteStr = 'Portal API unavailable (session may have expired). Showing last known value' + (cacheAge2 ? ` from ${cacheAge2}` : '') + '.';

  return res.json({
    success: true,
    balance: final._lastBalance || 'Unknown',
    balanceMB: final._lastBalanceMB || 0,
    checkedAt: final._lastBalanceCheckedAt || null,
    cacheAge: cacheAge2,
    fresh: false,
    note: noteStr,
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


// POST /retry-callback — manually re-fire a callback for a stuck/old order.
// Body: { filename, status?, completedAt?, orderId?, orderIds?, markLocalDone? }
// - orderId/orderIds override the status-log lookup.
// - markLocalDone (default true): also updates the local status log + uploaded list
//   so the bot stops treating the file as active/pending.
app.post('/retry-callback', async (req, res) => {
  const { filename, status, completedAt, orderId, orderIds } = req.body || {};
  const markLocalDone = req.body.markLocalDone !== false; // default true

  if (!filename || typeof filename !== 'string') {
    return res.status(400).json({ success: false, error: 'filename is required' });
  }

  const orderSystemUrl = process.env.ORDERSYSTEM_URL;
  const secret = process.env.GROUPSHARE_CALLBACK_SECRET;

  if (!orderSystemUrl) return res.status(500).json({ success: false, error: 'ORDERSYSTEM_URL not configured' });
  if (!secret)         return res.status(500).json({ success: false, error: 'GROUPSHARE_CALLBACK_SECRET not configured' });

  const statusLog = loadStatusLog();

  // Resolve order IDs: body override → flat status-log keys → scan merged batch records
  let resolvedOrderIds = orderIds || null;
  let resolvedOrderId  = orderId  || null;

  if (!resolvedOrderIds && !resolvedOrderId) {
    if (statusLog[`${filename}_orderIds`]) {
      resolvedOrderIds = statusLog[`${filename}_orderIds`];
    } else if (statusLog[`${filename}_orderId`]) {
      resolvedOrderId = statusLog[`${filename}_orderId`];
    } else {
      // Scan merged batch records (covers the old bug where flat keys were missing)
      for (const [key, val] of Object.entries(statusLog)) {
        if (!key.startsWith('NM-merged-') || typeof val !== 'object' || !val.sourceFiles) continue;
        const src = val.sourceFiles.find(s => s.filename === filename);
        if (src) {
          if (src.orderIds) { resolvedOrderIds = src.orderIds; break; }
          if (src.orderId)  { resolvedOrderId  = src.orderId;  break; }
        }
      }
    }
  }

  const resolvedStatus      = (status && typeof status === 'string') ? status : 'DONE';
  const resolvedCompletedAt = (completedAt && typeof completedAt === 'string')
    ? completedAt
    : (statusLog[`${filename}_completedAt`] || new Date().toISOString());

  const payload = { filename, status: resolvedStatus, completedAt: resolvedCompletedAt };
  if (resolvedOrderIds) payload.orderIds = resolvedOrderIds;
  else if (resolvedOrderId) payload.orderId = resolvedOrderId;

  const url = `${orderSystemUrl.replace(/\/$/, '')}/api/groupshare/callback?secret=${encodeURIComponent(secret)}`;

  console.log(`📡 Manual retry-callback for "${filename}" (${resolvedStatus}) — orderIds: ${JSON.stringify(resolvedOrderIds ?? resolvedOrderId ?? null)}`);

  let callbackResult;
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const text = await response.text().catch(() => '');
    if (response.ok) {
      console.log(`📤 retry-callback sent for "${filename}" — HTTP ${response.status}`);
      callbackResult = { success: true, httpStatus: response.status };
    } else {
      console.warn(`⚠️  retry-callback for "${filename}" returned HTTP ${response.status}: ${text}`);
      return res.status(502).json({ success: false, httpStatus: response.status, body: text, payload });
    }
  } catch (err) {
    console.error(`❌ retry-callback failed for "${filename}": ${err.message}`);
    return res.status(502).json({ success: false, error: err.message, payload });
  }

  // Optionally mark the file as done in the local status log + uploaded list.
  // This stops the bot from treating the file as active/pending on next cycle.
  let localUpdated = false;
  if (markLocalDone) {
    try {
      withFileLock(STATUS_LOG, () => {
        const l = loadStatusLog();
        // If this filename IS a merged batch record (object), preserve its metadata
        if (typeof l[filename] === 'object' && l[filename] !== null) {
          l[filename] = { ...l[filename], status: resolvedStatus, completedAt: resolvedCompletedAt };
        } else {
          // Flat status key — covers standalone source files
          l[filename] = resolvedStatus;
          l[`${filename}_completedAt`] = resolvedCompletedAt;
        }
        atomicWrite(STATUS_LOG, JSON.stringify(l, null, 2));
      });
      // Add to uploaded list so the bot skips it on the next scan
      withFileLock(UPLOADED_LOG, () => {
        const uploaded = loadUploadedLog();
        if (!uploaded.includes(filename)) {
          uploaded.push(filename);
          atomicWrite(UPLOADED_LOG, JSON.stringify(uploaded, null, 2));
        }
      });
      localUpdated = true;
      console.log(`📝 Local status for "${filename}" updated to ${resolvedStatus} and added to uploaded list`);
    } catch (err) {
      console.warn(`⚠️  retry-callback: failed to update local status for "${filename}": ${err.message}`);
    }
  }

  return res.json({ ...callbackResult, localUpdated, payload });
});


// GET /health — fast liveness probe for Render / uptime monitors.
// Must respond in well under 5 s — deliberately does NO disk I/O.
app.get('/health', (req, res) => {
  const mem = process.memoryUsage();
  const uptimeSecs = Math.floor(process.uptime());

  // Cache warmth: peek at TTL without doing any I/O
  const cacheStatus = {
    statusLog: _cache.statusLog.at > 0 && (Date.now() - _cache.statusLog.at) < STATUS_LOG_TTL,
    uploadedLog: _cache.uploadedLog.at > 0 && (Date.now() - _cache.uploadedLog.at) < UPLOADED_LOG_TTL,
    diskUsage: _cache.diskUsage.at > 0 && (Date.now() - _cache.diskUsage.at) < DISK_USAGE_TTL,
  };

  res.set('Cache-Control', 'no-store');
  res.json({
    status:  'ok',
    uptime:  uptimeSecs,
    uptimeHuman: uptimeSecs < 60
      ? `${uptimeSecs}s`
      : uptimeSecs < 3600
        ? `${Math.floor(uptimeSecs / 60)}m ${uptimeSecs % 60}s`
        : `${Math.floor(uptimeSecs / 3600)}h ${Math.floor((uptimeSecs % 3600) / 60)}m`,
    memory: {
      heapUsedMB: +(mem.heapUsed / 1024 / 1024).toFixed(1),
      heapTotalMB: +(mem.heapTotal / 1024 / 1024).toFixed(1),
      rssMB: +(mem.rss / 1024 / 1024).toFixed(1),
    },
    cache: cacheStatus,
    node: process.version,
    time: new Date().toISOString(),
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

  // Auto-refresh balance if cache is stale (> 2 min) — avoids waiting for the bot
  const BALANCE_STALE_MS = 2 * 60 * 1000;
  if (!statusLog._lastBalanceCheckedAt || Date.now() - new Date(statusLog._lastBalanceCheckedAt).getTime() > BALANCE_STALE_MS) {
    fetchFreshBalanceFromPortal().catch(() => {}); // fire-and-forget; result is in status log
  }

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
  });
});


// ── EVD AIRTIME API (gbeyfia.com) ──────────────────────────
// Env vars: EVD_API_URL          — base URL, default https://gbeyfia.com/api/v1
//           EVD_ACCOUNT_n_KEY, EVD_ACCOUNT_n_PHONE, EVD_ACCOUNT_n_AMOUNT,
//           EVD_ACCOUNT_n_NETWORK  (n = 1, 2, 3, …)
//           EVD_CALLBACK_SECRET    — used to verify incoming callbacks via HMAC-SHA256

const EVD_API_BASE = (process.env.EVD_API_URL || 'https://gbeyfia.com/api/v1').replace(/\/$/, '');

const EVD_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.evd-orders.json');

function loadEvdLog() {
  try {
    if (fs.existsSync(EVD_LOG)) return JSON.parse(fs.readFileSync(EVD_LOG, 'utf8'));
  } catch {
    try {
      if (fs.existsSync(EVD_LOG)) return JSON.parse(fs.readFileSync(EVD_LOG, 'utf8'));
    } catch {}
  }
  return [];
}

function saveEvdLog(orders) {
  atomicWrite(EVD_LOG, JSON.stringify(orders, null, 2));
}

function upsertEvdOrder(order) {
  // Strip undefined values so partial updates (e.g. callbacks missing some fields)
  // do not overwrite existing good data with undefined.
  const clean = Object.fromEntries(Object.entries(order).filter(([, v]) => v !== undefined));
  // Normalise order_id to string so numeric send-response IDs match string callback IDs
  if (clean.order_id != null) clean.order_id = String(clean.order_id);
  withFileLock(EVD_LOG, () => {
    let orders = loadEvdLog();
    if (clean.order_id != null) {
      // Remove ALL existing entries with this order_id (handles any duplicates that may
      // have accumulated), merge the update into the first/most-recent match, then
      // re-insert at the front so the latest state is always index 0.
      const existing = orders.find(o => String(o.order_id) === clean.order_id);
      orders = orders.filter(o => String(o.order_id) !== clean.order_id);
      orders.unshift(existing ? { ...existing, ...clean } : clean);
    } else {
      orders.unshift(clean); // no order_id — can't deduplicate, just prepend
    }
    saveEvdLog(orders);
  });
}

// Load all configured EVD accounts from env (EVD_ACCOUNT_1_*, EVD_ACCOUNT_2_*, …)
function getEvdAccounts() {
  const accounts = [];
  for (let n = 1; n <= 20; n++) {
    const key     = process.env[`EVD_ACCOUNT_${n}_KEY`];
    const phone   = process.env[`EVD_ACCOUNT_${n}_PHONE`];
    const amount  = parseFloat(process.env[`EVD_ACCOUNT_${n}_AMOUNT`] || '0');
    const network = process.env[`EVD_ACCOUNT_${n}_NETWORK`] || 'MTN';
    if (!key || key.startsWith('evd_your')) break; // stop at placeholder / missing
    accounts.push({ index: n, key, phone, amount, network });
  }
  return accounts;
}

// POST /evd/send — trigger an airtime top-up via the gbeyfia.com API.
// Body: { accountIndex? }  — defaults to account 1 (or send all if accountIndex === 'all')
app.post('/evd/send', async (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ success: false, error: 'Unauthorized' });

  const accounts = getEvdAccounts();
  if (!accounts.length) {
    return res.status(500).json({ success: false, error: 'No EVD accounts configured in environment' });
  }

  const { accountIndex } = req.body || {};
  const targets = (accountIndex === 'all')
    ? accounts
    : [accounts[(parseInt(accountIndex) || 1) - 1]].filter(Boolean);

  if (!targets.length) {
    return res.status(400).json({ success: false, error: `Account ${accountIndex} not found` });
  }

  const results = [];
  for (const acct of targets) {
    const ref = `evans_bot_${Date.now()}`;
    const payload = {
      key:     acct.key,
      phone:   acct.phone,
      amount:  acct.amount,
      network: acct.network,
      ref,
    };

    console.log(`📡 EVD send: account ${acct.index} — GH¢ ${acct.amount} → ${acct.phone} (${acct.network}) ref=${ref}`);

    try {
      const r = await fetch(`${EVD_API_BASE}/send`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      const data = await r.json().catch(() => ({}));

      if (r.ok && data.success) {
        const order = {
          order_id:    data.order_id,
          account:     acct.index,
          phone:       acct.phone,
          network:     acct.network,
          amount:      data.amount ?? acct.amount,
          status:      data.status ?? 'queued',
          ref:         data.ref ?? ref,
          chunks:      data.chunks ?? null,
          sentAt:      new Date().toISOString(),
          completedAt: null,
          paidAmount:  null,
        };
        upsertEvdOrder(order);
        console.log(`✅ EVD order #${data.order_id} queued — ${acct.phone} GH¢ ${order.amount}`);
        results.push({ success: true, account: acct.index, ...data });
      } else {
        // Map HTTP codes to meaningful messages
        const HTTP_ERRORS = {
          401: 'Bad or missing API key',
          402: 'Insufficient EVD wallet balance',
          403: 'EVD account inactive or deleted',
          422: 'Validation error — check phone/amount/network',
          500: 'EVD server error — retry later',
          503: 'EVD maintenance mode — retry later',
        };
        const msg = data.message || HTTP_ERRORS[r.status] || `HTTP ${r.status}`;
        console.warn(`⚠️  EVD send account ${acct.index} failed: ${msg}`);
        results.push({ success: false, account: acct.index, httpStatus: r.status, error: msg });
      }
    } catch (err) {
      console.error(`❌ EVD send account ${acct.index} error: ${err.message}`);
      results.push({ success: false, account: acct.index, error: err.message });
    }
  }

  const allOk = results.every(r => r.success);
  return res.status(allOk ? 200 : 207).json({ success: allOk, results });
});

// POST /evd/callback — receives order.completed webhook from gbeyfia.com.
// Verified via HMAC-SHA256: X-EVD-Signature header = hmac(EVD_CALLBACK_SECRET, raw body).
// NOTE: express.json() (global) parses the body before any route-level middleware runs, so
// express.raw() cannot be used here. Raw body is captured via the verify hook on express.json().
app.post('/evd/callback', (req, res) => {
  const secret = process.env.EVD_CALLBACK_SECRET;
  if (secret && secret !== 'your_evd_callback_secret_here') {
    const sig  = req.headers['x-evd-signature'] || '';
    const hmac = crypto.createHmac('sha256', secret).update(req.rawBody || '').digest('hex');
    // Guard length before timingSafeEqual — it throws RangeError when buffers differ in size
    if (sig.length !== hmac.length || !crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(hmac))) {
      console.warn('⚠️  EVD callback: invalid signature');
      return res.status(401).json({ success: false, error: 'Invalid signature' });
    }
  }

  // req.body is already parsed by the global express.json() middleware
  const body = req.body;
  if (!body || typeof body !== 'object') {
    return res.status(400).json({ success: false, error: 'Invalid JSON body' });
  }

  const { event, order_id, status, paid_amount, phone, ref, chunks, timestamp } = body;

  console.log(`📲 EVD callback: event=${event} order_id=${order_id} status=${status} phone=${phone} amount=${paid_amount}`);

  if (order_id != null) {
    // Only stamp completedAt for terminal statuses — intermediate updates like
    // "queued" or "processing" should not set a premature completedAt.
    const TERMINAL = ['completed', 'failed', 'cancelled', 'success'];
    const isTerminal = status && TERMINAL.includes(status.toLowerCase());
    upsertEvdOrder({
      order_id,
      phone:       phone       ?? undefined,
      status:      status      ?? undefined,
      paidAmount:  paid_amount ?? undefined,
      ref:         ref         ?? undefined,
      chunks:      chunks      ?? undefined,
      completedAt: isTerminal
        ? (timestamp ? new Date(timestamp).toISOString() : new Date().toISOString())
        : undefined,
    });
  }

  return res.json({ success: true });
});

// GET /evd/history — paginated airtime order history.
// Query: ?page=1&limit=50
app.get('/evd/history', (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ success: false, error: 'Unauthorized' });

  const orders = loadEvdLog();
  const page   = Math.max(1, parseInt(req.query.page  || '1'));
  const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
  const start  = (page - 1) * limit;
  const slice  = orders.slice(start, start + limit);

  return res.json({
    success: true,
    total:   orders.length,
    page,
    limit,
    orders:  slice,
  });
});

// GET /evd/accounts — list configured accounts (keys masked).
app.get('/evd/accounts', (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ success: false, error: 'Unauthorized' });
  const accounts = getEvdAccounts().map(a => ({
    index:   a.index,
    phone:   a.phone,
    amount:  a.amount,
    network: a.network,
    key:     a.key.slice(0, 8) + '…',
  }));
  return res.json({ success: true, accounts });
});

// Returns the effective enabled state: UI override (status log) takes precedence over env var.
function evdAutoIsEnabled() {
  const log = loadStatusLog();
  if (log._evdAutoEnabled != null) return log._evdAutoEnabled === true;
  return process.env.EVD_AUTO_ENABLED === 'true';
}

// Returns effective time window: status-log override > env var > null.
function evdAutoWindow() {
  const log = loadStatusLog();
  return {
    startTime: log._evdAutoStartTime ?? process.env.EVD_AUTO_START_TIME ?? null,
    endTime:   log._evdAutoEndTime   ?? process.env.EVD_AUTO_END_TIME   ?? null,
  };
}

// GET /evd/auto-status — current state of the auto-loader.
app.get('/evd/auto-status', (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ success: false, error: 'Unauthorized' });
  const enabled    = evdAutoIsEnabled();
  const minBalance = parseFloat(process.env.EVD_AUTO_MIN_BALANCE_GHC || '20');
  const pollMins   = parseFloat(process.env.EVD_AUTO_POLL_MINS || '3');
  const { startTime, endTime } = evdAutoWindow();
  const log        = loadStatusLog();
  const ghcBalance = log._lastAccountBalance ?? null;
  const orders     = loadEvdLog();
  const inFlight   = orders.find(o => { const s = (o.status||'').toLowerCase(); return s==='queued'||s==='processing'||s==='pending'; });
  return res.json({
    success: true,
    enabled,
    minBalance,
    pollMins,
    startTime,
    endTime,
    currentBalance:     ghcBalance,
    balanceUnknown:     ghcBalance == null,
    belowThreshold:     ghcBalance != null && ghcBalance < minBalance,
    inFlightOrder:      inFlight ? { order_id: inFlight.order_id, status: inFlight.status } : null,
  });
});

// POST /evd/auto-toggle — enable or disable the auto-loader from the UI.
// Body: { enabled: true|false }
app.post('/evd/auto-toggle', (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ success: false, error: 'Unauthorized' });
  const { enabled } = req.body;
  if (typeof enabled !== 'boolean') return res.status(400).json({ success: false, error: 'enabled must be boolean' });
  updateStatusLog({ _evdAutoEnabled: enabled });
  console.log(`🤖 EVD auto-loader ${enabled ? 'enabled' : 'disabled'} via dashboard`);
  return res.json({ success: true, enabled });
});

// POST /evd/auto-settings — update the active time window from the dashboard.
// Body: { startTime: "HH:MM" | null, endTime: "HH:MM" | null }
app.post('/evd/auto-settings', (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ success: false, error: 'Unauthorized' });
  const { startTime, endTime } = req.body || {};
  const TIME_RE = /^([01]\d|2[0-3]):[0-5]\d$/;
  if (startTime != null && startTime !== '' && !TIME_RE.test(startTime))
    return res.status(400).json({ success: false, error: 'startTime must be HH:MM (24 h)' });
  if (endTime != null && endTime !== '' && !TIME_RE.test(endTime))
    return res.status(400).json({ success: false, error: 'endTime must be HH:MM (24 h)' });
  const updates = {};
  if (startTime !== undefined) updates._evdAutoStartTime = startTime || null;
  if (endTime   !== undefined) updates._evdAutoEndTime   = endTime   || null;
  updateStatusLog(updates);
  const { startTime: st, endTime: et } = evdAutoWindow();
  console.log(`🤖 EVD active window updated: ${st || 'any'}–${et || 'any'} UTC`);
  return res.json({ success: true, startTime: st, endTime: et });
});


// ── SCHEDULED CLEANUP ─────────────────────────────────────
// Run on startup, then every hour
console.log(`🧹 File retention set to ${RETENTION_HOURS} hours`);
cleanupOldFiles();
setInterval(cleanupOldFiles, 60 * 60 * 1000);

// ── EVD AUTO AIRTIME LOADER ────────────────────────────────
// Polls the MTN portal GHC balance every EVD_AUTO_POLL_MINS minutes.
// If balance < EVD_AUTO_MIN_BALANCE_GHC and no EVD order is currently
// in-flight (queued/processing), triggers a send for all configured accounts.
// Cooldown is order-state-based, not time-based: once the pending order
// completes (via webhook) the next poll will re-evaluate and buy again if
// still low, so response time is bounded by the poll interval only.
async function runEvdAutoLoader() {
  if (!evdAutoIsEnabled()) return;

  // Active-window check — only run between configured start and end times (24 h HH:MM, UTC).
  // Window is set via dashboard (status log) or env var fallback.
  const { startTime, endTime } = evdAutoWindow();
  if (startTime && endTime) {
    const now  = new Date();
    const hhmm = `${String(now.getUTCHours()).padStart(2,'0')}:${String(now.getUTCMinutes()).padStart(2,'0')}`;
    if (hhmm < startTime || hhmm >= endTime) {
      console.log(`🤖 EVD auto-loader: outside active window (${startTime}–${endTime} UTC), current UTC time ${hhmm} — skipped`);
      return;
    }
  }

  const minBalance = parseFloat(process.env.EVD_AUTO_MIN_BALANCE_GHC || '20');
  const log        = loadStatusLog();
  let ghcBalance   = null;

  // Always fetch a fresh GHC balance directly from the MTN portal API.
  // This avoids any stale/missing _lastAccountBalance issues.
  if (log._portalCookieHeader) {
    try {
      const balRes = await fetch('https://up2u.mtn.com.gh/providers/api/check-balance', {
        method: 'POST',
        headers: {
          'Accept':       'application/json, text/plain, */*',
          'Content-Type': 'application/json',
          'Cookie':       log._portalCookieHeader,
          'Origin':       'https://up2u.mtn.com.gh',
          'Referer':      'https://up2u.mtn.com.gh/',
          'User-Agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        },
        body: JSON.stringify({}),
      });
      const balData = await balRes.json().catch(() => ({}));
      if (balData.success && typeof balData.body?.MainAccountBalanceCedis === 'number') {
        ghcBalance = balData.body.MainAccountBalanceCedis;
        updateStatusLog({
          _lastAccountBalance:     ghcBalance,
          _lastAccountBalanceText: `GH¢ ${ghcBalance.toFixed(2)}`,
        });
        console.log(`🤖 EVD auto-loader: GHC balance = GH¢ ${ghcBalance.toFixed(2)}`);
      } else {
        console.warn(`🤖 EVD auto-loader: balance API returned unexpected response — falling back to cached value`);
        ghcBalance = log._lastAccountBalance ?? null;
      }
    } catch (fetchErr) {
      console.warn(`🤖 EVD auto-loader: balance fetch failed (${fetchErr.message}) — falling back to cached value`);
      ghcBalance = log._lastAccountBalance ?? null;
    }
  } else {
    // No cookie yet (bot hasn't logged in) — fall back to whatever is cached
    ghcBalance = log._lastAccountBalance ?? null;
    if (ghcBalance != null) {
      console.log(`🤖 EVD auto-loader: no portal cookie yet — using cached GHC balance GH¢ ${ghcBalance.toFixed(2)}`);
    }
  }

  // Can't evaluate without a known GHC balance
  if (ghcBalance == null) {
    console.log('🤖 EVD auto-loader: skipped — GHC account balance not yet known');
    return;
  }

  if (ghcBalance >= minBalance) return; // balance is fine

  // Check if any EVD order is still in-flight (queued / processing).
  // Orders array is newest-first (unshift), so build a map of the LATEST status per
  // order_id. This avoids being blocked by stale "queued" duplicates that were
  // already resolved by a later callback entry in the log.
  const orders = loadEvdLog();
  const latestByOrderId = new Map();
  for (const o of orders) {
    const id = o.order_id != null ? String(o.order_id) : null;
    if (!id || latestByOrderId.has(id)) continue; // first hit = most recent
    latestByOrderId.set(id, o);
  }
  const inFlight = [...latestByOrderId.values()].find(o => {
    const s = (o.status || '').toLowerCase();
    return s === 'queued' || s === 'processing' || s === 'pending';
  });
  if (inFlight) {
    console.log(`🤖 EVD auto-loader: balance GH¢ ${ghcBalance.toFixed(2)} < GH¢ ${minBalance} but order #${inFlight.order_id} is still ${inFlight.status} — waiting`);
    return;
  }

  console.log(`🤖 EVD auto-loader: balance GH¢ ${ghcBalance.toFixed(2)} < GH¢ ${minBalance} — triggering purchase for all accounts`);

  const accounts = getEvdAccounts();
  for (const acct of accounts) {
    const ref = `evans_bot_auto_${Date.now()}`;
    const payload = { key: acct.key, phone: acct.phone, amount: acct.amount, network: acct.network, ref };
    try {
      const r    = await fetch(`${EVD_API_BASE}/send`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await r.json().catch(() => ({}));
      if (r.ok && data.success) {
        upsertEvdOrder({
          order_id:    data.order_id,
          account:     acct.index,
          phone:       acct.phone,
          network:     acct.network,
          amount:      data.amount ?? acct.amount,
          status:      data.status ?? 'queued',
          ref:         data.ref ?? ref,
          chunks:      data.chunks ?? null,
          sentAt:      new Date().toISOString(),
          completedAt: null,
          paidAmount:  null,
          auto:        true,
        });
        console.log(`✅ EVD auto-loader: order #${data.order_id} queued — GH¢ ${acct.amount} → ${acct.phone}`);
      } else {
        const HTTP_ERRORS = { 401: 'Bad API key', 402: 'Insufficient EVD wallet balance', 422: 'Validation error', 500: 'EVD server error', 503: 'EVD maintenance' };
        const msg = data.message || HTTP_ERRORS[r.status] || `HTTP ${r.status}`;
        console.warn(`⚠️  EVD auto-loader account ${acct.index} failed: ${msg}`);
      }
    } catch (err) {
      console.error(`❌ EVD auto-loader account ${acct.index} error: ${err.message}`);
    }
  }
}

{
  const pollMins = Math.max(1, parseFloat(process.env.EVD_AUTO_POLL_MINS || '3'));
  if (process.env.EVD_AUTO_ENABLED === 'true') {
    const { startTime: _st, endTime: _et } = evdAutoWindow();
    const window = (_st && _et) ? `, active ${_st}–${_et} UTC` : ', active all day';
    console.log(`🤖 EVD auto-loader enabled — threshold GH¢ ${process.env.EVD_AUTO_MIN_BALANCE_GHC || '20'}, polling every ${pollMins} min${window}`);
  }
  setInterval(runEvdAutoLoader, pollMins * 60 * 1000);
}


// ── PORT BINDING ───────────────────────────────────────────
const API_PORT = process.env.PORT || process.env.API_PORT || 7070;
const PUBLIC_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${API_PORT}`;


const server = app.listen(API_PORT, () => {
  console.log(`🚀 API server running on port ${API_PORT}`);
  // Increase timeouts to prevent 502 Bad Gateway from reverse proxy (Render)
  server.keepAliveTimeout = 120000; // 120s (must exceed proxy idle timeout)
  server.headersTimeout = 125000;   // slightly above keepAliveTimeout
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
