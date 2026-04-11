const express = require('express');
const multer = require('multer');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
require('dotenv').config();
const { withFileLock, atomicWrite } = require('./lock');


const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));


const UPLOADED_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.uploaded.json');
const STATUS_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.status.json');
const RETENTION_HOURS = parseInt(process.env.FILE_RETENTION_HOURS || '24');
const QUEUE_CAPACITY_MB  = 1.5 * 1024 * 1024;               // 1.5 TB — displayed capacity
const QUEUE_THRESHOLD_MB = QUEUE_CAPACITY_MB - (10 * 1024);  // 1.49 TB — reject when pending exceeds this


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
function getPendingQueueTotalMB(statusLog, uploadedLog) {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  if (!folderPath || !fs.existsSync(folderPath)) return 0;

  const DONE_STATES = new Set(['DONE', 'ABANDONED', 'FAILED']);
  let totalMB = 0;

  const files = fs.readdirSync(folderPath)
    .filter(f => f.endsWith('.xlsx') || f.endsWith('.xls'));

  for (const file of files) {
    if (uploadedLog.includes(file)) continue;
    if (DONE_STATES.has(statusLog[file])) continue;

    if (statusLog[`${file}_totalMB`] != null) {
      totalMB += statusLog[`${file}_totalMB`];
    } else {
      // Cache miss — parse the file directly
      const { totalDataGB } = getExcelStats(path.join(folderPath, file));
      if (totalDataGB != null) totalMB += totalDataGB * 1024;
    }
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

function balanceInsufficientResponse(res, availableMB) {
  const availableGB = parseFloat((availableMB / 1024).toFixed(2));
  console.warn(`🚫 Upload rejected — data balance insufficient: ${availableGB} GB available`);
  return res.status(503)
    .set('Retry-After', '300')
    .json({
      success: false,
      error: 'BALANCE_INSUFFICIENT',
      message: `Data balance is insufficient (${availableGB} GB available). Uploads are paused until a data bundle is purchased or balance is topped up.`,
      availableDataGB: availableGB,
    });
}

function fileExceedsBalanceResponse(res, requiredMB, availableMB) {
  const requiredGB = parseFloat((requiredMB / 1024).toFixed(2));
  const availableGB = parseFloat((availableMB / 1024).toFixed(2));
  console.warn(`🚫 Upload rejected — file allocation (${requiredGB} GB) exceeds available balance (${availableGB} GB)`);
  return res.status(503)
    .set('Retry-After', '300')
    .json({
      success: false,
      error: 'BALANCE_INSUFFICIENT',
      message: `File allocation (${requiredGB} GB) exceeds available balance (${availableGB} GB). Reduce allocation size or wait for balance top-up.`,
      requiredDataGB: requiredGB,
      availableDataGB: availableGB,
    });
}

function queueFullResponse(res, pendingMB) {
  const pendingGB = parseFloat((pendingMB / 1024).toFixed(2));
  const capacityGB = parseFloat((QUEUE_CAPACITY_MB / 1024).toFixed(2));
  console.warn(`🚫 Queue full — pending: ${pendingGB} GB / capacity: ${capacityGB} GB`);
  return res.status(503)
    .set('Retry-After', '300')
    .json({
      success: false,
      error: 'QUEUE_FULL',
      message: `Upload queue is full (${pendingGB} GB pending of ${capacityGB} GB capacity). Try again after current files have been processed.`,
      pendingDataGB: pendingGB,
      capacityGB,
    });
}


// ── ROUTES ────────────────────────────────────────────────


// POST /upload — accept an Excel file from external app
app.post('/upload', (req, res, next) => {
  // Capacity check BEFORE multer writes the file to disk
  const statusLog = loadStatusLog();
  const uploadedLog = loadUploadedLog();
  const pendingMB = getPendingQueueTotalMB(statusLog, uploadedLog);
  if (pendingMB > QUEUE_THRESHOLD_MB) return queueFullResponse(res, pendingMB);
  next();
}, upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ success: false, error: 'No file provided' });
  }

  // Check if file allocation exceeds available balance
  const { totalDataGB } = getExcelStats(req.file.path);
  const fileMB = totalDataGB != null ? totalDataGB * 1024 : 0;
  const statusLogNow = loadStatusLog();
  const availableMBNow = statusLogNow._lastBalanceMB || 0;
  if (fileMB > 0 && availableMBNow > 0 && fileMB > availableMBNow) {
    try { fs.unlinkSync(req.file.path); } catch {}
    return fileExceedsBalanceResponse(res, fileMB, availableMBNow);
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

    // Balance & capacity checks before saving the file
    const newFileMB = getFileTotalMBFromBuffer(buffer);
    const statusLog = loadStatusLog();
    const uploadedLog = loadUploadedLog();
    const availableMB = statusLog._lastBalanceMB || 0;
    if (newFileMB > 0 && availableMB > 0 && newFileMB > availableMB) {
      return fileExceedsBalanceResponse(res, newFileMB, availableMB);
    }
    const pendingMB = getPendingQueueTotalMB(statusLog, uploadedLog);
    if (pendingMB + newFileMB > QUEUE_THRESHOLD_MB) {
      return queueFullResponse(res, pendingMB + newFileMB);
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

// GET /status — get status of all files
app.get('/status', (req, res) => {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  const uploaded = loadUploadedLog();
  const statusLog = loadStatusLog();

  const allFiles = fs.readdirSync(folderPath)
    .filter(f => f.endsWith('.xlsx') || f.endsWith('.xls'))
    .map(f => {
      const stats = getExcelStats(path.join(folderPath, f));
      const entry = {
        filename: f,
        status: uploaded.includes(f) ? 'DONE' : (statusLog[f] || 'PENDING'),
        queuedAt: statusLog[f + '_queuedAt'] || null,
        completedAt: statusLog[f + '_completedAt'] || null,
        totalDataGB: stats.totalDataGB,
        rowCount: stats.rowCount,
      };
      if (statusLog[f + '_orderIds']) entry.orderIds = statusLog[f + '_orderIds'];
      else if (statusLog[f + '_orderId']) entry.orderId = statusLog[f + '_orderId'];
      return entry;
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
  const entry = {
    success: true,
    filename,
    status: uploaded.includes(filename) ? 'DONE' : (statusLog[filename] || 'PENDING'),
    queuedAt: statusLog[filename + '_queuedAt'] || null,
    completedAt: statusLog[filename + '_completedAt'] || null,
    totalDataGB: stats.totalDataGB,
    rowCount: stats.rowCount,
  };
  if (statusLog[filename + '_orderIds']) entry.orderIds = statusLog[filename + '_orderIds'];
  else if (statusLog[filename + '_orderId']) entry.orderId = statusLog[filename + '_orderId'];
  res.json(entry);
});


// GET /balance — return estimated balance instantly from status log.
// Add ?refresh=true to trigger a real portal read (waits up to 25s for bot to respond).
app.get('/balance', async (req, res) => {
  const wantRefresh = req.query.refresh === 'true';

  if (!wantRefresh) {
    // Fast path — return estimated balance immediately
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
    ? `Bot is busy processing "${busyFile}" (${busyStatus}) and cannot navigate away to refresh balance. Estimated value is from ${cacheAge || 'an earlier check'}.`
    : `Bot did not respond within 25 s. Estimated value is from ${cacheAge || 'an earlier check'}. Try again shortly.`;

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