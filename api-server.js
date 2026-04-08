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


// POST /upload — accept an Excel file from external app
app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ success: false, error: 'No file provided' });
  }

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
    const ext = path.extname(filename);
    const base = path.basename(filename, ext);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const savedName = `${base}-${timestamp}${ext}`;
    const savePath = path.join(process.env.EXCEL_FOLDER_PATH, savedName);

    fs.writeFileSync(savePath, Buffer.from(data, 'base64'));
    console.log(`📥 API received base64 file: ${savedName}`);

    // Persist order reference(s) so the bot can include them in the callback
    const orderMeta = {};
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


// GET /balance — trigger a live balance refresh and return the result
app.get('/balance', async (req, res) => {
  const requestedAt = Date.now();

  // Signal the bot to do an immediate balance check
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

  // Bot did not respond in time — clear the stale flag and return cached value with context
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

  // Human-readable age of cached reading
  let cacheAge = null;
  if (final._lastBalanceCheckedAt) {
    const ageMs = Date.now() - new Date(final._lastBalanceCheckedAt).getTime();
    const ageMins = Math.round(ageMs / 60000);
    cacheAge = ageMins < 1 ? 'less than a minute ago' : `${ageMins} minute${ageMins === 1 ? '' : 's'} ago`;
  }

  const note = busyFile
    ? `Bot is busy processing "${busyFile}" (${busyStatus}) and cannot navigate away to refresh balance. Cached value is from ${cacheAge || 'an earlier check'}.`
    : `Bot did not respond within 25 s. Cached value is from ${cacheAge || 'an earlier check'}. Try again shortly.`;

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