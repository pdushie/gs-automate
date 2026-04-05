const express = require('express');
const multer = require('multer');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const UPLOADED_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.uploaded.json');
const STATUS_LOG = path.join(process.env.EXCEL_FOLDER_PATH || '.', '.status.json');

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
    // Keep original filename, append timestamp to avoid collisions
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

function loadUploadedLog() {
  if (fs.existsSync(UPLOADED_LOG)) return JSON.parse(fs.readFileSync(UPLOADED_LOG, 'utf8'));
  return [];
}

function loadStatusLog() {
  if (fs.existsSync(STATUS_LOG)) return JSON.parse(fs.readFileSync(STATUS_LOG, 'utf8'));
  return {};
}

// ── ROUTES ────────────────────────────────────────────────

// POST /upload — accept an Excel file from external app
// Usage: multipart/form-data with field name "file"
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

// POST /upload-base64 — accept Excel as base64 string (for apps that can't do multipart)
// Body: { "filename": "myfile.xlsx", "data": "<base64 string>" }
app.post('/upload-base64', (req, res) => {
  const { filename, data } = req.body;

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

// GET /status — get status of all files
app.get('/status', (req, res) => {
  const folderPath = process.env.EXCEL_FOLDER_PATH;
  const uploaded = loadUploadedLog();
  const statusLog = loadStatusLog();

  const allFiles = fs.readdirSync(folderPath)
    .filter(f => f.endsWith('.xlsx') || f.endsWith('.xls'))
    .map(f => ({
      filename: f,
      status: uploaded.includes(f) ? 'DONE' : (statusLog[f] || 'PENDING'),
      queuedAt: statusLog[f + '_queuedAt'] || null,
      completedAt: statusLog[f + '_completedAt'] || null,
    }));

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

  res.json({
    success: true,
    filename,
    status: uploaded.includes(filename) ? 'DONE' : (statusLog[filename] || 'PENDING'),
    queuedAt: statusLog[filename + '_queuedAt'] || null,
    completedAt: statusLog[filename + '_completedAt'] || null,
  });
});

// GET /balance — get current data balance (read from last known check)
app.get('/balance', (req, res) => {
  const statusLog = loadStatusLog();
  res.json({
    success: true,
    balance: statusLog['_lastBalance'] || 'Unknown',
    balanceMB: statusLog['_lastBalanceMB'] || 0,
    checkedAt: statusLog['_lastBalanceCheckedAt'] || null,
  });
});

// GET /health — simple health check
app.get('/health', (req, res) => {
  res.json({ success: true, status: 'running', time: new Date().toISOString() });
});

const API_PORT = process.env.API_PORT || 7070;
app.listen(API_PORT, () => {
  console.log(`🚀 API server running at http://localhost:${API_PORT}`);
  console.log(`📡 Endpoints:`);
  console.log(`   POST http://localhost:${API_PORT}/upload         — upload .xlsx file (multipart)`);
  console.log(`   POST http://localhost:${API_PORT}/upload-base64  — upload .xlsx file (base64)`);
  console.log(`   GET  http://localhost:${API_PORT}/status         — list all file statuses`);
  console.log(`   GET  http://localhost:${API_PORT}/status/:file   — get specific file status`);
  console.log(`   GET  http://localhost:${API_PORT}/balance        — get current data balance`);
  console.log(`   GET  http://localhost:${API_PORT}/health         — health check`);
});

module.exports = app;