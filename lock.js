const fs = require('fs');

/**
 * Acquires an exclusive filesystem lock on `filePath`, runs `fn` synchronously,
 * then releases the lock. Safe across separate Node.js processes sharing the
 * same filesystem (e.g. api-server.js and index.js on Render).
 *
 * - Uses `fs.openSync('wx')` — atomic exclusive file creation on Linux/Windows.
 * - Stale lock detection: any lock older than 2 s is automatically removed.
 *   This means a crashed process never causes a permanent deadlock.
 * - Timeout fallback: if the lock still cannot be acquired after 2 s, the
 *   stale lock is force-removed and the operation proceeds anyway, so the
 *   worst case is a rare, non-fatal race — never a hang.
 */
function withFileLock(filePath, fn) {
  const lockPath = filePath + '.lock';
  const deadline = Date.now() + 2000;

  // ── Acquire lock ──────────────────────────────────────────
  while (true) {
    try {
      // 'wx' = create exclusively — fails with EEXIST if already present (atomic)
      const fd = fs.openSync(lockPath, 'wx');
      fs.closeSync(fd);
      break; // lock acquired
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;

      // Remove lock if the owning process crashed and left it stale
      try {
        const age = Date.now() - fs.statSync(lockPath).mtimeMs;
        if (age > 2000) fs.unlinkSync(lockPath);
      } catch { /* file was removed between EEXIST and stat — fine */ }

      // Deadlock protection: force-remove after deadline and proceed
      if (Date.now() >= deadline) {
        console.warn(`⚠️  Lock on ${require('path').basename(lockPath)} timed out — force-unlocking`);
        try { fs.unlinkSync(lockPath); } catch {}
        try {
          const fd = fs.openSync(lockPath, 'wx');
          fs.closeSync(fd);
        } catch { /* another process beat us — just proceed without the lock */ }
        break;
      }
      // Tight spin — the lock is held for microseconds in normal operation
    }
  }

  // ── Run the critical section ──────────────────────────────
  try {
    return fn();
  } finally {
    try { fs.unlinkSync(lockPath); } catch {}
  }
}

/**
 * Atomic write: write to a temp file then rename over the target.
 * On Linux (Render), rename() is atomic — readers always see a complete file.
 * Falls back to a direct write on Windows (where rename may occasionally fail).
 */
function atomicWrite(filePath, content) {
  const tmp = filePath + '.tmp';
  fs.writeFileSync(tmp, content, 'utf8');
  try {
    fs.renameSync(tmp, filePath);
  } catch {
    // Fallback for edge cases (e.g. cross-device rename on some systems)
    fs.writeFileSync(filePath, content, 'utf8');
    try { fs.unlinkSync(tmp); } catch {}
  }
}

module.exports = { withFileLock, atomicWrite };
