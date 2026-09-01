'use strict';
// A small, bounded, persistent diagnostics log.
//
// Deliberately not the runtime log. /var/log on webOS is a ramfs — it is wiped
// on every boot and anything written there costs real memory, which is exactly
// how a retry loop once ate 215 KB of RAM. This file instead lives beside the
// config under /var/lib, survives the power cycle that a bug report needs it to
// survive, and is capped so it can never grow without bound.
//
// Only notable events belong here: startup, probe results, errors, and state
// changes worth explaining after the fact. Never per-keypress traffic.

var fs   = require('fs');
var path = require('path');

var MAX_BYTES  = 96 * 1024;  // hard ceiling on disk
var KEEP_BYTES = 48 * 1024;  // how much tail we keep when trimming

function DiagLog(filePath) {
  this.path    = filePath;
  this.dir     = path.dirname(filePath);
  this.ready   = false;
  this.pending = [];
  this.init();
}

DiagLog.prototype.init = function () {
  try {
    if (!fs.existsSync(this.dir)) fs.mkdirSync(this.dir, { recursive: true });
    this.ready = true;
  } catch (e) {
    // A read-only or missing /var/lib is survivable; we just lose the file.
    this.ready = false;
  }
};

DiagLog.prototype.trimIfNeeded = function () {
  try {
    if (!fs.existsSync(this.path)) return;
    if (fs.statSync(this.path).size <= MAX_BYTES) return;
    var buf  = fs.readFileSync(this.path);
    var tail = buf.slice(buf.length - KEEP_BYTES).toString('utf8');
    // Drop the partial first line so the file always starts on a record.
    var nl = tail.indexOf('\n');
    if (nl !== -1) tail = tail.slice(nl + 1);
    fs.writeFileSync(this.path,
      '--- earlier entries trimmed ---\n' + tail, 'utf8');
  } catch (e) { /* trimming is best-effort */ }
};

// level: 'info' | 'warn' | 'error'
DiagLog.prototype.write = function (level, message) {
  var line = new Date().toISOString() + '  ' +
             (level.toUpperCase() + '   ').slice(0, 5) + '  ' +
             message + '\n';

  // Keep a short in-memory tail so /api/diagnostics works even if the file does not.
  this.pending.push(line);
  if (this.pending.length > 200) this.pending.shift();

  if (!this.ready) return;
  try {
    fs.appendFileSync(this.path, line, 'utf8');
    this.trimIfNeeded();
  } catch (e) { /* best-effort */ }
};

DiagLog.prototype.info  = function (m) { this.write('info', m); };
DiagLog.prototype.warn  = function (m) { this.write('warn', m); };
DiagLog.prototype.error = function (m) { this.write('error', m); };

// Records an event only when it differs from the last one under the same key,
// so a condition that repeats every sync interval logs once, not forever.
DiagLog.prototype.change = function (key, value, message) {
  if (!this._last) this._last = {};
  if (this._last[key] === value) return;
  this._last[key] = value;
  this.write('info', message);
};

DiagLog.prototype.read = function () {
  try {
    return fs.readFileSync(this.path, 'utf8');
  } catch (e) {
    return this.pending.join('');
  }
};

DiagLog.prototype.clear = function () {
  this.pending = [];
  this._last   = {};
  try { fs.unlinkSync(this.path); } catch (e) { /* fine if absent */ }
};

module.exports = { DiagLog: DiagLog };
