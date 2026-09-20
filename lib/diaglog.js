'use strict';
// A small, bounded, persistent diagnostics log.
//
// Deliberately not the runtime log. /var/log on webOS is a ramfs; it is wiped
// on every boot and anything written there costs real memory, which is exactly
// how a retry loop once ate 215 KB of RAM. This file instead lives beside the
// config under /var/lib, survives the power cycle that a bug report needs it to
// survive, and is capped so it can never grow without bound.
//
// Two of these are kept. The detail log carries the volume traffic and is
// trimmed from the front once it fills. The notable log carries only the rare
// entries worth keeping whatever else happens, so a fault on the first day of
// a two day capture is still in the file on the third.
//
// Every line carries seconds since boot as well as the wall clock, because the
// TV's clock is wrong for the first half minute after power on and steps
// forward once the network is up. Wall time alone cannot order early entries,
// and a boot can appear to start before the one before it ended.

var fs   = require('fs');
var path = require('path');

var DEFAULT_MAX_BYTES = 128 * 1024;

function uptimeSeconds() {
  try {
    return Math.round(parseFloat(fs.readFileSync('/proc/uptime', 'utf8').split(' ')[0]));
  } catch (e) { return null; }
}

// opts.maxBytes: hard ceiling on disk. Half of it survives a trim.
function DiagLog(filePath, opts) {
  opts = opts || {};
  this.path      = filePath;
  this.dir       = path.dirname(filePath);
  this.maxBytes  = opts.maxBytes || DEFAULT_MAX_BYTES;
  this.keepBytes = Math.floor(this.maxBytes / 2);
  this.tailLines = opts.tailLines || 200;
  this.ready     = false;
  this.pending   = [];
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
    if (fs.statSync(this.path).size <= this.maxBytes) return;
    var buf  = fs.readFileSync(this.path);
    var tail = buf.slice(buf.length - this.keepBytes).toString('utf8');
    // Drop the partial first line so the file always starts on a record.
    var nl = tail.indexOf('\n');
    if (nl !== -1) tail = tail.slice(nl + 1);
    fs.writeFileSync(this.path,
      '--- earlier entries trimmed ---\n' + tail, 'utf8');
  } catch (e) { /* trimming is best-effort */ }
};

// level: 'info' | 'warn' | 'error'
DiagLog.prototype.write = function (level, message) {
  var up   = uptimeSeconds();
  var line = new Date().toISOString() + '  ' +
             ('up' + (up === null ? '?' : up) + '       ').slice(0, 8) + ' ' +
             (level.toUpperCase() + '   ').slice(0, 5) + '  ' +
             message + '\n';

  // Keep a short in-memory tail so /api/diagnostics works even if the file does not.
  this.pending.push(line);
  if (this.pending.length > this.tailLines) this.pending.shift();

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
