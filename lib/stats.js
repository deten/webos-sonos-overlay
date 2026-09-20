'use strict';
// Counters and the previous-session record, both persisted across reboots.
//
// A log that runs for two days is too long to read looking for the one thing
// that went wrong. These counters give the shape of those two days in a dozen
// lines at the top of the report: how many corrections were written, how many
// external changes were adopted, how many times the player stopped sending
// events. The log is then for reading the detail once the counters say where.
//
// The session record exists for a question a running process cannot answer
// about itself: did the last run get to shut down cleanly? It is written on
// subscribe and updated by a heartbeat, so the next run can see how the last
// one ended and how long the TV was off.

var fs   = require('fs');
var path = require('path');

var FLUSH_INTERVAL_MS = 60000;   // keep flash writes rare

function Stats(filePath) {
  this.path  = filePath;
  this.dir   = path.dirname(filePath);
  this.data  = { since: new Date().toISOString(), counters: {}, session: {} };
  this.dirty = false;
  this.lastFlush = 0;
  this.load();
}

Stats.prototype.load = function () {
  try {
    var raw = JSON.parse(fs.readFileSync(this.path, 'utf8'));
    if (raw && typeof raw === 'object') {
      this.data.since    = raw.since    || this.data.since;
      this.data.counters = raw.counters || {};
      this.data.session  = raw.session  || {};
    }
  } catch (e) { /* first run, or unreadable; defaults stand */ }
};

Stats.prototype.bump = function (key, by) {
  var n = this.data.counters[key] || 0;
  this.data.counters[key] = n + (by === undefined ? 1 : by);
  this.dirty = true;
  this.flush(false);
};

// Highest value seen, for things like the longest gap between events.
Stats.prototype.max = function (key, value) {
  if (typeof value !== 'number' || isNaN(value)) return;
  if (!(key in this.data.counters) || value > this.data.counters[key]) {
    this.data.counters[key] = value;
    this.dirty = true;
    this.flush(false);
  }
};

Stats.prototype.get = function (key) {
  return this.data.counters[key] || 0;
};

Stats.prototype.counters = function () { return this.data.counters; };
Stats.prototype.since    = function () { return this.data.since; };

// The record the next run reads. Merged, not replaced.
Stats.prototype.session     = function () { return this.data.session || {}; };
Stats.prototype.prevSession = function () { return this.data.prevSession || {}; };

Stats.prototype.setSession = function (fields, flushNow) {
  var s = this.data.session || (this.data.session = {});
  Object.keys(fields).forEach(function (k) { s[k] = fields[k]; });
  this.dirty = true;
  this.flush(!!flushNow);
};

// Called once at startup: the session the previous run left behind becomes
// prevSession, and the current run starts with an empty one.
Stats.prototype.rotateSession = function () {
  this.data.prevSession = this.data.session || {};
  this.data.session     = {};
  this.dirty = true;
  this.flush(true);
  return this.data.prevSession;
};

Stats.prototype.flush = function (force) {
  var now = Date.now();
  if (!this.dirty) return;
  if (!force && (now - this.lastFlush) < FLUSH_INTERVAL_MS) return;
  try {
    if (!fs.existsSync(this.dir)) fs.mkdirSync(this.dir, { recursive: true });
    fs.writeFileSync(this.path, JSON.stringify(this.data), 'utf8');
    this.dirty     = false;
    this.lastFlush = now;
  } catch (e) { /* losing counters is survivable */ }
};

// Clearing the report clears the counters, not the record of the subscription
// this run is holding. That record is live state the next run needs, not log.
Stats.prototype.reset = function () {
  var keep = this.data.session || {};
  this.data = { since: new Date().toISOString(), counters: {}, session: keep };
  this.dirty = true;
  this.flush(true);
};

module.exports = { Stats: Stats };
