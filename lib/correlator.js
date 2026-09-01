'use strict';

// Default: if a GENA event hasn't arrived within this window after a keypress,
// treat the keypress as unmatched (external change arrived too late, or was dropped).
var DEFAULT_WINDOW_MS = 3000;

// Joins /dev/input keypress events with GENA volume/mute events and logs timing.
//
// Usage:
//   var c = new Correlator({ windowMs: 3000 });
//   c.lastVol   = <initial volume from getVolume()>;
//   c.lastMuted = false;
//
//   On input event: c.recordKeypress(direction, recvAt)
//   On GENA event:  c.recordGena(vol, muted, recvAt)
function Correlator(opts) {
  opts = opts || {};
  this.windowMs    = opts.windowMs || DEFAULT_WINDOW_MS;
  this.pendingKeys = [];   // [{ direction, recvAt }]
  this.lastVol     = null; // seeded from getVolume() before first event
  this.lastMuted   = null;
}

// Call when a /dev/input EV_KEY down or repeat event fires.
// direction: 'up' | 'down' | 'mute'
// recvAt: process.hrtime() snapshot from lib/input.js
Correlator.prototype.recordKeypress = function(direction, recvAt) {
  this.pendingKeys.push({ direction: direction, recvAt: recvAt });
  this._pruneOldKeys();
};

// Call when a GENA NOTIFY event arrives with a Master volume or mute change.
// vol:    numeric volume (null if this event has no Volume[Master] entry)
// muted:  boolean mute state (null if this event has no Mute[Master] entry)
// recvAt: process.hrtime() snapshot from lib/gena.js startListener
Correlator.prototype.recordGena = function(vol, muted, recvAt) {
  this._pruneOldKeys();

  var direction = this._inferDirection(vol, muted);

  // Update tracked state unconditionally.
  if (vol   !== null) this.lastVol   = vol;
  if (muted !== null) this.lastMuted = muted;

  if (direction === null) {
    // Can't correlate, no prior state or no change detected.
    console.log('[corr] GENA vol=' + vol + ' muted=' + muted +
                ' | direction unknown (no prior state or no delta)');
    return;
  }

  // Find the oldest unmatched keypress in the same direction.
  var matchIdx = -1;
  for (var i = 0; i < this.pendingKeys.length; i++) {
    if (this.pendingKeys[i].direction === direction) {
      matchIdx = i;
      break;
    }
  }

  if (matchIdx === -1) {
    // GENA event for which we have no keypress record, externally driven
    // (Sonos app, another controller, catch-up event after silence).
    console.log('[corr] GENA vol=' + vol + ' dir=' + direction +
                ' | no matching keypress, external or catch-up');
    return;
  }

  var key        = this.pendingKeys.splice(matchIdx, 1)[0];
  var latencyMs  = hrtimeDiffMs(key.recvAt, recvAt);
  var queueDepth = this.pendingKeys.length;

  // This is the primary Phase 2 measurement.
  console.log('[corr]',
    'dir='        + direction,
    '| key→gena=' + latencyMs.toFixed(2) + 'ms',
    '| vol='      + vol,
    '| unmatched_keys=' + queueDepth +
    (queueDepth > 0 ? ' ⚠ hold-repeat lag' : '')
  );
};

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

Correlator.prototype._inferDirection = function(vol, muted) {
  // Only classify as mute if the mute state actually changed.
  // Volume GENA events always include Mute[Master]=0, so checking presence
  // alone causes every event to be misclassified as 'mute'.
  if (muted !== null && this.lastMuted !== null && muted !== this.lastMuted) {
    return 'mute';
  }
  if (vol !== null && this.lastVol !== null) {
    if (vol > this.lastVol) return 'up';
    if (vol < this.lastVol) return 'down';
  }
  return null;
};

Correlator.prototype._pruneOldKeys = function() {
  var windowMs  = this.windowMs;
  var now       = process.hrtime();
  var remaining = [];
  for (var i = 0; i < this.pendingKeys.length; i++) {
    var ageMs = hrtimeDiffMs(this.pendingKeys[i].recvAt, now);
    if (ageMs <= windowMs) {
      remaining.push(this.pendingKeys[i]);
    } else {
      console.warn('[corr] dropping unmatched keypress dir=' +
                   this.pendingKeys[i].direction +
                   ' age=' + ageMs.toFixed(0) + 'ms, no GENA event arrived');
    }
  }
  this.pendingKeys = remaining;
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function hrtimeDiffMs(from, to) {
  var ns = (to[0] - from[0]) * 1e9 + (to[1] - from[1]);
  return ns / 1e6;
}

module.exports = { Correlator: Correlator };
