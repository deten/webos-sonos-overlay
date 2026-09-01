'use strict';

var fs = require('fs');

var EV_KEY         = 1;
var KEY_MUTE       = 113;   // 0x71
var KEY_VOLUMEDOWN = 114;   // 0x72 — confirmed in Phase 2 hexdump
var KEY_VOLUMEUP   = 115;   // 0x73 — confirmed in Phase 2 hexdump

// 32-bit Linux: struct input_event = 16 bytes (little-endian)
//   [0-3]  tv_sec   uint32
//   [4-7]  tv_usec  uint32
//   [8-9]  type     uint16   1 = EV_KEY
//   [10-11] code    uint16   113/114/115
//   [12-15] value   int32    1=down 2=repeat 0=up
var STRUCT_SIZE = 16;

function codeToDirection(code) {
  if (code === KEY_VOLUMEUP)   return 'up';
  if (code === KEY_VOLUMEDOWN) return 'down';
  if (code === KEY_MUTE)       return 'mute';
  return null;
}

// Scan /proc/bus/input/devices and return Promise<{ path, candidates }>.
// Selects the highest-priority EV_KEY-capable device.
// Priority: name contains "remote"/"magic" > "keyboard"/"kbd" > anything else.
// Falls back to null if the file is unreadable or no EV_KEY device is found.
function detectInputDevice() {
  return new Promise(function(resolve) {
    fs.readFile('/proc/bus/input/devices', 'utf8', function(err, data) {
      if (err) {
        console.warn('[input] cannot read /proc/bus/input/devices:', err.message);
        resolve({ path: null, candidates: [] });
        return;
      }

      var blocks    = data.split(/\n\n+/);
      var candidates = [];

      for (var i = 0; i < blocks.length; i++) {
        var block = blocks[i];

        // Must have a Handlers line with an eventN entry.
        var hMatch = block.match(/H:\s*Handlers=([^\n]+)/);
        if (!hMatch) continue;
        var eventMatch = hMatch[1].match(/\b(event\d+)\b/);
        if (!eventMatch) continue;
        var eventPath = '/dev/input/' + eventMatch[1];

        // Must declare EV_KEY (bit 1) in its EV capabilities bitmap.
        var evMatch = block.match(/B:\s*EV=([0-9a-fA-F]+)/);
        if (!evMatch) continue;
        if (!(parseInt(evMatch[1], 16) & 0x2)) continue;

        var nMatch = block.match(/N:\s*Name="([^"]+)"/);
        var name    = nMatch ? nMatch[1] : '';

        // Check KEY bitmap for Vol+/Vol-/Mute (key codes 115/114/113 = bits 51/50/49
        // of the second 64-bit chunk). KEY= is printed high-to-low in space-separated
        // 64-bit groups; chunk[len-2] covers bits 64-127.
        var hasVolKeys = false;
        var keyLine = block.match(/B:\s*KEY=([0-9a-fA-F ]+)/);
        if (keyLine) {
          var keyParts = keyLine[1].trim().split(' ');
          if (keyParts.length >= 2) {
            // Pad to 16 hex chars, take upper 32-bit half (bits 96-127 of chunk).
            var padded  = ('0000000000000000' + keyParts[keyParts.length - 2]).slice(-16);
            var upper32 = parseInt(padded.slice(0, 8), 16);
            // Bits 49-51 of the 64-bit chunk are bits 17-19 of the upper 32-bit half.
            hasVolKeys = (upper32 & 0x000E0000) !== 0;
          }
        }

        // Priority: KEY bitmap confirms vol keys > RCU/remote/magic name > kbd > other.
        var priority = hasVolKeys                      ? 3 :
                       /rcu|remote|magic/i.test(name) ? 2 :
                       /keyboard|kbd/i.test(name)     ? 1 : 0;

        candidates.push({ path: eventPath, name: name, priority: priority, hasVolKeys: hasVolKeys });
      }

      // Highest priority first; tie-break on path lexicographically (event0 < event1).
      candidates.sort(function(a, b) {
        if (b.priority !== a.priority) return b.priority - a.priority;
        return a.path.localeCompare(b.path);
      });

      resolve({
        path:       candidates.length > 0 ? candidates[0].path : null,
        candidates: candidates
      });
    });
  });
}

// Open devicePath and emit every EV_KEY down/repeat event for vol/mute keys.
//
// onEvent({ direction, value, kernelSec, kernelUsec, recvAt })
//   direction  — 'up' | 'down' | 'mute'
//   value      — 1 (down) | 2 (repeat)
//   kernelSec  — tv_sec from the kernel input_event struct
//   kernelUsec — tv_usec from the kernel input_event struct
//   recvAt     — process.hrtime() captured immediately after fs.read returns
//
// onError(err) — called on any fatal read error
//
// Returns { close() } — call to stop reading and close the fd.
function openInputDevice(devicePath, onEvent, onError) {
  var buf      = Buffer.alloc(STRUCT_SIZE);
  var closed   = false;
  var fd       = null;
  var readCount = 0;

  fs.open(devicePath, 'r', function(err, openedFd) {
    if (err) { onError(err); return; }
    fd = openedFd;
    console.log('[input] opened', devicePath, '(fd=' + fd + ') — waiting for key events...');
    readNext();
  });

  function readNext() {
    if (closed) return;
    // position: null — advance sequentially through the character device.
    // The libuv thread-pool worker blocks in read() until the kernel delivers an event.
    fs.read(fd, buf, 0, STRUCT_SIZE, null, function(err, bytesRead) {
      if (closed) return;
      if (err) { onError(err); return; }

      var recvAt = process.hrtime();
      readCount++;

      if (bytesRead === 0) {
        // Character device returned EOF — should not happen on /dev/input.
        console.warn('[input] read returned 0 bytes (unexpected EOF) after', readCount, 'reads');
        readNext();
        return;
      }

      if (bytesRead !== STRUCT_SIZE) {
        // Partial read — skip, loop back.
        console.warn('[input] partial read:', bytesRead, 'bytes (expected', STRUCT_SIZE + ')');
        readNext();
        return;
      }

      var kernelSec  = buf.readUInt32LE(0);
      var kernelUsec = buf.readUInt32LE(4);
      var type       = buf.readUInt16LE(8);
      var code       = buf.readUInt16LE(10);
      var value      = buf.readInt32LE(12);

      if (type === EV_KEY && (value === 1 || value === 2)) {
        var direction = codeToDirection(code);
        if (direction !== null) {
          onEvent({
            direction:  direction,
            value:      value,
            kernelSec:  kernelSec,
            kernelUsec: kernelUsec,
            recvAt:     recvAt
          });
        }
      }

      readNext();
    });
  }

  function close() {
    closed = true;
    if (fd !== null) {
      fs.close(fd, function() {});
      fd = null;
    }
  }

  return { close: close };
}

// Alternative reader: spawns `cat <devicePath>` and reads via a pipe.
// Uses Node's event-loop pipe handling (uv_pipe_t) instead of fs.read's
// thread-pool blocking call — useful if fs.read on a character device is
// silently stuck in a specific Node 8 build.
//
// Note: pipe buffering adds a few ms of extra latency to recvAt vs the
// kernel timestamp embedded in each input_event struct.
function openInputDeviceViaChild(devicePath, onEvent, onError) {
  var spawn   = require('child_process').spawn;
  var closed  = false;
  var partial = null;

  var child       = null;
  var restarts    = 0;
  var everEmitted = false;

  // A device that has never produced a single event and keeps dying is not
  // coming back — observed with event10 failing 2035 times in under three
  // hours, which filled the ramfs log with 212KB of noise. Give up on those.
  // Devices that have actually worked are retried forever.
  var MAX_DEAD_RESTARTS = 5;

  function onData(chunk) {
    var recvAt = process.hrtime();
    everEmitted = true;
    restarts = 0;

    var data = partial ? Buffer.concat([partial, chunk]) : chunk;
    partial  = null;

    var offset = 0;
    while (offset + STRUCT_SIZE <= data.length) {
      var kernelSec  = data.readUInt32LE(offset + 0);
      var kernelUsec = data.readUInt32LE(offset + 4);
      var type       = data.readUInt16LE(offset + 8);
      var code       = data.readUInt16LE(offset + 10);
      var value      = data.readInt32LE(offset + 12);
      offset += STRUCT_SIZE;

      if (type === EV_KEY && (value === 1 || value === 2)) {
        var direction = codeToDirection(code);
        if (direction !== null) {
          onEvent({
            direction:  direction,
            value:      value,
            kernelSec:  kernelSec,
            kernelUsec: kernelUsec,
            recvAt:     recvAt
          });
        }
      }
    }

    if (offset < data.length) {
      partial = data.slice(offset);
    }
  }

  // dd with bs=16 reads and writes exactly one input_event per iteration with
  // no stdio buffering. 'cat' buffers stdout at ~4KB when writing to a pipe,
  // so events never reach us until 256 structs accumulate.
  //
  // This process does die in practice — observed exiting with code 1 mid-session,
  // after which every keypress went unseen while the TV and the Arc kept moving.
  // That is how the two drifted into a permanent offset. Always restart it.
  function start() {
    child = spawn('dd', ['if=' + devicePath, 'bs=16']);

    child.on('error', function(e) {
      if (!closed) onError(e);
    });

    child.on('exit', function(code) {
      if (closed) return;
      restarts += 1;

      if (!everEmitted && restarts > MAX_DEAD_RESTARTS) {
        if (restarts === MAX_DEAD_RESTARTS + 1) {
          console.warn('[input] giving up on', devicePath,
            '— never produced an event in', restarts, 'attempts');
        }
        return;
      }

      // Back off gently so a device that is gone for good cannot spin the CPU,
      // but recover fast enough that the user never notices a dropped press.
      var delay = Math.min(200 * restarts, 5000);
      console.warn('[input] reader for', devicePath, 'exited with code', code,
        '— restarting in', delay + 'ms (restart #' + restarts + ')');
      setTimeout(function() { if (!closed) start(); }, delay);
    });

    child.stdout.on('data', onData);
  }

  start();

  function close() {
    closed = true;
    if (child) child.kill();
  }

  return { close: close };
}

// Open all devicePaths simultaneously via child processes and emit events
// from whichever device actually fires. Tags each event with sourceDev so
// the caller can log which device won.
function openInputDevicesMulti(devicePaths, onEvent, onError) {
  var handles = [];
  devicePaths.forEach(function(devicePath) {
    var handle = openInputDeviceViaChild(devicePath, function(event) {
      event.sourceDev = devicePath;
      onEvent(event);
    }, function(err) {
      console.warn('[input] error on', devicePath + ':', err.message);
    });
    handles.push(handle);
  });
  return {
    close: function() { handles.forEach(function(h) { h.close(); }); }
  };
}

module.exports = {
  openInputDevice:         openInputDevice,
  openInputDeviceViaChild: openInputDeviceViaChild,
  openInputDevicesMulti:   openInputDevicesMulti,
  detectInputDevice:       detectInputDevice
};
