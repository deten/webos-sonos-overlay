'use strict';

var Client = require('node-ssdp').Client;
var url    = require('url');

// Sonos-specific search target; narrower than ssdp:all, avoids flooding.
var SONOS_ST = 'urn:schemas-upnp-org:device:ZonePlayer:1';

// Fallback: ssdp:all filtered by USN or LOCATION containing ":1400/"
var FALLBACK_ST = 'ssdp:all';

// Send one M-SEARCH with the given ST and resolve with the first matching response.
function ssdpSearch(st, timeoutMs, filter) {
  return new Promise(function(resolve, reject) {
    var client  = new Client();
    var settled = false;

    function done(result) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { client.stop(); } catch(e) {}
      if (result instanceof Error) reject(result);
      else resolve(result);
    }

    var timer = setTimeout(function() {
      done(new Error('SSDP timeout (' + st + ') after ' + timeoutMs + 'ms'));
    }, timeoutMs);

    client.on('response', function(headers) {
      var location = headers['LOCATION'] || headers['location'] || '';
      if (!location) return;
      if (filter && !filter(headers, location)) return;

      var parsed = url.parse(location);
      var ip   = parsed.hostname;
      var port = parseInt(parsed.port, 10) || 1400;

      done({ ip: ip, port: port, location: location });
    });

    client.search(st);
  });
}

// Discover the Sonos device.
// opts.timeoutMs: per-search timeout; default 5 000 ms.
// Returns { ip, port, location }.
function discoverSonos(opts) {
  opts = opts || {};
  var timeoutMs = opts.timeoutMs || 5000;

  // Targeted search first (ZonePlayer:1 is Sonos-specific, fast and quiet).
  return ssdpSearch(SONOS_ST, timeoutMs, null).catch(function(err) {
    console.log('[ssdp] targeted search failed (' + err.message + '), falling back to ssdp:all...');

    // Fallback: broadcast ssdp:all and keep only responses on port 1400.
    return ssdpSearch(FALLBACK_ST, timeoutMs, function(headers, location) {
      var usn      = headers['USN'] || headers['usn'] || '';
      var isSonos  = usn.toLowerCase().indexOf('sonos') !== -1 ||
                     usn.indexOf('ZonePlayer') !== -1 ||
                     location.indexOf(':1400/') !== -1;
      return isSonos;
    });
  });
}

module.exports = { discoverSonos: discoverSonos };
