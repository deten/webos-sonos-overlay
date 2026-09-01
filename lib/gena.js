'use strict';

var http = require('http');

// Wrap http.request as a Promise.
// body must be a string or null.
var REQUEST_TIMEOUT_MS = 5000;

function httpRequest(options, body) {
  return new Promise(function(resolve, reject) {
    var req = http.request(options, function(res) {
      var chunks = [];
      res.on('data', function(chunk) { chunks.push(chunk); });
      res.on('end', function() {
        resolve({
          statusCode: res.statusCode,
          headers:    res.headers,
          body:       Buffer.concat(chunks).toString()
        });
      });
    });
    req.on('error', reject);
    // A hung SUBSCRIBE/RENEW would leave the reconnect loop awaiting forever.
    req.setTimeout(REQUEST_TIMEOUT_MS, function() {
      req.abort();
      reject(new Error('request timed out after ' + REQUEST_TIMEOUT_MS + 'ms'));
    });
    if (body) req.write(body);
    req.end();
  });
}

// Parse the negotiated TIMEOUT header value.
// Sonos may respond with a shorter timeout than we request.
// Always use this number, never the value we sent.
function parseTimeout(headerValue) {
  var m = (headerValue || '').match(/Second-(\d+)/i);
  return m ? parseInt(m[1], 10) : null;
}

// Start an HTTP server that handles incoming GENA NOTIFY requests.
// onNotify(headers, body, recvAt) is called for each NOTIFY:
//   headers:  lowercased HTTP headers object
//   body:     full request body string
//   recvAt:   process.hrtime() snapshot taken before reading the body
//
// Returns the http.Server so the caller can close it on shutdown.
function startListener(port, onNotify) {
  var server = http.createServer(function(req, res) {
    // Record receive time immediately, before body buffering.
    var recvAt = process.hrtime();

    if (req.method !== 'NOTIFY') {
      res.writeHead(405);
      res.end();
      return;
    }

    var chunks = [];
    req.on('data', function(chunk) { chunks.push(chunk); });
    req.on('end', function() {
      var body = Buffer.concat(chunks).toString();
      // 200 OK immediately; UPnP requires a response before the device will
      // send more events on this subscription.
      res.writeHead(200);
      res.end();
      try {
        onNotify(req.headers, body, recvAt);
      } catch(e) {
        console.error('[gena] onNotify threw:', e.message);
      }
    });

    req.on('error', function(e) {
      console.error('[gena] req error:', e.message);
      res.writeHead(500);
      res.end();
    });
  });

  server.listen(port, function() {
    console.log('[gena] listener on :%d', port);
  });

  return server;
}

// SUBSCRIBE to a GENA event source.
// Returns { sid, negotiatedSeconds }.
// Throws if the device returns non-200.
async function subscribe(device, callbackUrl, eventPath, requestedSeconds) {
  var opts = {
    hostname: device.ip,
    port:     device.port,
    path:     eventPath,
    method:   'SUBSCRIBE',
    headers: {
      'HOST':     device.ip + ':' + device.port,
      'CALLBACK': '<' + callbackUrl + '>',
      'NT':       'upnp:event',
      'TIMEOUT':  'Second-' + requestedSeconds
    }
  };

  var res = await httpRequest(opts, null);

  if (res.statusCode !== 200) {
    throw new Error('SUBSCRIBE returned HTTP ' + res.statusCode + ': ' + res.body.slice(0, 200));
  }

  var sid = res.headers['sid'] || res.headers['SID'] || '';
  if (!sid) throw new Error('SUBSCRIBE response missing SID header');

  var negotiatedSeconds = parseTimeout(res.headers['timeout']);
  if (negotiatedSeconds === null) {
    console.warn('[gena] TIMEOUT header missing in SUBSCRIBE response; assuming', requestedSeconds, 's');
    negotiatedSeconds = requestedSeconds;
  }

  return { sid: sid, negotiatedSeconds: negotiatedSeconds };
}

// Re-subscribe using an existing SID (renewal).
// Returns { negotiatedSeconds }.
async function renew(device, sid, eventPath, requestedSeconds) {
  var opts = {
    hostname: device.ip,
    port:     device.port,
    path:     eventPath,
    method:   'SUBSCRIBE',
    headers: {
      'HOST':    device.ip + ':' + device.port,
      'SID':     sid,
      'TIMEOUT': 'Second-' + requestedSeconds
    }
  };

  var res = await httpRequest(opts, null);

  if (res.statusCode !== 200) {
    throw new Error('RENEW returned HTTP ' + res.statusCode + ': ' + res.body.slice(0, 200));
  }

  var negotiatedSeconds = parseTimeout(res.headers['timeout']);
  if (negotiatedSeconds === null) {
    console.warn('[gena] TIMEOUT header missing in RENEW response; assuming', requestedSeconds, 's');
    negotiatedSeconds = requestedSeconds;
  }

  return { negotiatedSeconds: negotiatedSeconds };
}

// Cancel a GENA subscription. Errors are non-fatal (device may have already expired it).
async function unsubscribe(device, sid, eventPath) {
  var opts = {
    hostname: device.ip,
    port:     device.port,
    path:     eventPath,
    method:   'UNSUBSCRIBE',
    headers: {
      'HOST': device.ip + ':' + device.port,
      'SID':  sid
    }
  };

  try {
    await httpRequest(opts, null);
  } catch(e) {
    console.warn('[gena] UNSUBSCRIBE error (non-fatal):', e.message);
  }
}

module.exports = {
  startListener: startListener,
  subscribe:     subscribe,
  renew:         renew,
  unsubscribe:   unsubscribe
};
