'use strict';

var http      = require('http');
var XMLParser = require('fast-xml-parser').XMLParser;

var CONTROL_PATH = '/MediaRenderer/RenderingControl/Control';
var SERVICE_TYPE = 'urn:schemas-upnp-org:service:RenderingControl:1';

var parser = new XMLParser({
  ignoreAttributes:    false,
  attributeNamePrefix: '@_',
  parseAttributeValue: true
});

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
    // Without this a stale address stalls on the OS TCP timeout (minutes),
    // which would stall the reconnect loop that depends on failing fast.
    req.setTimeout(REQUEST_TIMEOUT_MS, function() {
      req.abort();
      reject(new Error('request timed out after ' + REQUEST_TIMEOUT_MS + 'ms'));
    });
    if (body) req.write(body);
    req.end();
  });
}

// Build a minimal UPnP SOAP envelope for the given action and arg object.
function soapEnvelope(action, args) {
  var argsXml = Object.keys(args).map(function(k) {
    return '<' + k + '>' + args[k] + '</' + k + '>';
  }).join('');

  return (
    '<?xml version="1.0" encoding="utf-8"?>' +
    '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"' +
    ' s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">' +
    '<s:Body>' +
    '<u:' + action + ' xmlns:u="' + SERVICE_TYPE + '">' +
    argsXml +
    '</u:' + action + '>' +
    '</s:Body>' +
    '</s:Envelope>'
  );
}

function soapRequest(device, action, args) {
  var body = soapEnvelope(action, args);

  var opts = {
    hostname: device.ip,
    port:     device.port,
    path:     CONTROL_PATH,
    method:   'POST',
    headers: {
      'Content-Type':   'text/xml; charset="utf-8"',
      'SOAPACTION':     '"' + SERVICE_TYPE + '#' + action + '"',
      'Content-Length': Buffer.byteLength(body, 'utf8')
    }
  };

  return httpRequest(opts, body);
}

// Set Master volume (0–100). Resolves when the SOAP call returns 200.
async function setVolume(device, volume) {
  var res = await soapRequest(device, 'SetVolume', {
    InstanceID:    0,
    Channel:       'Master',
    DesiredVolume: volume
  });

  if (res.statusCode !== 200) {
    throw new Error('SetVolume failed HTTP ' + res.statusCode + ': ' + res.body.slice(0, 300));
  }
}

// Query current Master volume. Returns a number.
async function getVolume(device) {
  var res = await soapRequest(device, 'GetVolume', {
    InstanceID: 0,
    Channel:    'Master'
  });

  if (res.statusCode !== 200) {
    throw new Error('GetVolume failed HTTP ' + res.statusCode + ': ' + res.body.slice(0, 300));
  }

  // Navigate: s:Envelope > s:Body > u:GetVolumeResponse > CurrentVolume.
  // fast-xml-parser uses the literal prefixed tag name as the key.
  var parsed   = parser.parse(res.body);
  var envelope = parsed['s:Envelope'] || parsed['SOAP-ENV:Envelope'] || {};
  var body     = envelope['s:Body']   || envelope['SOAP-ENV:Body']   || {};
  var response = body['u:GetVolumeResponse'] || {};
  var vol      = parseInt(response['CurrentVolume'], 10);

  if (isNaN(vol)) {
    throw new Error('GetVolume: could not parse CurrentVolume from: ' + res.body.slice(0, 300));
  }

  return vol;
}

// AVTransport lives on a different control path and service type than
// RenderingControl.
var AV_CONTROL_PATH = '/MediaRenderer/AVTransport/Control';
var AV_SERVICE_TYPE = 'urn:schemas-upnp-org:service:AVTransport:1';

// Returns 'PLAYING' | 'STOPPED' | 'PAUSED_PLAYBACK' | ... , or null if unknown.
// With TV audio flowing over eARC the Arc reports PLAYING and carries an
// x-sonos-htastream CurrentURI; idle it reports STOPPED with an empty URI.
async function getTransportState(device) {
  var body =
    '<?xml version="1.0" encoding="utf-8"?>' +
    '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"' +
    ' s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">' +
    '<s:Body><u:GetTransportInfo xmlns:u="' + AV_SERVICE_TYPE + '">' +
    '<InstanceID>0</InstanceID>' +
    '</u:GetTransportInfo></s:Body></s:Envelope>';

  var res = await httpRequest({
    hostname: device.ip,
    port:     device.port,
    path:     AV_CONTROL_PATH,
    method:   'POST',
    headers: {
      'Content-Type':   'text/xml; charset="utf-8"',
      'SOAPACTION':     '"' + AV_SERVICE_TYPE + '#GetTransportInfo"',
      'Content-Length': Buffer.byteLength(body, 'utf8')
    }
  }, body);

  if (res.statusCode !== 200) return null;
  var m = res.body.match(/<CurrentTransportState>([A-Z_]+)<\/CurrentTransportState>/);
  return m ? m[1] : null;
}

module.exports = {
  setVolume:         setVolume,
  getVolume:         getVolume,
  getTransportState: getTransportState
};
