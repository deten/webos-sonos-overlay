'use strict';

var XMLParser = require('fast-xml-parser').XMLParser;

var parserOpts = {
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  parseAttributeValue: true,
  processEntities: true    // decodes &lt; &gt; &amp; inside LastChange value
};

var parser = new XMLParser(parserOpts);

// Parse a raw GENA NOTIFY body.
// Returns an array of { name, channel, val } objects — one per state variable
// found in the InstanceID (Volume, Mute, VolumeMaster, etc.).
// Logs a warning if LastChange is absent so the caller knows which fields
// are actually present on this unit.
function parseLastChange(notifyBody) {
  var outer = parser.parse(notifyBody);

  // Sonos wraps everything in e:propertyset > e:property > LastChange.
  // Handle both prefixed and unprefixed namespace variants.
  var propertyset = outer['e:propertyset'] || outer['propertyset'] || {};
  var property    = propertyset['e:property'] || propertyset['property'] || {};

  // Multiple properties come back as an array; find the one with LastChange.
  if (Array.isArray(property)) {
    var found = null;
    for (var i = 0; i < property.length; i++) {
      if (property[i]['LastChange'] !== undefined) { found = property[i]; break; }
    }
    property = found || {};
  }

  var lastChange = property['LastChange'];
  if (lastChange === undefined || lastChange === '') {
    return [];
  }

  // LastChange value is entity-encoded XML; parse it a second time.
  var inner    = parser.parse(String(lastChange));
  var Event    = inner['Event'] || {};
  var instance = Event['InstanceID'] || {};

  var entries = [];
  var keys    = Object.keys(instance);

  for (var k = 0; k < keys.length; k++) {
    var key = keys[k];
    if (key[0] === '@') continue;          // skip attributes like @_val

    var items = instance[key];
    if (!Array.isArray(items)) items = [items];

    for (var j = 0; j < items.length; j++) {
      var item = items[j];
      if (typeof item !== 'object' || item === null) continue;
      entries.push({
        name:    key,
        channel: item['@_channel'] !== undefined ? String(item['@_channel']) : null,
        val:     item['@_val']     !== undefined ? item['@_val']             : null
      });
    }
  }

  return entries;
}

module.exports = { parseLastChange: parseLastChange };
