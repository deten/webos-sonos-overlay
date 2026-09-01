"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getOwnPropSymbols = Object.getOwnPropertySymbols;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __propIsEnum = Object.prototype.propertyIsEnumerable;
var __defNormalProp = (obj2, key, value) => key in obj2 ? __defProp(obj2, key, { enumerable: true, configurable: true, writable: true, value }) : obj2[key] = value;
var __spreadValues = (a, b) => {
  for (var prop in b || (b = {}))
    if (__hasOwnProp.call(b, prop))
      __defNormalProp(a, prop, b[prop]);
  if (__getOwnPropSymbols)
    for (var prop of __getOwnPropSymbols(b)) {
      if (__propIsEnum.call(b, prop))
        __defNormalProp(a, prop, b[prop]);
    }
  return a;
};
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};

// node_modules/async-limiter/index.js
var require_async_limiter = __commonJS({
  "node_modules/async-limiter/index.js"(exports2, module2) {
    "use strict";
    function Queue(options) {
      if (!(this instanceof Queue)) {
        return new Queue(options);
      }
      options = options || {};
      this.concurrency = options.concurrency || Infinity;
      this.pending = 0;
      this.jobs = [];
      this.cbs = [];
      this._done = done.bind(this);
    }
    var arrayAddMethods = [
      "push",
      "unshift",
      "splice"
    ];
    arrayAddMethods.forEach(function(method) {
      Queue.prototype[method] = function() {
        var methodResult = Array.prototype[method].apply(this.jobs, arguments);
        this._run();
        return methodResult;
      };
    });
    Object.defineProperty(Queue.prototype, "length", {
      get: function() {
        return this.pending + this.jobs.length;
      }
    });
    Queue.prototype._run = function() {
      if (this.pending === this.concurrency) {
        return;
      }
      if (this.jobs.length) {
        var job = this.jobs.shift();
        this.pending++;
        job(this._done);
        this._run();
      }
      if (this.pending === 0) {
        while (this.cbs.length !== 0) {
          var cb = this.cbs.pop();
          process.nextTick(cb);
        }
      }
    };
    Queue.prototype.onDone = function(cb) {
      if (typeof cb === "function") {
        this.cbs.push(cb);
        this._run();
      }
    };
    function done() {
      this.pending--;
      this._run();
    }
    module2.exports = Queue;
  }
});

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports2, module2) {
    "use strict";
    module2.exports = {
      BINARY_TYPES: ["nodebuffer", "arraybuffer", "fragments"],
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      kStatusCode: Symbol("status-code"),
      kWebSocket: Symbol("websocket"),
      EMPTY_BUFFER: Buffer.alloc(0),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports2, module2) {
    "use strict";
    var { EMPTY_BUFFER } = require_constants();
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      var offset = 0;
      for (var i = 0; i < list.length; i++) {
        const buf = list[i];
        buf.copy(target, offset);
        offset += buf.length;
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (var i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      const length = buffer.length;
      for (var i = 0; i < length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.byteLength === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data)) return data;
      var buf;
      if (data instanceof ArrayBuffer) {
        buf = Buffer.from(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = viewToBuffer(data);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    function viewToBuffer(view) {
      const buf = Buffer.from(view.buffer);
      if (view.byteLength !== view.buffer.byteLength) {
        return buf.slice(view.byteOffset, view.byteOffset + view.byteLength);
      }
      return buf;
    }
    try {
      const bufferUtil = require("bufferutil");
      const bu = bufferUtil.BufferUtil || bufferUtil;
      module2.exports = {
        concat,
        mask(source, mask, output, offset, length) {
          if (length < 48) _mask(source, mask, output, offset, length);
          else bu.mask(source, mask, output, offset, length);
        },
        toArrayBuffer,
        toBuffer,
        unmask(buffer, mask) {
          if (buffer.length < 32) _unmask(buffer, mask);
          else bu.unmask(buffer, mask);
        }
      };
    } catch (e) {
      module2.exports = {
        concat,
        mask: _mask,
        toArrayBuffer,
        toBuffer,
        unmask: _unmask
      };
    }
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports2, module2) {
    "use strict";
    var Limiter = require_async_limiter();
    var zlib = require("zlib");
    var bufferUtil = require_buffer_util();
    var { kStatusCode, NOOP } = require_constants();
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var EMPTY_BLOCK = Buffer.from([0]);
    var kPerMessageDeflate = Symbol("permessage-deflate");
    var kTotalLength = Symbol("total-length");
    var kCallback = Symbol("callback");
    var kBuffers = Symbol("buffers");
    var kError = Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} options.serverNoContextTakeover Request/accept disabling
       *     of server context takeover
       * @param {Boolean} options.clientNoContextTakeover Advertise/acknowledge
       *     disabling of client context takeover
       * @param {(Boolean|Number)} options.serverMaxWindowBits Request/confirm the
       *     use of a custom server window size
       * @param {(Boolean|Number)} options.clientMaxWindowBits Advertise support
       *     for, or request, a custom client window size
       * @param {Object} options.zlibDeflateOptions Options to pass to zlib on deflate
       * @param {Object} options.zlibInflateOptions Options to pass to zlib on inflate
       * @param {Number} options.threshold Size (in bytes) below which messages
       *     should not be compressed
       * @param {Number} options.concurrencyLimit The number of concurrent calls to
       *     zlib
       * @param {Boolean} isServer Create the instance in either server or client
       *     mode
       * @param {Number} maxPayload The maximum allowed message length
       */
      constructor(options, isServer, maxPayload) {
        this._maxPayload = maxPayload | 0;
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._isServer = !!isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter({ concurrency });
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          this._deflate.close();
          this._deflate = null;
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && !params.client_max_window_bits) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            var value = params[key];
            if (value.length > 1) {
              throw new Error('Parameter "'.concat(key, '" must have only a single value'));
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num = +value;
                if (!Number.isInteger(num) || num < 8 || num > 15) {
                  throw new TypeError(
                    'Invalid value for parameter "'.concat(key, '": ').concat(value)
                  );
                }
                value = num;
              } else if (!this._isServer) {
                throw new TypeError(
                  'Invalid value for parameter "'.concat(key, '": ').concat(value)
                );
              }
            } else if (key === "server_max_window_bits") {
              const num = +value;
              if (!Number.isInteger(num) || num < 8 || num > 15) {
                throw new TypeError(
                  'Invalid value for parameter "'.concat(key, '": ').concat(value)
                );
              }
              value = num;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  'Invalid value for parameter "'.concat(key, '": ').concat(value)
                );
              }
            } else {
              throw new Error('Unknown parameter "'.concat(key, '"'));
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited by async-limiter.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.push((done) => {
          this._decompress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited by async-limiter.
       *
       * @param {Buffer} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.push((done) => {
          this._compress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = "".concat(endpoint, "_max_window_bits");
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib.createInflateRaw(
            Object.assign({}, this._options.zlibInflateOptions, { windowBits })
          );
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin) this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (fin && this.params["".concat(endpoint, "_no_context_takeover")]) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {Buffer} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        if (!data || data.length === 0) {
          process.nextTick(callback, null, EMPTY_BLOCK);
          return;
        }
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = "".concat(endpoint, "_max_window_bits");
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib.createDeflateRaw(
            Object.assign({}, this._options.zlibDeflateOptions, { windowBits })
          );
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("error", NOOP);
          this._deflate.on("data", deflateOnData);
        }
        this._deflate.write(data);
        this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          var data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) data2 = data2.slice(0, data2.length - 4);
          if (fin && this.params["".concat(endpoint, "_no_context_takeover")]) {
            this._deflate.close();
            this._deflate = null;
          } else {
            this._deflate[kTotalLength] = 0;
            this._deflate[kBuffers] = [];
          }
          callback(null, data2);
        });
      }
    };
    module2.exports = PerMessageDeflate;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports2, module2) {
    "use strict";
    var Event2 = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @param {Object} target A reference to the target to which the event was dispatched
       */
      constructor(type, target) {
        this.target = target;
        this.type = type;
      }
    };
    var MessageEvent = class extends Event2 {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {(String|Buffer|ArrayBuffer|Buffer[])} data The received data
       * @param {WebSocket} target A reference to the target to which the event was dispatched
       */
      constructor(data, target) {
        super("message", target);
        this.data = data;
      }
    };
    var CloseEvent = class extends Event2 {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {Number} code The status code explaining why the connection is being closed
       * @param {String} reason A human-readable string explaining why the connection is closing
       * @param {WebSocket} target A reference to the target to which the event was dispatched
       */
      constructor(code, reason, target) {
        super("close", target);
        this.wasClean = target._closeFrameReceived && target._closeFrameSent;
        this.reason = reason;
        this.code = code;
      }
    };
    var OpenEvent = class extends Event2 {
      /**
       * Create a new `OpenEvent`.
       *
       * @param {WebSocket} target A reference to the target to which the event was dispatched
       */
      constructor(target) {
        super("open", target);
      }
    };
    var ErrorEvent = class extends Event2 {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {Object} error The error that generated this event
       * @param {WebSocket} target A reference to the target to which the event was dispatched
       */
      constructor(error, target) {
        super("error", target);
        this.message = error.message;
        this.error = error;
      }
    };
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} method A string representing the event type to listen for
       * @param {Function} listener The listener to add
       * @public
       */
      addEventListener(method, listener) {
        if (typeof listener !== "function") return;
        function onMessage(data) {
          listener.call(this, new MessageEvent(data, this));
        }
        function onClose(code, message) {
          listener.call(this, new CloseEvent(code, message, this));
        }
        function onError(error) {
          listener.call(this, new ErrorEvent(error, this));
        }
        function onOpen() {
          listener.call(this, new OpenEvent(this));
        }
        if (method === "message") {
          onMessage._listener = listener;
          this.on(method, onMessage);
        } else if (method === "close") {
          onClose._listener = listener;
          this.on(method, onClose);
        } else if (method === "error") {
          onError._listener = listener;
          this.on(method, onError);
        } else if (method === "open") {
          onOpen._listener = listener;
          this.on(method, onOpen);
        } else {
          this.on(method, listener);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} method A string representing the event type to remove
       * @param {Function} listener The listener to remove
       * @public
       */
      removeEventListener(method, listener) {
        const listeners = this.listeners(method);
        for (var i = 0; i < listeners.length; i++) {
          if (listeners[i] === listener || listeners[i]._listener === listener) {
            this.removeListener(method, listeners[i]);
          }
        }
      }
    };
    module2.exports = EventTarget;
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports2, module2) {
    "use strict";
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function push(dest, name, elem) {
      if (Object.prototype.hasOwnProperty.call(dest, name)) dest[name].push(elem);
      else dest[name] = [elem];
    }
    function parse(header) {
      const offers = {};
      if (header === void 0 || header === "") return offers;
      var params = {};
      var mustUnescape = false;
      var isEscaping = false;
      var inQuotes = false;
      var extensionName;
      var paramName;
      var start = -1;
      var end = -1;
      for (var i = 0; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError("Unexpected character at index ".concat(i));
            }
            if (end === -1) end = i;
            const name = header.slice(start, end);
            if (code === 44) {
              push(offers, name, params);
              params = {};
            } else {
              extensionName = name;
            }
            start = end = -1;
          } else {
            throw new SyntaxError("Unexpected character at index ".concat(i));
          }
        } else if (paramName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError("Unexpected character at index ".concat(i));
            }
            if (end === -1) end = i;
            push(params, header.slice(start, end), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = {};
              extensionName = void 0;
            }
            start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1) {
            paramName = header.slice(start, i);
            start = end = -1;
          } else {
            throw new SyntaxError("Unexpected character at index ".concat(i));
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError("Unexpected character at index ".concat(i));
            }
            if (start === -1) start = i;
            else if (!mustUnescape) mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1) start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError("Unexpected character at index ".concat(i));
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end === -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError("Unexpected character at index ".concat(i));
            }
            if (end === -1) end = i;
            var value = header.slice(start, end);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = {};
              extensionName = void 0;
            }
            paramName = void 0;
            start = end = -1;
          } else {
            throw new SyntaxError("Unexpected character at index ".concat(i));
          }
        }
      }
      if (start === -1 || inQuotes) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end === -1) end = i;
      const token = header.slice(start, end);
      if (extensionName === void 0) {
        push(offers, token, {});
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension) => {
        var configurations = extensions[extension];
        if (!Array.isArray(configurations)) configurations = [configurations];
        return configurations.map((params) => {
          return [extension].concat(
            Object.keys(params).map((k) => {
              var values = params[k];
              if (!Array.isArray(values)) values = [values];
              return values.map((v) => v === true ? k : "".concat(k, "=").concat(v)).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module2.exports = { format, parse };
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports2) {
    "use strict";
    try {
      const isValidUTF8 = require("utf-8-validate");
      exports2.isValidUTF8 = typeof isValidUTF8 === "object" ? isValidUTF8.Validation.isValidUTF8 : isValidUTF8;
    } catch (e) {
      exports2.isValidUTF8 = () => true;
    }
    exports2.isValidStatusCode = (code) => {
      return code >= 1e3 && code <= 1013 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    };
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports2, module2) {
    "use strict";
    var { Writable } = require("stream");
    var PerMessageDeflate = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var Receiver = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {String} binaryType The type for binary data
       * @param {Object} extensions An object containing the negotiated extensions
       * @param {Number} maxPayload The maximum allowed message length
       */
      constructor(binaryType, extensions, maxPayload) {
        super();
        this._binaryType = binaryType || BINARY_TYPES[0];
        this[kWebSocket] = void 0;
        this._extensions = extensions || {};
        this._maxPayload = maxPayload | 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragments = [];
        this._state = GET_INFO;
        this._loop = false;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = buf.slice(n);
          return buf.slice(0, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          if (n >= buf.length) {
            this._buffers.shift().copy(dst, dst.length - n);
          } else {
            buf.copy(dst, dst.length - n, 0, n);
            this._buffers[0] = buf.slice(n);
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        var err;
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              err = this.getInfo();
              break;
            case GET_PAYLOAD_LENGTH_16:
              err = this.getPayloadLength16();
              break;
            case GET_PAYLOAD_LENGTH_64:
              err = this.getPayloadLength64();
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              err = this.getData(cb);
              break;
            default:
              this._loop = false;
              return;
          }
        } while (this._loop);
        cb(err);
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @return {(RangeError|undefined)} A possible error
       * @private
       */
      getInfo() {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          this._loop = false;
          return error(RangeError, "RSV2 and RSV3 must be clear", true, 1002);
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate.extensionName]) {
          this._loop = false;
          return error(RangeError, "RSV1 must be clear", true, 1002);
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            this._loop = false;
            return error(RangeError, "RSV1 must be clear", true, 1002);
          }
          if (!this._fragmented) {
            this._loop = false;
            return error(RangeError, "invalid opcode 0", true, 1002);
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            this._loop = false;
            return error(RangeError, "invalid opcode ".concat(this._opcode), true, 1002);
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            this._loop = false;
            return error(RangeError, "FIN must be set", true, 1002);
          }
          if (compressed) {
            this._loop = false;
            return error(RangeError, "RSV1 must be clear", true, 1002);
          }
          if (this._payloadLength > 125) {
            this._loop = false;
            return error(
              RangeError,
              "invalid payload length ".concat(this._payloadLength),
              true,
              1002
            );
          }
        } else {
          this._loop = false;
          return error(RangeError, "invalid opcode ".concat(this._opcode), true, 1002);
        }
        if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
        else return this.haveLength();
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @return {(RangeError|undefined)} A possible error
       * @private
       */
      getPayloadLength16() {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        return this.haveLength();
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @return {(RangeError|undefined)} A possible error
       * @private
       */
      getPayloadLength64() {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num = buf.readUInt32BE(0);
        if (num > Math.pow(2, 53 - 32) - 1) {
          this._loop = false;
          return error(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009
          );
        }
        this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
        return this.haveLength();
      }
      /**
       * Payload length has been read.
       *
       * @return {(RangeError|undefined)} A possible error
       * @private
       */
      haveLength() {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            this._loop = false;
            return error(RangeError, "Max payload size exceeded", false, 1009);
          }
        }
        if (this._masked) this._state = GET_MASK;
        else this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      getData(cb) {
        var data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked) unmask(data, this._mask);
        }
        if (this._opcode > 7) return this.controlMessage(data);
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        return this.dataMessage();
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              return cb(
                error(RangeError, "Max payload size exceeded", false, 1009)
              );
            }
            this._fragments.push(buf);
          }
          const er = this.dataMessage();
          if (er) return cb(er);
          this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @return {(Error|undefined)} A possible error
       * @private
       */
      dataMessage() {
        if (this._fin) {
          const messageLength = this._messageLength;
          const fragments = this._fragments;
          this._totalPayloadLength = 0;
          this._messageLength = 0;
          this._fragmented = 0;
          this._fragments = [];
          if (this._opcode === 2) {
            var data;
            if (this._binaryType === "nodebuffer") {
              data = concat(fragments, messageLength);
            } else if (this._binaryType === "arraybuffer") {
              data = toArrayBuffer(concat(fragments, messageLength));
            } else {
              data = fragments;
            }
            this.emit("message", data);
          } else {
            const buf = concat(fragments, messageLength);
            if (!isValidUTF8(buf)) {
              this._loop = false;
              return error(Error, "invalid UTF-8 sequence", true, 1007);
            }
            this.emit("message", buf.toString());
          }
        }
        this._state = GET_INFO;
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data) {
        if (this._opcode === 8) {
          this._loop = false;
          if (data.length === 0) {
            this.emit("conclude", 1005, "");
            this.end();
          } else if (data.length === 1) {
            return error(RangeError, "invalid payload length 1", true, 1002);
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              return error(RangeError, "invalid status code ".concat(code), true, 1002);
            }
            const buf = data.slice(2);
            if (!isValidUTF8(buf)) {
              return error(Error, "invalid UTF-8 sequence", true, 1007);
            }
            this.emit("conclude", code, buf.toString());
            this.end();
          }
        } else if (this._opcode === 9) {
          this.emit("ping", data);
        } else {
          this.emit("pong", data);
        }
        this._state = GET_INFO;
      }
    };
    module2.exports = Receiver;
    function error(ErrorCtor, message, prefix, statusCode) {
      const err = new ErrorCtor(
        prefix ? "Invalid WebSocket frame: ".concat(message) : message
      );
      Error.captureStackTrace(err, error);
      err[kStatusCode] = statusCode;
      return err;
    }
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports2, module2) {
    "use strict";
    var { randomBytes } = require("crypto");
    var PerMessageDeflate = require_permessage_deflate();
    var { EMPTY_BUFFER } = require_constants();
    var { isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var Sender = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {net.Socket} socket The connection socket
       * @param {Object} extensions An object containing the negotiated extensions
       */
      constructor(socket, extensions) {
        this._extensions = extensions || {};
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._deflating = false;
        this._queue = [];
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {Buffer} data The data to frame
       * @param {Object} options Options object
       * @param {Number} options.opcode The opcode
       * @param {Boolean} options.readOnly Specifies whether `data` can be modified
       * @param {Boolean} options.fin Specifies whether or not to set the FIN bit
       * @param {Boolean} options.mask Specifies whether or not to mask `data`
       * @param {Boolean} options.rsv1 Specifies whether or not to set the RSV1 bit
       * @return {Buffer[]} The framed data as a list of `Buffer` instances
       * @public
       */
      static frame(data, options) {
        const merge = options.mask && options.readOnly;
        var offset = options.mask ? 6 : 2;
        var payloadLength = data.length;
        if (data.length >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (data.length > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge ? data.length + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1) target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(data.length, 2);
        } else if (payloadLength === 127) {
          target.writeUInt32BE(0, 2);
          target.writeUInt32BE(data.length, 6);
        }
        if (!options.mask) return [target, data];
        const mask = randomBytes(4);
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (merge) {
          applyMask(data, mask, target, offset, data.length);
          return [target];
        }
        applyMask(data, mask, data, 0, data.length);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {(Number|undefined)} code The status code component of the body
       * @param {String} data The message component of the body
       * @param {Boolean} mask Specifies whether or not to mask the message
       * @param {Function} cb Callback
       * @public
       */
      close(code, data, mask, cb) {
        var buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || data === "") {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          buf = Buffer.allocUnsafe(2 + Buffer.byteLength(data));
          buf.writeUInt16BE(code, 0);
          buf.write(data, 2);
        }
        if (this._deflating) {
          this.enqueue([this.doClose, buf, mask, cb]);
        } else {
          this.doClose(buf, mask, cb);
        }
      }
      /**
       * Frames and sends a close message.
       *
       * @param {Buffer} data The message to send
       * @param {Boolean} mask Specifies whether or not to mask `data`
       * @param {Function} cb Callback
       * @private
       */
      doClose(data, mask, cb) {
        this.sendFrame(
          _Sender.frame(data, {
            fin: true,
            rsv1: false,
            opcode: 8,
            mask,
            readOnly: false
          }),
          cb
        );
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} mask Specifies whether or not to mask `data`
       * @param {Function} cb Callback
       * @public
       */
      ping(data, mask, cb) {
        const buf = toBuffer(data);
        if (this._deflating) {
          this.enqueue([this.doPing, buf, mask, toBuffer.readOnly, cb]);
        } else {
          this.doPing(buf, mask, toBuffer.readOnly, cb);
        }
      }
      /**
       * Frames and sends a ping message.
       *
       * @param {*} data The message to send
       * @param {Boolean} mask Specifies whether or not to mask `data`
       * @param {Boolean} readOnly Specifies whether `data` can be modified
       * @param {Function} cb Callback
       * @private
       */
      doPing(data, mask, readOnly, cb) {
        this.sendFrame(
          _Sender.frame(data, {
            fin: true,
            rsv1: false,
            opcode: 9,
            mask,
            readOnly
          }),
          cb
        );
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} mask Specifies whether or not to mask `data`
       * @param {Function} cb Callback
       * @public
       */
      pong(data, mask, cb) {
        const buf = toBuffer(data);
        if (this._deflating) {
          this.enqueue([this.doPong, buf, mask, toBuffer.readOnly, cb]);
        } else {
          this.doPong(buf, mask, toBuffer.readOnly, cb);
        }
      }
      /**
       * Frames and sends a pong message.
       *
       * @param {*} data The message to send
       * @param {Boolean} mask Specifies whether or not to mask `data`
       * @param {Boolean} readOnly Specifies whether `data` can be modified
       * @param {Function} cb Callback
       * @private
       */
      doPong(data, mask, readOnly, cb) {
        this.sendFrame(
          _Sender.frame(data, {
            fin: true,
            rsv1: false,
            opcode: 10,
            mask,
            readOnly
          }),
          cb
        );
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} options.compress Specifies whether or not to compress `data`
       * @param {Boolean} options.binary Specifies whether `data` is binary or text
       * @param {Boolean} options.fin Specifies whether the fragment is the last one
       * @param {Boolean} options.mask Specifies whether or not to mask `data`
       * @param {Function} cb Callback
       * @public
       */
      send(data, options, cb) {
        const buf = toBuffer(data);
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        var opcode = options.binary ? 2 : 1;
        var rsv1 = options.compress;
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate) {
            rsv1 = buf.length >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin) this._firstFragment = true;
        if (perMessageDeflate) {
          const opts = {
            fin: options.fin,
            rsv1,
            opcode,
            mask: options.mask,
            readOnly: toBuffer.readOnly
          };
          if (this._deflating) {
            this.enqueue([this.dispatch, buf, this._compress, opts, cb]);
          } else {
            this.dispatch(buf, this._compress, opts, cb);
          }
        } else {
          this.sendFrame(
            _Sender.frame(buf, {
              fin: options.fin,
              rsv1: false,
              opcode,
              mask: options.mask,
              readOnly: toBuffer.readOnly
            }),
            cb
          );
        }
      }
      /**
       * Dispatches a data message.
       *
       * @param {Buffer} data The message to send
       * @param {Boolean} compress Specifies whether or not to compress `data`
       * @param {Object} options Options object
       * @param {Number} options.opcode The opcode
       * @param {Boolean} options.readOnly Specifies whether `data` can be modified
       * @param {Boolean} options.fin Specifies whether or not to set the FIN bit
       * @param {Boolean} options.mask Specifies whether or not to mask `data`
       * @param {Boolean} options.rsv1 Specifies whether or not to set the RSV1 bit
       * @param {Function} cb Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        this._deflating = true;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          this._deflating = false;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (!this._deflating && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[1].length;
          params[0].apply(this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[1].length;
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {Buffer[]} list The frame to send
       * @param {Function} cb Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module2.exports = Sender;
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var crypto = require("crypto");
    var https = require("https");
    var http2 = require("http");
    var net = require("net");
    var tls = require("tls");
    var url = require("url");
    var PerMessageDeflate = require_permessage_deflate();
    var EventTarget = require_event_target();
    var extension = require_extension();
    var Receiver = require_receiver();
    var Sender = require_sender();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      GUID,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var protocolVersions = [8, 13];
    var closeTimeout = 30 * 1e3;
    var WebSocket = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|url.Url|url.URL)} address The URL to which to connect
       * @param {(String|String[])} protocols The subprotocols
       * @param {Object} options Connection options
       */
      constructor(address, protocols, options) {
        super();
        this.readyState = _WebSocket.CONNECTING;
        this.protocol = "";
        this._binaryType = BINARY_TYPES[0];
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = "";
        this._closeTimer = null;
        this._closeCode = 1006;
        this._extensions = {};
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._isServer = false;
          this._redirects = 0;
          if (Array.isArray(protocols)) {
            protocols = protocols.join(", ");
          } else if (typeof protocols === "object" && protocols !== null) {
            options = protocols;
            protocols = void 0;
          }
          initAsClient(this, address, protocols, options);
        } else {
          this._isServer = true;
        }
      }
      get CONNECTING() {
        return _WebSocket.CONNECTING;
      }
      get CLOSING() {
        return _WebSocket.CLOSING;
      }
      get CLOSED() {
        return _WebSocket.CLOSED;
      }
      get OPEN() {
        return _WebSocket.OPEN;
      }
      /**
       * This deviates from the WHATWG interface since ws doesn't support the
       * required default "blob" type (instead we define a custom "nodebuffer"
       * type).
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type)) return;
        this._binaryType = type;
        if (this._receiver) this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket) return 0;
        return (this._socket.bufferSize || 0) + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {net.Socket} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Number} maxPayload The maximum allowed message size
       * @private
       */
      setSocket(socket, head, maxPayload) {
        const receiver2 = new Receiver(
          this._binaryType,
          this._extensions,
          maxPayload
        );
        this._sender = new Sender(socket, this._extensions);
        this._receiver = receiver2;
        this._socket = socket;
        receiver2[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver2.on("conclude", receiverOnConclude);
        receiver2.on("drain", receiverOnDrain);
        receiver2.on("error", receiverOnError);
        receiver2.on("message", receiverOnMessage);
        receiver2.on("ping", receiverOnPing);
        receiver2.on("pong", receiverOnPong);
        socket.setTimeout(0);
        socket.setNoDelay();
        if (head.length > 0) socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this.readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        this.readyState = _WebSocket.CLOSED;
        if (!this._socket) {
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate.extensionName]) {
          this._extensions[PerMessageDeflate.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} code Status code explaining why the connection is closing
       * @param {String} data A string explaining why the connection is closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          return abortHandshake(this, this._req, msg);
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && this._closeFrameReceived) this._socket.end();
          return;
        }
        this.readyState = _WebSocket.CLOSING;
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err) return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived) this._socket.end();
        });
        this._closeTimer = setTimeout(
          this._socket.destroy.bind(this._socket),
          closeTimeout
        );
      }
      /**
       * Send a ping.
       *
       * @param {*} data The data to send
       * @param {Boolean} mask Indicates whether or not to mask `data`
       * @param {Function} cb Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (this.readyState !== _WebSocket.OPEN) {
          const err = new Error(
            "WebSocket is not open: readyState ".concat(this.readyState, " ") + "(".concat(readyStates[this.readyState], ")")
          );
          if (cb) return cb(err);
          throw err;
        }
        if (typeof data === "number") data = data.toString();
        if (mask === void 0) mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} data The data to send
       * @param {Boolean} mask Indicates whether or not to mask `data`
       * @param {Function} cb Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (this.readyState !== _WebSocket.OPEN) {
          const err = new Error(
            "WebSocket is not open: readyState ".concat(this.readyState, " ") + "(".concat(readyStates[this.readyState], ")")
          );
          if (cb) return cb(err);
          throw err;
        }
        if (typeof data === "number") data = data.toString();
        if (mask === void 0) mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} options.compress Specifies whether or not to compress `data`
       * @param {Boolean} options.binary Specifies whether `data` is binary or text
       * @param {Boolean} options.fin Specifies whether the fragment is the last one
       * @param {Boolean} options.mask Specifies whether or not to mask `data`
       * @param {Function} cb Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (this.readyState !== _WebSocket.OPEN) {
          const err = new Error(
            "WebSocket is not open: readyState ".concat(this.readyState, " ") + "(".concat(readyStates[this.readyState], ")")
          );
          if (cb) return cb(err);
          throw err;
        }
        if (typeof data === "number") data = data.toString();
        const opts = Object.assign(
          {
            binary: typeof data !== "string",
            mask: !this._isServer,
            compress: true,
            fin: true
          },
          options
        );
        if (!this._extensions[PerMessageDeflate.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          return abortHandshake(this, this._req, msg);
        }
        if (this._socket) {
          this.readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    readyStates.forEach((readyState, i) => {
      WebSocket[readyState] = i;
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket.prototype, "on".concat(method), {
        /**
         * Return the listener of the event.
         *
         * @return {(Function|undefined)} The event listener or `undefined`
         * @public
         */
        get() {
          const listeners = this.listeners(method);
          for (var i = 0; i < listeners.length; i++) {
            if (listeners[i]._listener) return listeners[i]._listener;
          }
          return void 0;
        },
        /**
         * Add a listener for the event.
         *
         * @param {Function} listener The listener to add
         * @public
         */
        set(listener) {
          const listeners = this.listeners(method);
          for (var i = 0; i < listeners.length; i++) {
            if (listeners[i]._listener) this.removeListener(method, listeners[i]);
          }
          this.addEventListener(method, listener);
        }
      });
    });
    WebSocket.prototype.addEventListener = EventTarget.addEventListener;
    WebSocket.prototype.removeEventListener = EventTarget.removeEventListener;
    module2.exports = WebSocket;
    function initAsClient(websocket, address, protocols, options) {
      const opts = Object.assign(
        {
          protocolVersion: protocolVersions[1],
          maxPayload: 100 * 1024 * 1024,
          perMessageDeflate: true,
          followRedirects: false,
          maxRedirects: 10
        },
        options,
        {
          createConnection: void 0,
          socketPath: void 0,
          hostname: void 0,
          protocol: void 0,
          timeout: void 0,
          method: void 0,
          auth: void 0,
          host: void 0,
          path: void 0,
          port: void 0
        }
      );
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          "Unsupported protocol version: ".concat(opts.protocolVersion, " ") + "(supported versions: ".concat(protocolVersions.join(", "), ")")
        );
      }
      var parsedUrl;
      if (typeof address === "object" && address.href !== void 0) {
        parsedUrl = address;
        websocket.url = address.href;
      } else {
        parsedUrl = url.URL ? new url.URL(address) : url.parse(address);
        websocket.url = address;
      }
      const isUnixSocket = parsedUrl.protocol === "ws+unix:";
      if (!parsedUrl.host && (!isUnixSocket || !parsedUrl.pathname)) {
        throw new Error("Invalid URL: ".concat(websocket.url));
      }
      const isSecure = parsedUrl.protocol === "wss:" || parsedUrl.protocol === "https:";
      const defaultPort = isSecure ? 443 : 80;
      const key = crypto.randomBytes(16).toString("base64");
      const get = isSecure ? https.get : http2.get;
      const path2 = parsedUrl.search ? "".concat(parsedUrl.pathname || "/").concat(parsedUrl.search) : parsedUrl.pathname || "/";
      var perMessageDeflate;
      opts.createConnection = isSecure ? tlsConnect : netConnect;
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = Object.assign(
        {
          "Sec-WebSocket-Version": opts.protocolVersion,
          "Sec-WebSocket-Key": key,
          Connection: "Upgrade",
          Upgrade: "websocket"
        },
        opts.headers
      );
      opts.path = path2;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate(
          opts.perMessageDeflate !== true ? opts.perMessageDeflate : {},
          false,
          opts.maxPayload
        );
        opts.headers["Sec-WebSocket-Extensions"] = extension.format({
          [PerMessageDeflate.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols) {
        opts.headers["Sec-WebSocket-Protocol"] = protocols;
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.auth) {
        opts.auth = parsedUrl.auth;
      } else if (parsedUrl.username || parsedUrl.password) {
        opts.auth = "".concat(parsedUrl.username, ":").concat(parsedUrl.password);
      }
      if (isUnixSocket) {
        const parts = path2.split(":");
        opts.socketPath = parts[0];
        opts.path = parts[1];
      }
      var req = websocket._req = get(opts);
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (websocket._req.aborted) return;
        req = websocket._req = null;
        websocket.readyState = WebSocket.CLOSING;
        websocket.emit("error", err);
        websocket.emitClose();
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          const addr = url.URL ? new url.URL(location, address) : url.resolve(address, location);
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            "Unexpected server response: ".concat(res.statusCode)
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket.CONNECTING) return;
        req = websocket._req = null;
        const digest = crypto.createHash("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        const protList = (protocols || "").split(/, */);
        var protError;
        if (!protocols && serverProt) {
          protError = "Server sent a subprotocol but none was requested";
        } else if (protocols && !serverProt) {
          protError = "Server sent no subprotocol";
        } else if (serverProt && !protList.includes(serverProt)) {
          protError = "Server sent an invalid subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt) websocket.protocol = serverProt;
        if (perMessageDeflate) {
          try {
            const extensions = extension.parse(
              res.headers["sec-websocket-extensions"]
            );
            if (extensions[PerMessageDeflate.extensionName]) {
              perMessageDeflate.accept(extensions[PerMessageDeflate.extensionName]);
              websocket._extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            abortHandshake(
              websocket,
              socket,
              "Invalid Sec-WebSocket-Extensions header"
            );
            return;
          }
        }
        websocket.setSocket(socket, head, opts.maxPayload);
      });
    }
    function netConnect(options) {
      if (options.protocolVersion) options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      options.servername = options.servername || options.host;
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket.readyState = WebSocket.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream.setHeader) {
        stream.abort();
        stream.once("abort", websocket.emitClose.bind(websocket));
        websocket.emit("error", err);
      } else {
        stream.destroy(err);
        stream.once("error", websocket.emit.bind(websocket, "error"));
        stream.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._socket.removeListener("data", socketOnData);
      websocket._socket.resume();
      websocket._closeFrameReceived = true;
      websocket._closeMessage = reason;
      websocket._closeCode = code;
      if (code === 1005) websocket.close();
      else websocket.close(code, reason);
    }
    function receiverOnDrain() {
      this[kWebSocket]._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      websocket._socket.removeListener("data", socketOnData);
      websocket.readyState = WebSocket.CLOSING;
      websocket._closeCode = err[kStatusCode];
      websocket.emit("error", err);
      websocket._socket.destroy();
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data) {
      this[kWebSocket].emit("message", data);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      websocket.pong(data, !websocket._isServer, NOOP);
      websocket.emit("ping", data);
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("end", socketOnEnd);
      websocket.readyState = WebSocket.CLOSING;
      websocket._socket.read();
      websocket._receiver.end();
      this.removeListener("data", socketOnData);
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket.readyState = WebSocket.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      websocket.readyState = WebSocket.CLOSING;
      this.destroy();
    }
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var crypto = require("crypto");
    var http2 = require("http");
    var PerMessageDeflate = require_permessage_deflate();
    var extension = require_extension();
    var WebSocket = require_websocket();
    var { GUID } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var WebSocketServer2 = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Number} options.backlog The maximum length of the queue of pending
       *     connections
       * @param {Boolean} options.clientTracking Specifies whether or not to track
       *     clients
       * @param {Function} options.handleProtocols An hook to handle protocols
       * @param {String} options.host The hostname where to bind the server
       * @param {Number} options.maxPayload The maximum allowed message size
       * @param {Boolean} options.noServer Enable no server mode
       * @param {String} options.path Accept only connections matching this path
       * @param {(Boolean|Object)} options.perMessageDeflate Enable/disable
       *     permessage-deflate
       * @param {Number} options.port The port where to bind the server
       * @param {http.Server} options.server A pre-created HTTP/S server to use
       * @param {Function} options.verifyClient An hook to reject connections
       * @param {Function} callback A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = Object.assign(
          {
            maxPayload: 100 * 1024 * 1024,
            perMessageDeflate: false,
            handleProtocols: null,
            clientTracking: true,
            verifyClient: null,
            noServer: false,
            backlog: null,
            // use default (511 as implemented in net.js)
            server: null,
            host: null,
            path: null,
            port: null
          },
          options
        );
        if (options.port == null && !options.server && !options.noServer) {
          throw new TypeError(
            'One of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http2.createServer((req, res) => {
            const body = http2.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, (ws) => {
                this.emit("connection", ws, req);
              });
            }
          });
        }
        if (options.perMessageDeflate === true) options.perMessageDeflate = {};
        if (options.clientTracking) this.clients = /* @__PURE__ */ new Set();
        this.options = options;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server) return null;
        return this._server.address();
      }
      /**
       * Close the server.
       *
       * @param {Function} cb Callback
       * @public
       */
      close(cb) {
        if (cb) this.once("close", cb);
        if (this.clients) {
          for (const client of this.clients) client.terminate();
        }
        const server = this._server;
        if (server) {
          this._removeListeners();
          this._removeListeners = this._server = null;
          if (this.options.port != null) {
            server.close(() => this.emit("close"));
            return;
          }
        }
        process.nextTick(emitClose, this);
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path) return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {net.Socket} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"] !== void 0 ? req.headers["sec-websocket-key"].trim() : false;
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        const extensions = {};
        if (req.method !== "GET" || upgrade === void 0 || upgrade.toLowerCase() !== "websocket" || !key || !keyRegex.test(key) || version !== 8 && version !== 13 || !this.shouldHandle(req)) {
          return abortHandshake(socket, 400);
        }
        if (this.options.perMessageDeflate) {
          const perMessageDeflate = new PerMessageDeflate(
            this.options.perMessageDeflate,
            true,
            this.options.maxPayload
          );
          try {
            const offers = extension.parse(req.headers["sec-websocket-extensions"]);
            if (offers[PerMessageDeflate.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate.extensionName]);
              extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            return abortHandshake(socket, 400);
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers["".concat(version === 8 ? "sec-websocket-origin" : "origin")],
            secure: !!(req.connection.authorized || req.connection.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(key, extensions, req, socket, head, cb);
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(key, extensions, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Object} extensions The accepted extensions
       * @param {http.IncomingMessage} req The request object
       * @param {net.Socket} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @private
       */
      completeUpgrade(key, extensions, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        const digest = crypto.createHash("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          "Sec-WebSocket-Accept: ".concat(digest)
        ];
        const ws = new WebSocket(null);
        var protocol = req.headers["sec-websocket-protocol"];
        if (protocol) {
          protocol = protocol.split(",").map(trim);
          if (this.options.handleProtocols) {
            protocol = this.options.handleProtocols(protocol, req);
          } else {
            protocol = protocol[0];
          }
          if (protocol) {
            headers.push("Sec-WebSocket-Protocol: ".concat(protocol));
            ws.protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate.extensionName]) {
          const params = extensions[PerMessageDeflate.extensionName].params;
          const value = extension.format({
            [PerMessageDeflate.extensionName]: [params]
          });
          headers.push("Sec-WebSocket-Extensions: ".concat(value));
          ws._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws.setSocket(socket, head, this.options.maxPayload);
        if (this.clients) {
          this.clients.add(ws);
          ws.on("close", () => this.clients.delete(ws));
        }
        cb(ws);
      }
    };
    module2.exports = WebSocketServer2;
    function addListeners(server, map) {
      for (const event of Object.keys(map)) server.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server) {
      server.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      if (socket.writable) {
        message = message || http2.STATUS_CODES[code];
        headers = Object.assign(
          {
            Connection: "close",
            "Content-type": "text/html",
            "Content-Length": Buffer.byteLength(message)
          },
          headers
        );
        socket.write(
          "HTTP/1.1 ".concat(code, " ").concat(http2.STATUS_CODES[code], "\r\n") + Object.keys(headers).map((h) => "".concat(h, ": ").concat(headers[h])).join("\r\n") + "\r\n\r\n" + message
        );
      }
      socket.removeListener("error", socketOnError);
      socket.destroy();
    }
    function trim(str) {
      return str.trim();
    }
  }
});

// node_modules/ws/index.js
var require_ws = __commonJS({
  "node_modules/ws/index.js"(exports2, module2) {
    "use strict";
    var WebSocket = require_websocket();
    WebSocket.Server = require_websocket_server();
    WebSocket.Receiver = require_receiver();
    WebSocket.Sender = require_sender();
    module2.exports = WebSocket;
  }
});

// lib/gena.js
var require_gena = __commonJS({
  "lib/gena.js"(exports2, module2) {
    "use strict";
    var http2 = require("http");
    var REQUEST_TIMEOUT_MS = 5e3;
    function httpRequest(options, body) {
      return new Promise(function(resolve, reject) {
        var req = http2.request(options, function(res) {
          var chunks = [];
          res.on("data", function(chunk) {
            chunks.push(chunk);
          });
          res.on("end", function() {
            resolve({
              statusCode: res.statusCode,
              headers: res.headers,
              body: Buffer.concat(chunks).toString()
            });
          });
        });
        req.on("error", reject);
        req.setTimeout(REQUEST_TIMEOUT_MS, function() {
          req.abort();
          reject(new Error("request timed out after " + REQUEST_TIMEOUT_MS + "ms"));
        });
        if (body) req.write(body);
        req.end();
      });
    }
    function parseTimeout(headerValue) {
      var m = (headerValue || "").match(/Second-(\d+)/i);
      return m ? parseInt(m[1], 10) : null;
    }
    function startListener2(port, onNotify) {
      var server = http2.createServer(function(req, res) {
        var recvAt = process.hrtime();
        if (req.method !== "NOTIFY") {
          res.writeHead(405);
          res.end();
          return;
        }
        var chunks = [];
        req.on("data", function(chunk) {
          chunks.push(chunk);
        });
        req.on("end", function() {
          var body = Buffer.concat(chunks).toString();
          res.writeHead(200);
          res.end();
          try {
            onNotify(req.headers, body, recvAt);
          } catch (e) {
            console.error("[gena] onNotify threw:", e.message);
          }
        });
        req.on("error", function(e) {
          console.error("[gena] req error:", e.message);
          res.writeHead(500);
          res.end();
        });
      });
      server.listen(port, function() {
        console.log("[gena] listener on :%d", port);
      });
      return server;
    }
    async function subscribe2(device, callbackUrl, eventPath, requestedSeconds) {
      var opts = {
        hostname: device.ip,
        port: device.port,
        path: eventPath,
        method: "SUBSCRIBE",
        headers: {
          "HOST": device.ip + ":" + device.port,
          "CALLBACK": "<" + callbackUrl + ">",
          "NT": "upnp:event",
          "TIMEOUT": "Second-" + requestedSeconds
        }
      };
      var res = await httpRequest(opts, null);
      if (res.statusCode !== 200) {
        throw new Error("SUBSCRIBE returned HTTP " + res.statusCode + ": " + res.body.slice(0, 200));
      }
      var sid = res.headers["sid"] || res.headers["SID"] || "";
      if (!sid) throw new Error("SUBSCRIBE response missing SID header");
      var negotiatedSeconds = parseTimeout(res.headers["timeout"]);
      if (negotiatedSeconds === null) {
        console.warn("[gena] TIMEOUT header missing in SUBSCRIBE response; assuming", requestedSeconds, "s");
        negotiatedSeconds = requestedSeconds;
      }
      return { sid, negotiatedSeconds };
    }
    async function renew2(device, sid, eventPath, requestedSeconds) {
      var opts = {
        hostname: device.ip,
        port: device.port,
        path: eventPath,
        method: "SUBSCRIBE",
        headers: {
          "HOST": device.ip + ":" + device.port,
          "SID": sid,
          "TIMEOUT": "Second-" + requestedSeconds
        }
      };
      var res = await httpRequest(opts, null);
      if (res.statusCode !== 200) {
        throw new Error("RENEW returned HTTP " + res.statusCode + ": " + res.body.slice(0, 200));
      }
      var negotiatedSeconds = parseTimeout(res.headers["timeout"]);
      if (negotiatedSeconds === null) {
        console.warn("[gena] TIMEOUT header missing in RENEW response; assuming", requestedSeconds, "s");
        negotiatedSeconds = requestedSeconds;
      }
      return { negotiatedSeconds };
    }
    async function unsubscribe2(device, sid, eventPath) {
      var opts = {
        hostname: device.ip,
        port: device.port,
        path: eventPath,
        method: "UNSUBSCRIBE",
        headers: {
          "HOST": device.ip + ":" + device.port,
          "SID": sid
        }
      };
      try {
        await httpRequest(opts, null);
      } catch (e) {
        console.warn("[gena] UNSUBSCRIBE error (non-fatal):", e.message);
      }
    }
    module2.exports = {
      startListener: startListener2,
      subscribe: subscribe2,
      renew: renew2,
      unsubscribe: unsubscribe2
    };
  }
});

// node_modules/fast-xml-parser/lib/fxp.cjs
var require_fxp = __commonJS({
  "node_modules/fast-xml-parser/lib/fxp.cjs"(exports2, module2) {
    (() => {
      "use strict";
      var t = { d: (e2, n2) => {
        for (var i2 in n2) t.o(n2, i2) && !t.o(e2, i2) && Object.defineProperty(e2, i2, { enumerable: true, get: n2[i2] });
      }, o: (t2, e2) => Object.prototype.hasOwnProperty.call(t2, e2), r: (t2) => {
        "undefined" != typeof Symbol && Symbol.toStringTag && Object.defineProperty(t2, Symbol.toStringTag, { value: "Module" }), Object.defineProperty(t2, "__esModule", { value: true });
      } }, e = {};
      t.r(e), t.d(e, { XMLBuilder: () => Bt, XMLParser: () => Tt, XMLValidator: () => Ut });
      const n = ":A-Za-z_\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u02FF\\u0370-\\u037D\\u037F-\\u1FFF\\u200C-\\u200D\\u2070-\\u218F\\u2C00-\\u2FEF\\u3001-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFFD", i = new RegExp("^[" + n + "][" + n + "\\-.\\d\\u00B7\\u0300-\\u036F\\u203F-\\u2040]*$");
      function s(t2, e2) {
        const n2 = [];
        let i2 = e2.exec(t2);
        for (; i2; ) {
          const s2 = [];
          s2.startIndex = e2.lastIndex - i2[0].length;
          const r2 = i2.length;
          for (let t3 = 0; t3 < r2; t3++) s2.push(i2[t3]);
          n2.push(s2), i2 = e2.exec(t2);
        }
        return n2;
      }
      const r = function(t2) {
        return !(null == i.exec(t2));
      }, o = ["hasOwnProperty", "toString", "valueOf", "__defineGetter__", "__defineSetter__", "__lookupGetter__", "__lookupSetter__"], a = ["__proto__", "constructor", "prototype"], h = { allowBooleanAttributes: false, unpairedTags: [] };
      function l(t2, e2) {
        e2 = Object.assign({}, h, e2);
        const n2 = [];
        let i2 = false, s2 = false;
        "\uFEFF" === t2[0] && (t2 = t2.substr(1));
        for (let r2 = 0; r2 < t2.length; r2++) if ("<" === t2[r2] && "?" === t2[r2 + 1]) {
          if (r2 += 2, r2 = p(t2, r2), r2.err) return r2;
        } else {
          if ("<" !== t2[r2]) {
            if (u(t2[r2])) continue;
            return b("InvalidChar", "char '" + t2[r2] + "' is not expected.", w(t2, r2));
          }
          {
            let o2 = r2;
            if (r2++, "!" === t2[r2]) {
              r2 = c(t2, r2);
              continue;
            }
            {
              let a2 = false;
              "/" === t2[r2] && (a2 = true, r2++);
              let h2 = "";
              for (; r2 < t2.length && ">" !== t2[r2] && " " !== t2[r2] && "	" !== t2[r2] && "\n" !== t2[r2] && "\r" !== t2[r2]; r2++) h2 += t2[r2];
              if (h2 = h2.trim(), "/" === h2[h2.length - 1] && (h2 = h2.substring(0, h2.length - 1), r2--), !E(h2)) {
                let e3;
                return e3 = 0 === h2.trim().length ? "Invalid space after '<'." : "Tag '" + h2 + "' is an invalid name.", b("InvalidTag", e3, w(t2, r2));
              }
              const l2 = g(t2, r2);
              if (false === l2) return b("InvalidAttr", "Attributes for '" + h2 + "' have open quote.", w(t2, r2));
              let d2 = l2.value;
              if (r2 = l2.index, "/" === d2[d2.length - 1]) {
                const n3 = r2 - d2.length;
                d2 = d2.substring(0, d2.length - 1);
                const s3 = x(d2, e2);
                if (true !== s3) return b(s3.err.code, s3.err.msg, w(t2, n3 + s3.err.line));
                i2 = true;
              } else if (a2) {
                if (!l2.tagClosed) return b("InvalidTag", "Closing tag '" + h2 + "' doesn't have proper closing.", w(t2, r2));
                if (d2.trim().length > 0) return b("InvalidTag", "Closing tag '" + h2 + "' can't have attributes or invalid starting.", w(t2, o2));
                if (0 === n2.length) return b("InvalidTag", "Closing tag '" + h2 + "' has not been opened.", w(t2, o2));
                {
                  const e3 = n2.pop();
                  if (h2 !== e3.tagName) {
                    let n3 = w(t2, e3.tagStartPos);
                    return b("InvalidTag", "Expected closing tag '" + e3.tagName + "' (opened in line " + n3.line + ", col " + n3.col + ") instead of closing tag '" + h2 + "'.", w(t2, o2));
                  }
                  0 == n2.length && (s2 = true);
                }
              } else {
                const a3 = x(d2, e2);
                if (true !== a3) return b(a3.err.code, a3.err.msg, w(t2, r2 - d2.length + a3.err.line));
                if (true === s2) return b("InvalidXml", "Multiple possible root nodes found.", w(t2, r2));
                -1 !== e2.unpairedTags.indexOf(h2) || n2.push({ tagName: h2, tagStartPos: o2 }), i2 = true;
              }
              for (r2++; r2 < t2.length; r2++) if ("<" === t2[r2]) {
                if ("!" === t2[r2 + 1]) {
                  r2++, r2 = c(t2, r2);
                  continue;
                }
                if ("?" !== t2[r2 + 1]) break;
                if (r2 = p(t2, ++r2), r2.err) return r2;
              } else if ("&" === t2[r2]) {
                const e3 = N(t2, r2);
                if (-1 == e3) return b("InvalidChar", "char '&' is not expected.", w(t2, r2));
                r2 = e3;
              } else if (true === s2 && !u(t2[r2])) return b("InvalidXml", "Extra text at the end", w(t2, r2));
              "<" === t2[r2] && r2--;
            }
          }
        }
        return i2 ? 1 == n2.length ? b("InvalidTag", "Unclosed tag '" + n2[0].tagName + "'.", w(t2, n2[0].tagStartPos)) : !(n2.length > 0) || b("InvalidXml", "Invalid '" + JSON.stringify(n2.map((t3) => t3.tagName), null, 4).replace(/\r?\n/g, "") + "' found.", { line: 1, col: 1 }) : b("InvalidXml", "Start tag expected.", 1);
      }
      function u(t2) {
        return " " === t2 || "	" === t2 || "\n" === t2 || "\r" === t2;
      }
      function p(t2, e2) {
        const n2 = e2;
        for (; e2 < t2.length; e2++) if ("?" == t2[e2] || " " == t2[e2]) {
          const i2 = t2.substr(n2, e2 - n2);
          if (e2 > 5 && "xml" === i2) return b("InvalidXml", "XML declaration allowed only at the start of the document.", w(t2, e2));
          if ("?" == t2[e2] && ">" == t2[e2 + 1]) {
            e2++;
            break;
          }
          continue;
        }
        return e2;
      }
      function c(t2, e2) {
        if (t2.length > e2 + 5 && "-" === t2[e2 + 1] && "-" === t2[e2 + 2]) {
          for (e2 += 3; e2 < t2.length; e2++) if ("-" === t2[e2] && "-" === t2[e2 + 1] && ">" === t2[e2 + 2]) {
            e2 += 2;
            break;
          }
        } else if (t2.length > e2 + 8 && "D" === t2[e2 + 1] && "O" === t2[e2 + 2] && "C" === t2[e2 + 3] && "T" === t2[e2 + 4] && "Y" === t2[e2 + 5] && "P" === t2[e2 + 6] && "E" === t2[e2 + 7]) {
          let n2 = 1;
          for (e2 += 8; e2 < t2.length; e2++) if ("<" === t2[e2]) n2++;
          else if (">" === t2[e2] && (n2--, 0 === n2)) break;
        } else if (t2.length > e2 + 9 && "[" === t2[e2 + 1] && "C" === t2[e2 + 2] && "D" === t2[e2 + 3] && "A" === t2[e2 + 4] && "T" === t2[e2 + 5] && "A" === t2[e2 + 6] && "[" === t2[e2 + 7]) {
          for (e2 += 8; e2 < t2.length; e2++) if ("]" === t2[e2] && "]" === t2[e2 + 1] && ">" === t2[e2 + 2]) {
            e2 += 2;
            break;
          }
        }
        return e2;
      }
      const d = '"', f = "'";
      function g(t2, e2) {
        let n2 = "", i2 = "", s2 = false;
        for (; e2 < t2.length; e2++) {
          if (t2[e2] === d || t2[e2] === f) "" === i2 ? i2 = t2[e2] : i2 !== t2[e2] || (i2 = "");
          else if (">" === t2[e2] && "" === i2) {
            s2 = true;
            break;
          }
          n2 += t2[e2];
        }
        return "" === i2 && { value: n2, index: e2, tagClosed: s2 };
      }
      const m = new RegExp("(\\s*)([^\\s=]+)(\\s*=)?(\\s*(['\"])(([\\s\\S])*?)\\5)?", "g");
      function x(t2, e2) {
        const n2 = s(t2, m), i2 = {};
        for (let t3 = 0; t3 < n2.length; t3++) {
          if (0 === n2[t3][1].length) return b("InvalidAttr", "Attribute '" + n2[t3][2] + "' has no space in starting.", v(n2[t3]));
          if (void 0 !== n2[t3][3] && void 0 === n2[t3][4]) return b("InvalidAttr", "Attribute '" + n2[t3][2] + "' is without value.", v(n2[t3]));
          if (void 0 === n2[t3][3] && !e2.allowBooleanAttributes) return b("InvalidAttr", "boolean attribute '" + n2[t3][2] + "' is not allowed.", v(n2[t3]));
          const s2 = n2[t3][2];
          if (!y(s2)) return b("InvalidAttr", "Attribute '" + s2 + "' is an invalid name.", v(n2[t3]));
          if (Object.prototype.hasOwnProperty.call(i2, s2)) return b("InvalidAttr", "Attribute '" + s2 + "' is repeated.", v(n2[t3]));
          i2[s2] = 1;
        }
        return true;
      }
      function N(t2, e2) {
        if (";" === t2[++e2]) return -1;
        if ("#" === t2[e2]) return (function(t3, e3) {
          let n3 = /\d/;
          for ("x" === t3[e3] && (e3++, n3 = /[\da-fA-F]/); e3 < t3.length; e3++) {
            if (";" === t3[e3]) return e3;
            if (!t3[e3].match(n3)) break;
          }
          return -1;
        })(t2, ++e2);
        let n2 = 0;
        for (; e2 < t2.length; e2++, n2++) if (!(t2[e2].match(/\w/) && n2 < 20)) {
          if (";" === t2[e2]) break;
          return -1;
        }
        return e2;
      }
      function b(t2, e2, n2) {
        return { err: { code: t2, msg: e2, line: n2.line || n2, col: n2.col } };
      }
      function y(t2) {
        return r(t2);
      }
      function E(t2) {
        return r(t2);
      }
      function w(t2, e2) {
        const n2 = t2.substring(0, e2).split(/\r?\n/);
        return { line: n2.length, col: n2[n2.length - 1].length + 1 };
      }
      function v(t2) {
        return t2.startIndex + t2[1].length;
      }
      const S = (t2) => o.includes(t2) ? "__" + t2 : t2, _ = { preserveOrder: false, attributeNamePrefix: "@_", attributesGroupName: false, textNodeName: "#text", ignoreAttributes: true, removeNSPrefix: false, allowBooleanAttributes: false, parseTagValue: true, parseAttributeValue: false, trimValues: true, cdataPropName: false, numberParseOptions: { hex: true, leadingZeros: true, eNotation: true }, tagValueProcessor: function(t2, e2) {
        return e2;
      }, attributeValueProcessor: function(t2, e2) {
        return e2;
      }, stopNodes: [], alwaysCreateTextNode: false, isArray: () => false, commentPropName: false, unpairedTags: [], processEntities: true, htmlEntities: false, entityDecoder: null, ignoreDeclaration: false, ignorePiTags: false, transformTagName: false, transformAttributeName: false, updateTag: function(t2, e2, n2) {
        return t2;
      }, captureMetaData: false, maxNestedTags: 100, strictReservedNames: true, jPath: true, onDangerousProperty: S };
      function A(t2, e2) {
        if ("string" != typeof t2) return;
        const n2 = t2.toLowerCase();
        if (o.some((t3) => n2 === t3.toLowerCase())) throw new Error("[SECURITY] Invalid ".concat(e2, ': "').concat(t2, '" is a reserved JavaScript keyword that could cause prototype pollution'));
        if (a.some((t3) => n2 === t3.toLowerCase())) throw new Error("[SECURITY] Invalid ".concat(e2, ': "').concat(t2, '" is a reserved JavaScript keyword that could cause prototype pollution'));
      }
      function T(t2, e2) {
        var _a, _b, _c, _d, _e, _f, _g, _h;
        return "boolean" == typeof t2 ? { enabled: t2, maxEntitySize: 1e4, maxExpansionDepth: 1e4, maxTotalExpansions: 1 / 0, maxExpandedLength: 1e5, maxEntityCount: 1e3, allowedTags: null, tagFilter: null, appliesTo: "all" } : "object" == typeof t2 && null !== t2 ? { enabled: false !== t2.enabled, maxEntitySize: Math.max(1, (_a = t2.maxEntitySize) != null ? _a : 1e4), maxExpansionDepth: Math.max(1, (_b = t2.maxExpansionDepth) != null ? _b : 1e4), maxTotalExpansions: Math.max(1, (_c = t2.maxTotalExpansions) != null ? _c : 1 / 0), maxExpandedLength: Math.max(1, (_d = t2.maxExpandedLength) != null ? _d : 1e5), maxEntityCount: Math.max(1, (_e = t2.maxEntityCount) != null ? _e : 1e3), allowedTags: (_f = t2.allowedTags) != null ? _f : null, tagFilter: (_g = t2.tagFilter) != null ? _g : null, appliesTo: (_h = t2.appliesTo) != null ? _h : "all" } : T(true);
      }
      const C = function(t2) {
        const e2 = Object.assign({}, _, t2), n2 = [{ value: e2.attributeNamePrefix, name: "attributeNamePrefix" }, { value: e2.attributesGroupName, name: "attributesGroupName" }, { value: e2.textNodeName, name: "textNodeName" }, { value: e2.cdataPropName, name: "cdataPropName" }, { value: e2.commentPropName, name: "commentPropName" }];
        for (const { value: t3, name: e3 } of n2) t3 && A(t3, e3);
        return null === e2.onDangerousProperty && (e2.onDangerousProperty = S), e2.processEntities = T(e2.processEntities, e2.htmlEntities), e2.unpairedTagsSet = new Set(e2.unpairedTags), e2.stopNodes && Array.isArray(e2.stopNodes) && (e2.stopNodes = e2.stopNodes.map((t3) => "string" == typeof t3 && t3.startsWith("*.") ? ".." + t3.substring(2) : t3)), e2;
      };
      let P;
      P = "function" != typeof Symbol ? "@@xmlMetadata" : Symbol("XML Node Metadata");
      class O {
        constructor(t2) {
          this.tagname = t2, this.child = [], this[":@"] = /* @__PURE__ */ Object.create(null);
        }
        add(t2, e2) {
          "__proto__" === t2 && (t2 = "#__proto__"), this.child.push({ [t2]: e2 });
        }
        addChild(t2, e2) {
          "__proto__" === t2.tagname && (t2.tagname = "#__proto__"), t2[":@"] && Object.keys(t2[":@"]).length > 0 ? this.child.push({ [t2.tagname]: t2.child, ":@": t2[":@"] }) : this.child.push({ [t2.tagname]: t2.child }), void 0 !== e2 && (this.child[this.child.length - 1][P] = { startIndex: e2 });
        }
        static getMetaDataSymbol() {
          return P;
        }
      }
      class $ {
        constructor(t2) {
          this.suppressValidationErr = !t2, this.options = t2;
        }
        readDocType(t2, e2) {
          const n2 = /* @__PURE__ */ Object.create(null);
          let i2 = 0;
          if ("O" !== t2[e2 + 3] || "C" !== t2[e2 + 4] || "T" !== t2[e2 + 5] || "Y" !== t2[e2 + 6] || "P" !== t2[e2 + 7] || "E" !== t2[e2 + 8]) throw new Error("Invalid Tag instead of DOCTYPE");
          {
            e2 += 9;
            let s2 = 1, r2 = false, o2 = false, a2 = "";
            for (; e2 < t2.length; e2++) if ("<" !== t2[e2] || o2) if (">" === t2[e2]) {
              if (o2 ? "-" === t2[e2 - 1] && "-" === t2[e2 - 2] && (o2 = false, s2--) : s2--, 0 === s2) break;
            } else "[" === t2[e2] ? r2 = true : a2 += t2[e2];
            else {
              if (r2 && D(t2, "!ENTITY", e2)) {
                let s3, r3;
                if (e2 += 7, [s3, r3, e2] = this.readEntityExp(t2, e2 + 1, this.suppressValidationErr), -1 === r3.indexOf("&")) {
                  if (false !== this.options.enabled && null != this.options.maxEntityCount && i2 >= this.options.maxEntityCount) throw new Error("Entity count (".concat(i2 + 1, ") exceeds maximum allowed (").concat(this.options.maxEntityCount, ")"));
                  n2[s3] = r3, i2++;
                }
              } else if (r2 && D(t2, "!ELEMENT", e2)) {
                e2 += 8;
                const { index: n3 } = this.readElementExp(t2, e2 + 1);
                e2 = n3;
              } else if (r2 && D(t2, "!ATTLIST", e2)) e2 += 8;
              else if (r2 && D(t2, "!NOTATION", e2)) {
                e2 += 9;
                const { index: n3 } = this.readNotationExp(t2, e2 + 1, this.suppressValidationErr);
                e2 = n3;
              } else {
                if (!D(t2, "!--", e2)) throw new Error("Invalid DOCTYPE");
                o2 = true;
              }
              s2++, a2 = "";
            }
            if (0 !== s2) throw new Error("Unclosed DOCTYPE");
          }
          return { entities: n2, i: e2 };
        }
        readEntityExp(t2, e2) {
          const n2 = e2 = I(t2, e2);
          for (; e2 < t2.length && !/\s/.test(t2[e2]) && '"' !== t2[e2] && "'" !== t2[e2]; ) e2++;
          let i2 = t2.substring(n2, e2);
          if (M(i2), e2 = I(t2, e2), !this.suppressValidationErr) {
            if ("SYSTEM" === t2.substring(e2, e2 + 6).toUpperCase()) throw new Error("External entities are not supported");
            if ("%" === t2[e2]) throw new Error("Parameter entities are not supported");
          }
          let s2 = "";
          if ([e2, s2] = this.readIdentifierVal(t2, e2, "entity"), false !== this.options.enabled && null != this.options.maxEntitySize && s2.length > this.options.maxEntitySize) throw new Error('Entity "'.concat(i2, '" size (').concat(s2.length, ") exceeds maximum allowed size (").concat(this.options.maxEntitySize, ")"));
          return [i2, s2, --e2];
        }
        readNotationExp(t2, e2) {
          const n2 = e2 = I(t2, e2);
          for (; e2 < t2.length && !/\s/.test(t2[e2]); ) e2++;
          let i2 = t2.substring(n2, e2);
          !this.suppressValidationErr && M(i2), e2 = I(t2, e2);
          const s2 = t2.substring(e2, e2 + 6).toUpperCase();
          if (!this.suppressValidationErr && "SYSTEM" !== s2 && "PUBLIC" !== s2) throw new Error('Expected SYSTEM or PUBLIC, found "'.concat(s2, '"'));
          e2 += s2.length, e2 = I(t2, e2);
          let r2 = null, o2 = null;
          if ("PUBLIC" === s2) [e2, r2] = this.readIdentifierVal(t2, e2, "publicIdentifier"), '"' !== t2[e2 = I(t2, e2)] && "'" !== t2[e2] || ([e2, o2] = this.readIdentifierVal(t2, e2, "systemIdentifier"));
          else if ("SYSTEM" === s2 && ([e2, o2] = this.readIdentifierVal(t2, e2, "systemIdentifier"), !this.suppressValidationErr && !o2)) throw new Error("Missing mandatory system identifier for SYSTEM notation");
          return { notationName: i2, publicIdentifier: r2, systemIdentifier: o2, index: --e2 };
        }
        readIdentifierVal(t2, e2, n2) {
          let i2 = "";
          const s2 = t2[e2];
          if ('"' !== s2 && "'" !== s2) throw new Error('Expected quoted string, found "'.concat(s2, '"'));
          const r2 = ++e2;
          for (; e2 < t2.length && t2[e2] !== s2; ) e2++;
          if (i2 = t2.substring(r2, e2), t2[e2] !== s2) throw new Error("Unterminated ".concat(n2, " value"));
          return [++e2, i2];
        }
        readElementExp(t2, e2) {
          const n2 = e2 = I(t2, e2);
          for (; e2 < t2.length && !/\s/.test(t2[e2]); ) e2++;
          let i2 = t2.substring(n2, e2);
          if (!this.suppressValidationErr && !r(i2)) throw new Error('Invalid element name: "'.concat(i2, '"'));
          let s2 = "";
          if ("E" === t2[e2 = I(t2, e2)] && D(t2, "MPTY", e2)) e2 += 4;
          else if ("A" === t2[e2] && D(t2, "NY", e2)) e2 += 2;
          else if ("(" === t2[e2]) {
            const n3 = ++e2;
            for (; e2 < t2.length && ")" !== t2[e2]; ) e2++;
            if (s2 = t2.substring(n3, e2), ")" !== t2[e2]) throw new Error("Unterminated content model");
          } else if (!this.suppressValidationErr) throw new Error('Invalid Element Expression, found "'.concat(t2[e2], '"'));
          return { elementName: i2, contentModel: s2.trim(), index: e2 };
        }
        readAttlistExp(t2, e2) {
          let n2 = e2 = I(t2, e2);
          for (; e2 < t2.length && !/\s/.test(t2[e2]); ) e2++;
          let i2 = t2.substring(n2, e2);
          for (M(i2), n2 = e2 = I(t2, e2); e2 < t2.length && !/\s/.test(t2[e2]); ) e2++;
          let s2 = t2.substring(n2, e2);
          if (!M(s2)) throw new Error('Invalid attribute name: "'.concat(s2, '"'));
          e2 = I(t2, e2);
          let r2 = "";
          if ("NOTATION" === t2.substring(e2, e2 + 8).toUpperCase()) {
            if (r2 = "NOTATION", "(" !== t2[e2 = I(t2, e2 += 8)]) throw new Error("Expected '(', found \"".concat(t2[e2], '"'));
            e2++;
            let n3 = [];
            for (; e2 < t2.length && ")" !== t2[e2]; ) {
              const i3 = e2;
              for (; e2 < t2.length && "|" !== t2[e2] && ")" !== t2[e2]; ) e2++;
              let s3 = t2.substring(i3, e2);
              if (s3 = s3.trim(), !M(s3)) throw new Error('Invalid notation name: "'.concat(s3, '"'));
              n3.push(s3), "|" === t2[e2] && (e2++, e2 = I(t2, e2));
            }
            if (")" !== t2[e2]) throw new Error("Unterminated list of notations");
            e2++, r2 += " (" + n3.join("|") + ")";
          } else {
            const n3 = e2;
            for (; e2 < t2.length && !/\s/.test(t2[e2]); ) e2++;
            r2 += t2.substring(n3, e2);
            const i3 = ["CDATA", "ID", "IDREF", "IDREFS", "ENTITY", "ENTITIES", "NMTOKEN", "NMTOKENS"];
            if (!this.suppressValidationErr && !i3.includes(r2.toUpperCase())) throw new Error('Invalid attribute type: "'.concat(r2, '"'));
          }
          e2 = I(t2, e2);
          let o2 = "";
          return "#REQUIRED" === t2.substring(e2, e2 + 8).toUpperCase() ? (o2 = "#REQUIRED", e2 += 8) : "#IMPLIED" === t2.substring(e2, e2 + 7).toUpperCase() ? (o2 = "#IMPLIED", e2 += 7) : [e2, o2] = this.readIdentifierVal(t2, e2, "ATTLIST"), { elementName: i2, attributeName: s2, attributeType: r2, defaultValue: o2, index: e2 };
        }
      }
      const I = (t2, e2) => {
        for (; e2 < t2.length && /\s/.test(t2[e2]); ) e2++;
        return e2;
      };
      function D(t2, e2, n2) {
        for (let i2 = 0; i2 < e2.length; i2++) if (e2[i2] !== t2[n2 + i2 + 1]) return false;
        return true;
      }
      function M(t2) {
        if (r(t2)) return t2;
        throw new Error("Invalid entity name ".concat(t2));
      }
      const j = /^[-+]?0x[a-fA-F0-9]+$/, V = /^([\-\+])?(0*)([0-9]*(\.[0-9]*)?)$/, L = { hex: true, leadingZeros: true, decimalPoint: ".", eNotation: true, infinity: "original" };
      const k = /^([-+])?(0*)(\d*(\.\d*)?[eE][-\+]?\d+)$/;
      class F {
        constructor(t2) {
          this._matcher = t2;
        }
        get separator() {
          return this._matcher.separator;
        }
        getCurrentTag() {
          const t2 = this._matcher.path;
          return t2.length > 0 ? t2[t2.length - 1].tag : void 0;
        }
        getCurrentNamespace() {
          const t2 = this._matcher.path;
          return t2.length > 0 ? t2[t2.length - 1].namespace : void 0;
        }
        getAttrValue(t2) {
          var _a;
          const e2 = this._matcher.path;
          if (0 !== e2.length) return (_a = e2[e2.length - 1].values) == null ? void 0 : _a[t2];
        }
        hasAttr(t2) {
          const e2 = this._matcher.path;
          if (0 === e2.length) return false;
          const n2 = e2[e2.length - 1];
          return void 0 !== n2.values && t2 in n2.values;
        }
        getPosition() {
          var _a;
          const t2 = this._matcher.path;
          return 0 === t2.length ? -1 : (_a = t2[t2.length - 1].position) != null ? _a : 0;
        }
        getCounter() {
          var _a;
          const t2 = this._matcher.path;
          return 0 === t2.length ? -1 : (_a = t2[t2.length - 1].counter) != null ? _a : 0;
        }
        getIndex() {
          return this.getPosition();
        }
        getDepth() {
          return this._matcher.path.length;
        }
        toString(t2, e2 = true) {
          return this._matcher.toString(t2, e2);
        }
        toArray() {
          return this._matcher.path.map((t2) => t2.tag);
        }
        matches(t2) {
          return this._matcher.matches(t2);
        }
        matchesAny(t2) {
          return t2.matchesAny(this._matcher);
        }
      }
      class R {
        constructor(t2 = {}) {
          this.separator = t2.separator || ".", this.path = [], this.siblingStacks = [], this._pathStringCache = null, this._view = new F(this);
        }
        push(t2, e2 = null, n2 = null) {
          this._pathStringCache = null, this.path.length > 0 && (this.path[this.path.length - 1].values = void 0);
          const i2 = this.path.length;
          this.siblingStacks[i2] || (this.siblingStacks[i2] = /* @__PURE__ */ new Map());
          const s2 = this.siblingStacks[i2], r2 = n2 ? "".concat(n2, ":").concat(t2) : t2, o2 = s2.get(r2) || 0;
          let a2 = 0;
          for (const t3 of s2.values()) a2 += t3;
          s2.set(r2, o2 + 1);
          const h2 = { tag: t2, position: a2, counter: o2 };
          null != n2 && (h2.namespace = n2), null != e2 && (h2.values = e2), this.path.push(h2);
        }
        pop() {
          if (0 === this.path.length) return;
          this._pathStringCache = null;
          const t2 = this.path.pop();
          return this.siblingStacks.length > this.path.length + 1 && (this.siblingStacks.length = this.path.length + 1), t2;
        }
        updateCurrent(t2) {
          if (this.path.length > 0) {
            const e2 = this.path[this.path.length - 1];
            null != t2 && (e2.values = t2);
          }
        }
        getCurrentTag() {
          return this.path.length > 0 ? this.path[this.path.length - 1].tag : void 0;
        }
        getCurrentNamespace() {
          return this.path.length > 0 ? this.path[this.path.length - 1].namespace : void 0;
        }
        getAttrValue(t2) {
          var _a;
          if (0 !== this.path.length) return (_a = this.path[this.path.length - 1].values) == null ? void 0 : _a[t2];
        }
        hasAttr(t2) {
          if (0 === this.path.length) return false;
          const e2 = this.path[this.path.length - 1];
          return void 0 !== e2.values && t2 in e2.values;
        }
        getPosition() {
          var _a;
          return 0 === this.path.length ? -1 : (_a = this.path[this.path.length - 1].position) != null ? _a : 0;
        }
        getCounter() {
          var _a;
          return 0 === this.path.length ? -1 : (_a = this.path[this.path.length - 1].counter) != null ? _a : 0;
        }
        getIndex() {
          return this.getPosition();
        }
        getDepth() {
          return this.path.length;
        }
        toString(t2, e2 = true) {
          const n2 = t2 || this.separator;
          if (n2 === this.separator && true === e2) {
            if (null !== this._pathStringCache) return this._pathStringCache;
            const t3 = this.path.map((t4) => t4.namespace ? "".concat(t4.namespace, ":").concat(t4.tag) : t4.tag).join(n2);
            return this._pathStringCache = t3, t3;
          }
          return this.path.map((t3) => e2 && t3.namespace ? "".concat(t3.namespace, ":").concat(t3.tag) : t3.tag).join(n2);
        }
        toArray() {
          return this.path.map((t2) => t2.tag);
        }
        reset() {
          this._pathStringCache = null, this.path = [], this.siblingStacks = [];
        }
        matches(t2) {
          const e2 = t2.segments;
          return 0 !== e2.length && (t2.hasDeepWildcard() ? this._matchWithDeepWildcard(e2) : this._matchSimple(e2));
        }
        _matchSimple(t2) {
          if (this.path.length !== t2.length) return false;
          for (let e2 = 0; e2 < t2.length; e2++) if (!this._matchSegment(t2[e2], this.path[e2], e2 === this.path.length - 1)) return false;
          return true;
        }
        _matchWithDeepWildcard(t2) {
          let e2 = this.path.length - 1, n2 = t2.length - 1;
          for (; n2 >= 0 && e2 >= 0; ) {
            const i2 = t2[n2];
            if ("deep-wildcard" === i2.type) {
              if (n2--, n2 < 0) return true;
              const i3 = t2[n2];
              let s2 = false;
              for (let t3 = e2; t3 >= 0; t3--) if (this._matchSegment(i3, this.path[t3], t3 === this.path.length - 1)) {
                e2 = t3 - 1, n2--, s2 = true;
                break;
              }
              if (!s2) return false;
            } else {
              if (!this._matchSegment(i2, this.path[e2], e2 === this.path.length - 1)) return false;
              e2--, n2--;
            }
          }
          return n2 < 0;
        }
        _matchSegment(t2, e2, n2) {
          var _a;
          if ("*" !== t2.tag && t2.tag !== e2.tag) return false;
          if (void 0 !== t2.namespace && "*" !== t2.namespace && t2.namespace !== e2.namespace) return false;
          if (void 0 !== t2.attrName) {
            if (!n2) return false;
            if (!e2.values || !(t2.attrName in e2.values)) return false;
            if (void 0 !== t2.attrValue && String(e2.values[t2.attrName]) !== String(t2.attrValue)) return false;
          }
          if (void 0 !== t2.position) {
            if (!n2) return false;
            const i2 = (_a = e2.counter) != null ? _a : 0;
            if ("first" === t2.position && 0 !== i2) return false;
            if ("odd" === t2.position && i2 % 2 != 1) return false;
            if ("even" === t2.position && i2 % 2 != 0) return false;
            if ("nth" === t2.position && i2 !== t2.positionValue) return false;
          }
          return true;
        }
        matchesAny(t2) {
          return t2.matchesAny(this);
        }
        snapshot() {
          return { path: this.path.map((t2) => __spreadValues({}, t2)), siblingStacks: this.siblingStacks.map((t2) => new Map(t2)) };
        }
        restore(t2) {
          this._pathStringCache = null, this.path = t2.path.map((t3) => __spreadValues({}, t3)), this.siblingStacks = t2.siblingStacks.map((t3) => new Map(t3));
        }
        readOnly() {
          return this._view;
        }
      }
      class G {
        constructor(t2, e2 = {}, n2) {
          this.pattern = t2, this.separator = e2.separator || ".", this.segments = this._parse(t2), this.data = n2, this._hasDeepWildcard = this.segments.some((t3) => "deep-wildcard" === t3.type), this._hasAttributeCondition = this.segments.some((t3) => void 0 !== t3.attrName), this._hasPositionSelector = this.segments.some((t3) => void 0 !== t3.position);
        }
        _parse(t2) {
          const e2 = [];
          let n2 = 0, i2 = "";
          for (; n2 < t2.length; ) t2[n2] === this.separator ? n2 + 1 < t2.length && t2[n2 + 1] === this.separator ? (i2.trim() && (e2.push(this._parseSegment(i2.trim())), i2 = ""), e2.push({ type: "deep-wildcard" }), n2 += 2) : (i2.trim() && e2.push(this._parseSegment(i2.trim())), i2 = "", n2++) : (i2 += t2[n2], n2++);
          return i2.trim() && e2.push(this._parseSegment(i2.trim())), e2;
        }
        _parseSegment(t2) {
          const e2 = { type: "tag" };
          let n2 = null, i2 = t2;
          const s2 = t2.match(/^([^\[]+)(\[[^\]]*\])(.*)$/);
          if (s2 && (i2 = s2[1] + s2[3], s2[2])) {
            const t3 = s2[2].slice(1, -1);
            t3 && (n2 = t3);
          }
          let r2, o2, a2 = i2;
          if (i2.includes("::")) {
            const e3 = i2.indexOf("::");
            if (r2 = i2.substring(0, e3).trim(), a2 = i2.substring(e3 + 2).trim(), !r2) throw new Error("Invalid namespace in pattern: ".concat(t2));
          }
          let h2 = null;
          if (a2.includes(":")) {
            const t3 = a2.lastIndexOf(":"), e3 = a2.substring(0, t3).trim(), n3 = a2.substring(t3 + 1).trim();
            ["first", "last", "odd", "even"].includes(n3) || /^nth\(\d+\)$/.test(n3) ? (o2 = e3, h2 = n3) : o2 = a2;
          } else o2 = a2;
          if (!o2) throw new Error("Invalid segment pattern: ".concat(t2));
          if (e2.tag = o2, r2 && (e2.namespace = r2), n2) if (n2.includes("=")) {
            const t3 = n2.indexOf("=");
            e2.attrName = n2.substring(0, t3).trim(), e2.attrValue = n2.substring(t3 + 1).trim();
          } else e2.attrName = n2.trim();
          if (h2) {
            const t3 = h2.match(/^nth\((\d+)\)$/);
            t3 ? (e2.position = "nth", e2.positionValue = parseInt(t3[1], 10)) : e2.position = h2;
          }
          return e2;
        }
        get length() {
          return this.segments.length;
        }
        hasDeepWildcard() {
          return this._hasDeepWildcard;
        }
        hasAttributeCondition() {
          return this._hasAttributeCondition;
        }
        hasPositionSelector() {
          return this._hasPositionSelector;
        }
        toString() {
          return this.pattern;
        }
      }
      class B {
        constructor() {
          this._byDepthAndTag = /* @__PURE__ */ new Map(), this._wildcardByDepth = /* @__PURE__ */ new Map(), this._deepWildcards = [], this._patterns = /* @__PURE__ */ new Set(), this._sealed = false;
        }
        add(t2) {
          if (this._sealed) throw new TypeError("ExpressionSet is sealed. Create a new ExpressionSet to add more expressions.");
          if (this._patterns.has(t2.pattern)) return this;
          if (this._patterns.add(t2.pattern), t2.hasDeepWildcard()) return this._deepWildcards.push(t2), this;
          const e2 = t2.length, n2 = t2.segments[t2.segments.length - 1], i2 = n2 == null ? void 0 : n2.tag;
          if (i2 && "*" !== i2) {
            const n3 = "".concat(e2, ":").concat(i2);
            this._byDepthAndTag.has(n3) || this._byDepthAndTag.set(n3, []), this._byDepthAndTag.get(n3).push(t2);
          } else this._wildcardByDepth.has(e2) || this._wildcardByDepth.set(e2, []), this._wildcardByDepth.get(e2).push(t2);
          return this;
        }
        addAll(t2) {
          for (const e2 of t2) this.add(e2);
          return this;
        }
        has(t2) {
          return this._patterns.has(t2.pattern);
        }
        get size() {
          return this._patterns.size;
        }
        seal() {
          return this._sealed = true, this;
        }
        get isSealed() {
          return this._sealed;
        }
        matchesAny(t2) {
          return null !== this.findMatch(t2);
        }
        findMatch(t2) {
          const e2 = t2.getDepth(), n2 = "".concat(e2, ":").concat(t2.getCurrentTag()), i2 = this._byDepthAndTag.get(n2);
          if (i2) {
            for (let e3 = 0; e3 < i2.length; e3++) if (t2.matches(i2[e3])) return i2[e3];
          }
          const s2 = this._wildcardByDepth.get(e2);
          if (s2) {
            for (let e3 = 0; e3 < s2.length; e3++) if (t2.matches(s2[e3])) return s2[e3];
          }
          for (let e3 = 0; e3 < this._deepWildcards.length; e3++) if (t2.matches(this._deepWildcards[e3])) return this._deepWildcards[e3];
          return null;
        }
      }
      const U = { cent: "\xA2", pound: "\xA3", curren: "\xA4", yen: "\xA5", euro: "\u20AC", dollar: "$", euro: "\u20AC", fnof: "\u0192", inr: "\u20B9", af: "\u060B", birr: "\u1265\u122D", peso: "\u20B1", rub: "\u20BD", won: "\u20A9", yuan: "\xA5", cedil: "\xB8" }, W = { amp: "&", apos: "'", gt: ">", lt: "<", quot: '"' }, X = { nbsp: "\xA0", copy: "\xA9", reg: "\xAE", trade: "\u2122", mdash: "\u2014", ndash: "\u2013", hellip: "\u2026", laquo: "\xAB", raquo: "\xBB", lsquo: "\u2018", rsquo: "\u2019", ldquo: "\u201C", rdquo: "\u201D", bull: "\u2022", para: "\xB6", sect: "\xA7", deg: "\xB0", frac12: "\xBD", frac14: "\xBC", frac34: "\xBE" }, Y = new Set("!?\\\\/[]$%{}^&*()<>|+");
      function z(t2) {
        if ("#" === t2[0]) throw new Error("[EntityReplacer] Invalid character '#' in entity name: \"".concat(t2, '"'));
        for (const e2 of t2) if (Y.has(e2)) throw new Error("[EntityReplacer] Invalid character '".concat(e2, "' in entity name: \"").concat(t2, '"'));
        return t2;
      }
      function q(...t2) {
        const e2 = /* @__PURE__ */ Object.create(null);
        for (const n2 of t2) if (n2) for (const t3 of Object.keys(n2)) {
          const i2 = n2[t3];
          if ("string" == typeof i2) e2[t3] = i2;
          else if (i2 && "object" == typeof i2 && void 0 !== i2.val) {
            const n3 = i2.val;
            "string" == typeof n3 && (e2[t3] = n3);
          }
        }
        return e2;
      }
      const Z = "external", J = "base", K = "all", Q = Object.freeze({ allow: 0, leave: 1, remove: 2, throw: 3 }), H = /* @__PURE__ */ new Set([9, 10, 13]);
      class tt {
        constructor(t2 = {}) {
          var _a, _b;
          var e2;
          this._limit = t2.limit || {}, this._maxTotalExpansions = this._limit.maxTotalExpansions || 0, this._maxExpandedLength = this._limit.maxExpandedLength || 0, this._postCheck = "function" == typeof t2.postCheck ? t2.postCheck : (t3) => t3, this._limitTiers = (e2 = (_a = this._limit.applyLimitsTo) != null ? _a : Z) && e2 !== Z ? e2 === K ? /* @__PURE__ */ new Set([K]) : e2 === J ? /* @__PURE__ */ new Set([J]) : Array.isArray(e2) ? new Set(e2) : /* @__PURE__ */ new Set([Z]) : /* @__PURE__ */ new Set([Z]), this._numericAllowed = (_b = t2.numericAllowed) != null ? _b : true, this._baseMap = q(W, t2.namedEntities || null), this._externalMap = /* @__PURE__ */ Object.create(null), this._inputMap = /* @__PURE__ */ Object.create(null), this._totalExpansions = 0, this._expandedLength = 0, this._removeSet = new Set(t2.remove && Array.isArray(t2.remove) ? t2.remove : []), this._leaveSet = new Set(t2.leave && Array.isArray(t2.leave) ? t2.leave : []);
          const n2 = (function(t3) {
            var _a2, _b2;
            if (!t3) return { xmlVersion: 1, onLevel: Q.allow, nullLevel: Q.remove };
            const e3 = 1.1 === t3.xmlVersion ? 1.1 : 1, n3 = (_a2 = Q[t3.onNCR]) != null ? _a2 : Q.allow, i2 = (_b2 = Q[t3.nullNCR]) != null ? _b2 : Q.remove;
            return { xmlVersion: e3, onLevel: n3, nullLevel: Math.max(i2, Q.remove) };
          })(t2.ncr);
          this._ncrXmlVersion = n2.xmlVersion, this._ncrOnLevel = n2.onLevel, this._ncrNullLevel = n2.nullLevel;
        }
        setExternalEntities(t2) {
          if (t2) for (const e2 of Object.keys(t2)) z(e2);
          this._externalMap = q(t2);
        }
        addExternalEntity(t2, e2) {
          z(t2), "string" == typeof e2 && -1 === e2.indexOf("&") && (this._externalMap[t2] = e2);
        }
        addInputEntities(t2) {
          this._totalExpansions = 0, this._expandedLength = 0, this._inputMap = q(t2);
        }
        reset() {
          return this._inputMap = /* @__PURE__ */ Object.create(null), this._totalExpansions = 0, this._expandedLength = 0, this;
        }
        setXmlVersion(t2) {
          this._ncrXmlVersion = 1.1 === t2 ? 1.1 : 1;
        }
        decode(t2) {
          if ("string" != typeof t2 || 0 === t2.length) return t2;
          const e2 = t2, n2 = [], i2 = t2.length;
          let s2 = 0, r2 = 0;
          const o2 = this._maxTotalExpansions > 0, a2 = this._maxExpandedLength > 0, h2 = o2 || a2;
          for (; r2 < i2; ) {
            if (38 !== t2.charCodeAt(r2)) {
              r2++;
              continue;
            }
            let e3 = r2 + 1;
            for (; e3 < i2 && 59 !== t2.charCodeAt(e3) && e3 - r2 <= 32; ) e3++;
            if (e3 >= i2 || 59 !== t2.charCodeAt(e3)) {
              r2++;
              continue;
            }
            const l3 = t2.slice(r2 + 1, e3);
            if (0 === l3.length) {
              r2++;
              continue;
            }
            let u2, p2;
            if (this._removeSet.has(l3)) u2 = "", void 0 === p2 && (p2 = Z);
            else {
              if (this._leaveSet.has(l3)) {
                r2++;
                continue;
              }
              if (35 === l3.charCodeAt(0)) {
                const t3 = this._resolveNCR(l3);
                if (void 0 === t3) {
                  r2++;
                  continue;
                }
                u2 = t3, p2 = J;
              } else {
                const t3 = this._resolveName(l3);
                u2 = t3 == null ? void 0 : t3.value, p2 = t3 == null ? void 0 : t3.tier;
              }
            }
            if (void 0 !== u2) {
              if (r2 > s2 && n2.push(t2.slice(s2, r2)), n2.push(u2), s2 = e3 + 1, r2 = s2, h2 && this._tierCounts(p2)) {
                if (o2 && (this._totalExpansions++, this._totalExpansions > this._maxTotalExpansions)) throw new Error("[EntityReplacer] Entity expansion count limit exceeded: ".concat(this._totalExpansions, " > ").concat(this._maxTotalExpansions));
                if (a2) {
                  const t3 = u2.length - (l3.length + 2);
                  if (t3 > 0 && (this._expandedLength += t3, this._expandedLength > this._maxExpandedLength)) throw new Error("[EntityReplacer] Expanded content length limit exceeded: ".concat(this._expandedLength, " > ").concat(this._maxExpandedLength));
                }
              }
            } else r2++;
          }
          s2 < i2 && n2.push(t2.slice(s2));
          const l2 = 0 === n2.length ? t2 : n2.join("");
          return this._postCheck(l2, e2);
        }
        _tierCounts(t2) {
          return !!this._limitTiers.has(K) || this._limitTiers.has(t2);
        }
        _resolveName(t2) {
          return t2 in this._inputMap ? { value: this._inputMap[t2], tier: Z } : t2 in this._externalMap ? { value: this._externalMap[t2], tier: Z } : t2 in this._baseMap ? { value: this._baseMap[t2], tier: J } : void 0;
        }
        _classifyNCR(t2) {
          return 0 === t2 ? this._ncrNullLevel : t2 >= 55296 && t2 <= 57343 || 1 === this._ncrXmlVersion && t2 >= 1 && t2 <= 31 && !H.has(t2) ? Q.remove : -1;
        }
        _applyNCRAction(t2, e2, n2) {
          switch (t2) {
            case Q.allow:
              return String.fromCodePoint(n2);
            case Q.remove:
              return "";
            case Q.leave:
              return;
            case Q.throw:
              throw new Error("[EntityDecoder] Prohibited numeric character reference &".concat(e2, "; (U+").concat(n2.toString(16).toUpperCase().padStart(4, "0"), ")"));
            default:
              return String.fromCodePoint(n2);
          }
        }
        _resolveNCR(t2) {
          const e2 = t2.charCodeAt(1);
          let n2;
          if (n2 = 120 === e2 || 88 === e2 ? parseInt(t2.slice(2), 16) : parseInt(t2.slice(1), 10), Number.isNaN(n2) || n2 < 0 || n2 > 1114111) return;
          const i2 = this._classifyNCR(n2);
          if (!this._numericAllowed && i2 < Q.remove) return;
          const s2 = -1 === i2 ? this._ncrOnLevel : Math.max(this._ncrOnLevel, i2);
          return this._applyNCRAction(s2, t2, n2);
        }
      }
      function et(t2, e2) {
        if (!t2) return {};
        const n2 = e2.attributesGroupName ? t2[e2.attributesGroupName] : t2;
        if (!n2) return {};
        const i2 = {};
        for (const t3 in n2) t3.startsWith(e2.attributeNamePrefix) ? i2[t3.substring(e2.attributeNamePrefix.length)] = n2[t3] : i2[t3] = n2[t3];
        return i2;
      }
      function nt(t2) {
        if (!t2 || "string" != typeof t2) return;
        const e2 = t2.indexOf(":");
        if (-1 !== e2 && e2 > 0) {
          const n2 = t2.substring(0, e2);
          if ("xmlns" !== n2) return n2;
        }
      }
      class it {
        constructor(t2, e2) {
          var n2;
          this.options = t2, this.currentNode = null, this.tagsNodeStack = [], this.parseXml = ht, this.parseTextData = st, this.resolveNameSpace = rt, this.buildAttributesMap = at, this.isItStopNode = ct, this.replaceEntitiesValue = ut, this.readStopNodeData = mt, this.saveTextToParentTag = pt, this.addChild = lt, this.ignoreAttributesFn = "function" == typeof (n2 = this.options.ignoreAttributes) ? n2 : Array.isArray(n2) ? (t3) => {
            for (const e3 of n2) {
              if ("string" == typeof e3 && t3 === e3) return true;
              if (e3 instanceof RegExp && e3.test(t3)) return true;
            }
          } : () => false, this.entityExpansionCount = 0, this.currentExpandedLength = 0;
          let i2 = __spreadValues({}, W);
          this.options.entityDecoder ? this.entityDecoder = this.options.entityDecoder : ("object" == typeof this.options.htmlEntities ? i2 = this.options.htmlEntities : true === this.options.htmlEntities && (i2 = __spreadValues(__spreadValues({}, X), U)), this.entityDecoder = new tt({ namedEntities: __spreadValues(__spreadValues({}, i2), e2), numericAllowed: this.options.htmlEntities, limit: { maxTotalExpansions: this.options.processEntities.maxTotalExpansions, maxExpandedLength: this.options.processEntities.maxExpandedLength, applyLimitsTo: this.options.processEntities.appliesTo } })), this.matcher = new R(), this.readonlyMatcher = this.matcher.readOnly(), this.isCurrentNodeStopNode = false, this.stopNodeExpressionsSet = new B();
          const s2 = this.options.stopNodes;
          if (s2 && s2.length > 0) {
            for (let t3 = 0; t3 < s2.length; t3++) {
              const e3 = s2[t3];
              "string" == typeof e3 ? this.stopNodeExpressionsSet.add(new G(e3)) : e3 instanceof G && this.stopNodeExpressionsSet.add(e3);
            }
            this.stopNodeExpressionsSet.seal();
          }
        }
      }
      function st(t2, e2, n2, i2, s2, r2, o2) {
        const a2 = this.options;
        if (void 0 !== t2 && (a2.trimValues && !i2 && (t2 = t2.trim()), t2.length > 0)) {
          o2 || (t2 = this.replaceEntitiesValue(t2, e2, n2));
          const i3 = a2.jPath ? n2.toString() : n2, h2 = a2.tagValueProcessor(e2, t2, i3, s2, r2);
          return null == h2 ? t2 : typeof h2 != typeof t2 || h2 !== t2 ? h2 : a2.trimValues || t2.trim() === t2 ? xt(t2, a2.parseTagValue, a2.numberParseOptions) : t2;
        }
      }
      function rt(t2) {
        if (this.options.removeNSPrefix) {
          const e2 = t2.split(":"), n2 = "/" === t2.charAt(0) ? "/" : "";
          if ("xmlns" === e2[0]) return "";
          2 === e2.length && (t2 = n2 + e2[1]);
        }
        return t2;
      }
      const ot = new RegExp("([^\\s=]+)\\s*(=\\s*(['\"])([\\s\\S]*?)\\3)?", "gm");
      function at(t2, e2, n2, i2 = false) {
        const r2 = this.options;
        if (true === i2 || true !== r2.ignoreAttributes && "string" == typeof t2) {
          const i3 = s(t2, ot), o2 = i3.length, a2 = {}, h2 = new Array(o2);
          let l2 = false;
          const u2 = {};
          for (let t3 = 0; t3 < o2; t3++) {
            const e3 = this.resolveNameSpace(i3[t3][1]), s2 = i3[t3][4];
            if (e3.length && void 0 !== s2) {
              let i4 = s2;
              r2.trimValues && (i4 = i4.trim()), i4 = this.replaceEntitiesValue(i4, n2, this.readonlyMatcher), h2[t3] = i4, u2[e3] = i4, l2 = true;
            }
          }
          l2 && "object" == typeof e2 && e2.updateCurrent && e2.updateCurrent(u2);
          const p2 = r2.jPath ? e2.toString() : this.readonlyMatcher;
          let c2 = false;
          for (let t3 = 0; t3 < o2; t3++) {
            const e3 = this.resolveNameSpace(i3[t3][1]);
            if (this.ignoreAttributesFn(e3, p2)) continue;
            let n3 = r2.attributeNamePrefix + e3;
            if (e3.length) if (r2.transformAttributeName && (n3 = r2.transformAttributeName(n3)), n3 = bt(n3, r2), void 0 !== i3[t3][4]) {
              const i4 = h2[t3], s2 = r2.attributeValueProcessor(e3, i4, p2);
              a2[n3] = null == s2 ? i4 : typeof s2 != typeof i4 || s2 !== i4 ? s2 : xt(i4, r2.parseAttributeValue, r2.numberParseOptions), c2 = true;
            } else r2.allowBooleanAttributes && (a2[n3] = true, c2 = true);
          }
          if (!c2) return;
          if (r2.attributesGroupName && !r2.preserveOrder) {
            const t3 = {};
            return t3[r2.attributesGroupName] = a2, t3;
          }
          return a2;
        }
      }
      const ht = function(t2) {
        t2 = t2.replace(/\r\n?/g, "\n");
        const e2 = new O("!xml");
        let n2 = e2, i2 = "";
        this.matcher.reset(), this.entityDecoder.reset(), this.entityExpansionCount = 0, this.currentExpandedLength = 0;
        const s2 = this.options, r2 = new $(s2.processEntities), o2 = t2.length;
        for (let a2 = 0; a2 < o2; a2++) if ("<" === t2[a2]) {
          const h2 = t2.charCodeAt(a2 + 1);
          if (47 === h2) {
            const e3 = dt(t2, ">", a2, "Closing Tag is not closed.");
            let r3 = t2.substring(a2 + 2, e3).trim();
            if (s2.removeNSPrefix) {
              const t3 = r3.indexOf(":");
              -1 !== t3 && (r3 = r3.substr(t3 + 1));
            }
            r3 = Nt(s2.transformTagName, r3, "", s2).tagName, n2 && (i2 = this.saveTextToParentTag(i2, n2, this.readonlyMatcher));
            const o3 = this.matcher.getCurrentTag();
            if (r3 && s2.unpairedTagsSet.has(r3)) throw new Error("Unpaired tag can not be used as closing tag: </".concat(r3, ">"));
            o3 && s2.unpairedTagsSet.has(o3) && (this.matcher.pop(), this.tagsNodeStack.pop()), this.matcher.pop(), this.isCurrentNodeStopNode = false, n2 = this.tagsNodeStack.pop(), i2 = "", a2 = e3;
          } else if (63 === h2) {
            let e3 = gt(t2, a2, false, "?>");
            if (!e3) throw new Error("Pi Tag is not closed.");
            i2 = this.saveTextToParentTag(i2, n2, this.readonlyMatcher);
            const r3 = this.buildAttributesMap(e3.tagExp, this.matcher, e3.tagName, true);
            if (r3) {
              const t3 = r3[this.options.attributeNamePrefix + "version"];
              this.entityDecoder.setXmlVersion(Number(t3) || 1);
            }
            if (s2.ignoreDeclaration && "?xml" === e3.tagName || s2.ignorePiTags) ;
            else {
              const t3 = new O(e3.tagName);
              t3.add(s2.textNodeName, ""), e3.tagName !== e3.tagExp && e3.attrExpPresent && true !== s2.ignoreAttributes && (t3[":@"] = r3), this.addChild(n2, t3, this.readonlyMatcher, a2);
            }
            a2 = e3.closeIndex + 1;
          } else if (33 === h2 && 45 === t2.charCodeAt(a2 + 2) && 45 === t2.charCodeAt(a2 + 3)) {
            const e3 = dt(t2, "-->", a2 + 4, "Comment is not closed.");
            if (s2.commentPropName) {
              const r3 = t2.substring(a2 + 4, e3 - 2);
              i2 = this.saveTextToParentTag(i2, n2, this.readonlyMatcher), n2.add(s2.commentPropName, [{ [s2.textNodeName]: r3 }]);
            }
            a2 = e3;
          } else if (33 === h2 && 68 === t2.charCodeAt(a2 + 2)) {
            const e3 = r2.readDocType(t2, a2);
            this.entityDecoder.addInputEntities(e3.entities), a2 = e3.i;
          } else if (33 === h2 && 91 === t2.charCodeAt(a2 + 2)) {
            const e3 = dt(t2, "]]>", a2, "CDATA is not closed.") - 2, r3 = t2.substring(a2 + 9, e3);
            i2 = this.saveTextToParentTag(i2, n2, this.readonlyMatcher);
            let o3 = this.parseTextData(r3, n2.tagname, this.readonlyMatcher, true, false, true, true);
            null == o3 && (o3 = ""), s2.cdataPropName ? n2.add(s2.cdataPropName, [{ [s2.textNodeName]: r3 }]) : n2.add(s2.textNodeName, o3), a2 = e3 + 2;
          } else {
            let r3 = gt(t2, a2, s2.removeNSPrefix);
            if (!r3) {
              const e3 = t2.substring(Math.max(0, a2 - 50), Math.min(o2, a2 + 50));
              throw new Error("readTagExp returned undefined at position ".concat(a2, '. Context: "').concat(e3, '"'));
            }
            let h3 = r3.tagName;
            const l2 = r3.rawTagName;
            let u2 = r3.tagExp, p2 = r3.attrExpPresent, c2 = r3.closeIndex;
            if ({ tagName: h3, tagExp: u2 } = Nt(s2.transformTagName, h3, u2, s2), s2.strictReservedNames && (h3 === s2.commentPropName || h3 === s2.cdataPropName || h3 === s2.textNodeName || h3 === s2.attributesGroupName)) throw new Error("Invalid tag name: ".concat(h3));
            n2 && i2 && "!xml" !== n2.tagname && (i2 = this.saveTextToParentTag(i2, n2, this.readonlyMatcher, false));
            const d2 = n2;
            d2 && s2.unpairedTagsSet.has(d2.tagname) && (n2 = this.tagsNodeStack.pop(), this.matcher.pop());
            let f2 = false;
            u2.length > 0 && u2.lastIndexOf("/") === u2.length - 1 && (f2 = true, "/" === h3[h3.length - 1] ? (h3 = h3.substr(0, h3.length - 1), u2 = h3) : u2 = u2.substr(0, u2.length - 1), p2 = h3 !== u2);
            let g2, m2 = null, x2 = {};
            g2 = nt(l2), h3 !== e2.tagname && this.matcher.push(h3, {}, g2), h3 !== u2 && p2 && (m2 = this.buildAttributesMap(u2, this.matcher, h3), m2 && (x2 = et(m2, s2))), h3 !== e2.tagname && (this.isCurrentNodeStopNode = this.isItStopNode());
            const N2 = a2;
            if (this.isCurrentNodeStopNode) {
              let e3 = "";
              if (f2) a2 = r3.closeIndex;
              else if (s2.unpairedTagsSet.has(h3)) a2 = r3.closeIndex;
              else {
                const n3 = this.readStopNodeData(t2, l2, c2 + 1);
                if (!n3) throw new Error("Unexpected end of ".concat(l2));
                a2 = n3.i, e3 = n3.tagContent;
              }
              const i3 = new O(h3);
              m2 && (i3[":@"] = m2), i3.add(s2.textNodeName, e3), this.matcher.pop(), this.isCurrentNodeStopNode = false, this.addChild(n2, i3, this.readonlyMatcher, N2);
            } else {
              if (f2) {
                ({ tagName: h3, tagExp: u2 } = Nt(s2.transformTagName, h3, u2, s2));
                const t3 = new O(h3);
                m2 && (t3[":@"] = m2), this.addChild(n2, t3, this.readonlyMatcher, N2), this.matcher.pop(), this.isCurrentNodeStopNode = false;
              } else {
                if (s2.unpairedTagsSet.has(h3)) {
                  const t3 = new O(h3);
                  m2 && (t3[":@"] = m2), this.addChild(n2, t3, this.readonlyMatcher, N2), this.matcher.pop(), this.isCurrentNodeStopNode = false, a2 = r3.closeIndex;
                  continue;
                }
                {
                  const t3 = new O(h3);
                  if (this.tagsNodeStack.length > s2.maxNestedTags) throw new Error("Maximum nested tags exceeded");
                  this.tagsNodeStack.push(n2), m2 && (t3[":@"] = m2), this.addChild(n2, t3, this.readonlyMatcher, N2), n2 = t3;
                }
              }
              i2 = "", a2 = c2;
            }
          }
        } else i2 += t2[a2];
        return e2.child;
      };
      function lt(t2, e2, n2, i2) {
        this.options.captureMetaData || (i2 = void 0);
        const s2 = this.options.jPath ? n2.toString() : n2, r2 = this.options.updateTag(e2.tagname, s2, e2[":@"]);
        false === r2 || ("string" == typeof r2 ? (e2.tagname = r2, t2.addChild(e2, i2)) : t2.addChild(e2, i2));
      }
      function ut(t2, e2, n2) {
        const i2 = this.options.processEntities;
        if (!i2 || !i2.enabled) return t2;
        if (i2.allowedTags) {
          const s2 = this.options.jPath ? n2.toString() : n2;
          if (!(Array.isArray(i2.allowedTags) ? i2.allowedTags.includes(e2) : i2.allowedTags(e2, s2))) return t2;
        }
        if (i2.tagFilter) {
          const s2 = this.options.jPath ? n2.toString() : n2;
          if (!i2.tagFilter(e2, s2)) return t2;
        }
        return this.entityDecoder.decode(t2);
      }
      function pt(t2, e2, n2, i2) {
        return t2 && (void 0 === i2 && (i2 = 0 === e2.child.length), void 0 !== (t2 = this.parseTextData(t2, e2.tagname, n2, false, !!e2[":@"] && 0 !== Object.keys(e2[":@"]).length, i2)) && "" !== t2 && e2.add(this.options.textNodeName, t2), t2 = ""), t2;
      }
      function ct() {
        return 0 !== this.stopNodeExpressionsSet.size && this.matcher.matchesAny(this.stopNodeExpressionsSet);
      }
      function dt(t2, e2, n2, i2) {
        const s2 = t2.indexOf(e2, n2);
        if (-1 === s2) throw new Error(i2);
        return s2 + e2.length - 1;
      }
      function ft(t2, e2, n2, i2) {
        const s2 = t2.indexOf(e2, n2);
        if (-1 === s2) throw new Error(i2);
        return s2;
      }
      function gt(t2, e2, n2, i2 = ">") {
        const s2 = (function(t3, e3, n3 = ">") {
          let i3 = 0;
          const s3 = t3.length, r3 = n3.charCodeAt(0), o3 = n3.length > 1 ? n3.charCodeAt(1) : -1;
          let a3 = "", h3 = e3;
          for (let n4 = e3; n4 < s3; n4++) {
            const e4 = t3.charCodeAt(n4);
            if (i3) e4 === i3 && (i3 = 0);
            else if (34 === e4 || 39 === e4) i3 = e4;
            else if (e4 === r3) {
              if (-1 === o3) return a3 += t3.substring(h3, n4), { data: a3, index: n4 };
              if (t3.charCodeAt(n4 + 1) === o3) return a3 += t3.substring(h3, n4), { data: a3, index: n4 };
            } else 9 !== e4 || i3 || (a3 += t3.substring(h3, n4) + " ", h3 = n4 + 1);
          }
        })(t2, e2 + 1, i2);
        if (!s2) return;
        let r2 = s2.data;
        const o2 = s2.index, a2 = r2.search(/\s/);
        let h2 = r2, l2 = true;
        -1 !== a2 && (h2 = r2.substring(0, a2), r2 = r2.substring(a2 + 1).replace(/^\s+/, ""));
        const u2 = h2;
        if (n2) {
          const t3 = h2.indexOf(":");
          -1 !== t3 && (h2 = h2.substr(t3 + 1), l2 = h2 !== s2.data.substr(t3 + 1));
        }
        return { tagName: h2, tagExp: r2, closeIndex: o2, attrExpPresent: l2, rawTagName: u2 };
      }
      function mt(t2, e2, n2) {
        const i2 = n2;
        let s2 = 1;
        const r2 = t2.length;
        for (; n2 < r2; n2++) if ("<" === t2[n2]) {
          const r3 = t2.charCodeAt(n2 + 1);
          if (47 === r3) {
            const r4 = ft(t2, ">", n2, "".concat(e2, " is not closed"));
            if (t2.substring(n2 + 2, r4).trim() === e2 && (s2--, 0 === s2)) return { tagContent: t2.substring(i2, n2), i: r4 };
            n2 = r4;
          } else if (63 === r3) n2 = dt(t2, "?>", n2 + 1, "StopNode is not closed.");
          else if (33 === r3 && 45 === t2.charCodeAt(n2 + 2) && 45 === t2.charCodeAt(n2 + 3)) n2 = dt(t2, "-->", n2 + 3, "StopNode is not closed.");
          else if (33 === r3 && 91 === t2.charCodeAt(n2 + 2)) n2 = dt(t2, "]]>", n2, "StopNode is not closed.") - 2;
          else {
            const i3 = gt(t2, n2, ">");
            i3 && ((i3 && i3.tagName) === e2 && "/" !== i3.tagExp[i3.tagExp.length - 1] && s2++, n2 = i3.closeIndex);
          }
        }
      }
      function xt(t2, e2, n2) {
        if (e2 && "string" == typeof t2) {
          const e3 = t2.trim();
          return "true" === e3 || "false" !== e3 && (function(t3, e4 = {}) {
            if (e4 = Object.assign({}, L, e4), !t3 || "string" != typeof t3) return t3;
            let n3 = t3.trim();
            if (0 === n3.length) return t3;
            if (void 0 !== e4.skipLike && e4.skipLike.test(n3)) return t3;
            if ("0" === n3) return 0;
            if (e4.hex && j.test(n3)) return (function(t4) {
              if (parseInt) return parseInt(t4, 16);
              if (Number.parseInt) return Number.parseInt(t4, 16);
              if (window && window.parseInt) return window.parseInt(t4, 16);
              throw new Error("parseInt, Number.parseInt, window.parseInt are not supported");
            })(n3);
            if (isFinite(n3)) {
              if (n3.includes("e") || n3.includes("E")) return (function(t4, e5, n4) {
                if (!n4.eNotation) return t4;
                const i3 = e5.match(k);
                if (i3) {
                  let s2 = i3[1] || "";
                  const r2 = -1 === i3[3].indexOf("e") ? "E" : "e", o2 = i3[2], a2 = s2 ? t4[o2.length + 1] === r2 : t4[o2.length] === r2;
                  return o2.length > 1 && a2 ? t4 : (1 !== o2.length || !i3[3].startsWith(".".concat(r2)) && i3[3][0] !== r2) && o2.length > 0 ? n4.leadingZeros && !a2 ? (e5 = (i3[1] || "") + i3[3], Number(e5)) : t4 : Number(e5);
                }
                return t4;
              })(t3, n3, e4);
              {
                const s2 = V.exec(n3);
                if (s2) {
                  const r2 = s2[1] || "", o2 = s2[2];
                  let a2 = (i2 = s2[3]) && -1 !== i2.indexOf(".") ? ("." === (i2 = i2.replace(/0+$/, "")) ? i2 = "0" : "." === i2[0] ? i2 = "0" + i2 : "." === i2[i2.length - 1] && (i2 = i2.substring(0, i2.length - 1)), i2) : i2;
                  const h2 = r2 ? "." === t3[o2.length + 1] : "." === t3[o2.length];
                  if (!e4.leadingZeros && (o2.length > 1 || 1 === o2.length && !h2)) return t3;
                  {
                    const i3 = Number(n3), s3 = String(i3);
                    if (0 === i3) return i3;
                    if (-1 !== s3.search(/[eE]/)) return e4.eNotation ? i3 : t3;
                    if (-1 !== n3.indexOf(".")) return "0" === s3 || s3 === a2 || s3 === "".concat(r2).concat(a2) ? i3 : t3;
                    let h3 = o2 ? a2 : n3;
                    return o2 ? h3 === s3 || r2 + h3 === s3 ? i3 : t3 : h3 === s3 || h3 === r2 + s3 ? i3 : t3;
                  }
                }
                return t3;
              }
            }
            var i2;
            return (function(t4, e5, n4) {
              const i3 = e5 === 1 / 0;
              switch (n4.infinity.toLowerCase()) {
                case "null":
                  return null;
                case "infinity":
                  return e5;
                case "string":
                  return i3 ? "Infinity" : "-Infinity";
                default:
                  return t4;
              }
            })(t3, Number(n3), e4);
          })(t2, n2);
        }
        return void 0 !== t2 ? t2 : "";
      }
      function Nt(t2, e2, n2, i2) {
        if (t2) {
          const i3 = t2(e2);
          n2 === e2 && (n2 = i3), e2 = i3;
        }
        return { tagName: e2 = bt(e2, i2), tagExp: n2 };
      }
      function bt(t2, e2) {
        if (a.includes(t2)) throw new Error('[SECURITY] Invalid name: "'.concat(t2, '" is a reserved JavaScript keyword that could cause prototype pollution'));
        return o.includes(t2) ? e2.onDangerousProperty(t2) : t2;
      }
      const yt = O.getMetaDataSymbol();
      function Et(t2, e2) {
        if (!t2 || "object" != typeof t2) return {};
        if (!e2) return t2;
        const n2 = {};
        for (const i2 in t2) i2.startsWith(e2) ? n2[i2.substring(e2.length)] = t2[i2] : n2[i2] = t2[i2];
        return n2;
      }
      function wt(t2, e2, n2, i2) {
        return vt(t2, e2, n2, i2);
      }
      function vt(t2, e2, n2, i2) {
        let s2;
        const r2 = {};
        for (let o2 = 0; o2 < t2.length; o2++) {
          const a2 = t2[o2], h2 = St(a2);
          if (void 0 !== h2 && h2 !== e2.textNodeName) {
            const t3 = Et(a2[":@"] || {}, e2.attributeNamePrefix);
            n2.push(h2, t3);
          }
          if (h2 === e2.textNodeName) void 0 === s2 ? s2 = a2[h2] : s2 += "" + a2[h2];
          else {
            if (void 0 === h2) continue;
            if (a2[h2]) {
              let t3 = vt(a2[h2], e2, n2, i2);
              const s3 = At(t3, e2);
              if (a2[":@"] ? _t(t3, a2[":@"], i2, e2) : 1 !== Object.keys(t3).length || void 0 === t3[e2.textNodeName] || e2.alwaysCreateTextNode ? 0 === Object.keys(t3).length && (e2.alwaysCreateTextNode ? t3[e2.textNodeName] = "" : t3 = "") : t3 = t3[e2.textNodeName], void 0 !== a2[yt] && "object" == typeof t3 && null !== t3 && (t3[yt] = a2[yt]), void 0 !== r2[h2] && Object.prototype.hasOwnProperty.call(r2, h2)) Array.isArray(r2[h2]) || (r2[h2] = [r2[h2]]), r2[h2].push(t3);
              else {
                const n3 = e2.jPath ? i2.toString() : i2;
                e2.isArray(h2, n3, s3) ? r2[h2] = [t3] : r2[h2] = t3;
              }
              void 0 !== h2 && h2 !== e2.textNodeName && n2.pop();
            }
          }
        }
        return "string" == typeof s2 ? s2.length > 0 && (r2[e2.textNodeName] = s2) : void 0 !== s2 && (r2[e2.textNodeName] = s2), r2;
      }
      function St(t2) {
        const e2 = Object.keys(t2);
        for (let t3 = 0; t3 < e2.length; t3++) {
          const n2 = e2[t3];
          if (":@" !== n2) return n2;
        }
      }
      function _t(t2, e2, n2, i2) {
        if (e2) {
          const s2 = Object.keys(e2), r2 = s2.length;
          for (let o2 = 0; o2 < r2; o2++) {
            const r3 = s2[o2], a2 = r3.startsWith(i2.attributeNamePrefix) ? r3.substring(i2.attributeNamePrefix.length) : r3, h2 = i2.jPath ? n2.toString() + "." + a2 : n2;
            i2.isArray(r3, h2, true, true) ? t2[r3] = [e2[r3]] : t2[r3] = e2[r3];
          }
        }
      }
      function At(t2, e2) {
        const { textNodeName: n2 } = e2, i2 = Object.keys(t2).length;
        return 0 === i2 || !(1 !== i2 || !t2[n2] && "boolean" != typeof t2[n2] && 0 !== t2[n2]);
      }
      class Tt {
        constructor(t2) {
          this.externalEntities = {}, this.options = C(t2);
        }
        parse(t2, e2) {
          if ("string" != typeof t2 && t2.toString) t2 = t2.toString();
          else if ("string" != typeof t2) throw new Error("XML data is accepted in String or Bytes[] form.");
          if (e2) {
            true === e2 && (e2 = {});
            const n3 = l(t2, e2);
            if (true !== n3) throw Error("".concat(n3.err.msg, ":").concat(n3.err.line, ":").concat(n3.err.col));
          }
          const n2 = new it(this.options, this.externalEntities), i2 = n2.parseXml(t2);
          return this.options.preserveOrder || void 0 === i2 ? i2 : wt(i2, this.options, n2.matcher, n2.readonlyMatcher);
        }
        addEntity(t2, e2) {
          if (-1 !== e2.indexOf("&")) throw new Error("Entity value can't have '&'");
          if (-1 !== t2.indexOf("&") || -1 !== t2.indexOf(";")) throw new Error("An entity must be set without '&' and ';'. Eg. use '#xD' for '&#xD;'");
          if ("&" === e2) throw new Error("An entity with value '&' is not permitted");
          this.externalEntities[t2] = e2;
        }
        static getMetaDataSymbol() {
          return O.getMetaDataSymbol();
        }
      }
      function Ct(t2, e2) {
        let n2 = "";
        e2.format && e2.indentBy.length > 0 && (n2 = "\n");
        const i2 = [];
        if (e2.stopNodes && Array.isArray(e2.stopNodes)) for (let t3 = 0; t3 < e2.stopNodes.length; t3++) {
          const n3 = e2.stopNodes[t3];
          "string" == typeof n3 ? i2.push(new G(n3)) : n3 instanceof G && i2.push(n3);
        }
        return Pt(t2, e2, n2, new R(), i2);
      }
      function Pt(t2, e2, n2, i2, s2) {
        let r2 = "", o2 = false;
        if (e2.maxNestedTags && i2.getDepth() > e2.maxNestedTags) throw new Error("Maximum nested tags exceeded");
        if (!Array.isArray(t2)) {
          if (null != t2) {
            let n3 = t2.toString();
            return n3 = Vt(n3, e2), n3;
          }
          return "";
        }
        for (let a2 = 0; a2 < t2.length; a2++) {
          const h2 = t2[a2], l2 = Dt(h2);
          if (void 0 === l2) continue;
          const u2 = Ot(h2[":@"], e2);
          i2.push(l2, u2);
          const p2 = jt(i2, s2);
          if (l2 === e2.textNodeName) {
            let t3 = h2[l2];
            p2 || (t3 = e2.tagValueProcessor(l2, t3), t3 = Vt(t3, e2)), o2 && (r2 += n2), r2 += t3, o2 = false, i2.pop();
            continue;
          }
          if (l2 === e2.cdataPropName) {
            o2 && (r2 += n2);
            const t3 = h2[l2][0][e2.textNodeName];
            r2 += "<![CDATA[".concat(String(t3).replace(/\]\]>/g, "]]]]><![CDATA[>"), "]]>"), o2 = false, i2.pop();
            continue;
          }
          if (l2 === e2.commentPropName) {
            const t3 = h2[l2][0][e2.textNodeName];
            r2 += n2 + "<!--".concat(String(t3).replace(/--/g, "- -").replace(/-$/, "- "), "-->"), o2 = true, i2.pop();
            continue;
          }
          if ("?" === l2[0]) {
            const t3 = Mt(h2[":@"], e2, p2), s3 = "?xml" === l2 ? "" : n2;
            let a3 = h2[l2][0][e2.textNodeName];
            a3 = 0 !== a3.length ? " " + a3 : "", r2 += s3 + "<".concat(l2).concat(a3).concat(t3, "?>"), o2 = true, i2.pop();
            continue;
          }
          let c2 = n2;
          "" !== c2 && (c2 += e2.indentBy);
          const d2 = n2 + "<".concat(l2).concat(Mt(h2[":@"], e2, p2));
          let f2;
          f2 = p2 ? $t(h2[l2], e2) : Pt(h2[l2], e2, c2, i2, s2), -1 !== e2.unpairedTags.indexOf(l2) ? e2.suppressUnpairedNode ? r2 += d2 + ">" : r2 += d2 + "/>" : f2 && 0 !== f2.length || !e2.suppressEmptyNode ? f2 && f2.endsWith(">") ? r2 += d2 + ">".concat(f2).concat(n2, "</").concat(l2, ">") : (r2 += d2 + ">", f2 && "" !== n2 && (f2.includes("/>") || f2.includes("</")) ? r2 += n2 + e2.indentBy + f2 + n2 : r2 += f2, r2 += "</".concat(l2, ">")) : r2 += d2 + "/>", o2 = true, i2.pop();
        }
        return r2;
      }
      function Ot(t2, e2) {
        if (!t2 || e2.ignoreAttributes) return null;
        const n2 = {};
        let i2 = false;
        for (let s2 in t2) Object.prototype.hasOwnProperty.call(t2, s2) && (n2[s2.startsWith(e2.attributeNamePrefix) ? s2.substr(e2.attributeNamePrefix.length) : s2] = t2[s2], i2 = true);
        return i2 ? n2 : null;
      }
      function $t(t2, e2) {
        if (!Array.isArray(t2)) return null != t2 ? t2.toString() : "";
        let n2 = "";
        for (let i2 = 0; i2 < t2.length; i2++) {
          const s2 = t2[i2], r2 = Dt(s2);
          if (r2 === e2.textNodeName) n2 += s2[r2];
          else if (r2 === e2.cdataPropName) n2 += s2[r2][0][e2.textNodeName];
          else if (r2 === e2.commentPropName) n2 += s2[r2][0][e2.textNodeName];
          else {
            if (r2 && "?" === r2[0]) continue;
            if (r2) {
              const t3 = It(s2[":@"], e2), i3 = $t(s2[r2], e2);
              i3 && 0 !== i3.length ? n2 += "<".concat(r2).concat(t3, ">").concat(i3, "</").concat(r2, ">") : n2 += "<".concat(r2).concat(t3, "/>");
            }
          }
        }
        return n2;
      }
      function It(t2, e2) {
        let n2 = "";
        if (t2 && !e2.ignoreAttributes) for (let i2 in t2) {
          if (!Object.prototype.hasOwnProperty.call(t2, i2)) continue;
          let s2 = t2[i2];
          true === s2 && e2.suppressBooleanAttributes ? n2 += " ".concat(i2.substr(e2.attributeNamePrefix.length)) : n2 += " ".concat(i2.substr(e2.attributeNamePrefix.length), '="').concat(s2, '"');
        }
        return n2;
      }
      function Dt(t2) {
        const e2 = Object.keys(t2);
        for (let n2 = 0; n2 < e2.length; n2++) {
          const i2 = e2[n2];
          if (Object.prototype.hasOwnProperty.call(t2, i2) && ":@" !== i2) return i2;
        }
      }
      function Mt(t2, e2, n2) {
        let i2 = "";
        if (t2 && !e2.ignoreAttributes) for (let s2 in t2) {
          if (!Object.prototype.hasOwnProperty.call(t2, s2)) continue;
          let r2;
          n2 ? r2 = t2[s2] : (r2 = e2.attributeValueProcessor(s2, t2[s2]), r2 = Vt(r2, e2)), true === r2 && e2.suppressBooleanAttributes ? i2 += " ".concat(s2.substr(e2.attributeNamePrefix.length)) : i2 += " ".concat(s2.substr(e2.attributeNamePrefix.length), '="').concat(r2, '"');
        }
        return i2;
      }
      function jt(t2, e2) {
        if (!e2 || 0 === e2.length) return false;
        for (let n2 = 0; n2 < e2.length; n2++) if (t2.matches(e2[n2])) return true;
        return false;
      }
      function Vt(t2, e2) {
        if (t2 && t2.length > 0 && e2.processEntities) for (let n2 = 0; n2 < e2.entities.length; n2++) {
          const i2 = e2.entities[n2];
          t2 = t2.replace(i2.regex, i2.val);
        }
        return t2;
      }
      const Lt = { attributeNamePrefix: "@_", attributesGroupName: false, textNodeName: "#text", ignoreAttributes: true, cdataPropName: false, format: false, indentBy: "  ", suppressEmptyNode: false, suppressUnpairedNode: true, suppressBooleanAttributes: true, tagValueProcessor: function(t2, e2) {
        return e2;
      }, attributeValueProcessor: function(t2, e2) {
        return e2;
      }, preserveOrder: false, commentPropName: false, unpairedTags: [], entities: [{ regex: new RegExp("&", "g"), val: "&amp;" }, { regex: new RegExp(">", "g"), val: "&gt;" }, { regex: new RegExp("<", "g"), val: "&lt;" }, { regex: new RegExp("'", "g"), val: "&apos;" }, { regex: new RegExp('"', "g"), val: "&quot;" }], processEntities: true, stopNodes: [], oneListGroup: false, maxNestedTags: 100, jPath: true };
      function kt(t2) {
        if (this.options = Object.assign({}, Lt, t2), this.options.stopNodes && Array.isArray(this.options.stopNodes) && (this.options.stopNodes = this.options.stopNodes.map((t3) => "string" == typeof t3 && t3.startsWith("*.") ? ".." + t3.substring(2) : t3)), this.stopNodeExpressions = [], this.options.stopNodes && Array.isArray(this.options.stopNodes)) for (let t3 = 0; t3 < this.options.stopNodes.length; t3++) {
          const e3 = this.options.stopNodes[t3];
          "string" == typeof e3 ? this.stopNodeExpressions.push(new G(e3)) : e3 instanceof G && this.stopNodeExpressions.push(e3);
        }
        var e2;
        true === this.options.ignoreAttributes || this.options.attributesGroupName ? this.isAttribute = function() {
          return false;
        } : (this.ignoreAttributesFn = "function" == typeof (e2 = this.options.ignoreAttributes) ? e2 : Array.isArray(e2) ? (t3) => {
          for (const n2 of e2) {
            if ("string" == typeof n2 && t3 === n2) return true;
            if (n2 instanceof RegExp && n2.test(t3)) return true;
          }
        } : () => false, this.attrPrefixLen = this.options.attributeNamePrefix.length, this.isAttribute = Gt), this.processTextOrObjNode = Ft, this.options.format ? (this.indentate = Rt, this.tagEndChar = ">\n", this.newLine = "\n") : (this.indentate = function() {
          return "";
        }, this.tagEndChar = ">", this.newLine = "");
      }
      function Ft(t2, e2, n2, i2) {
        const s2 = this.extractAttributes(t2);
        if (i2.push(e2, s2), this.checkStopNode(i2)) {
          const s3 = this.buildRawContent(t2), r3 = this.buildAttributesForStopNode(t2);
          return i2.pop(), this.buildObjectNode(s3, e2, r3, n2);
        }
        const r2 = this.j2x(t2, n2 + 1, i2);
        return i2.pop(), void 0 !== t2[this.options.textNodeName] && 1 === Object.keys(t2).length ? this.buildTextValNode(t2[this.options.textNodeName], e2, r2.attrStr, n2, i2) : this.buildObjectNode(r2.val, e2, r2.attrStr, n2);
      }
      function Rt(t2) {
        return this.options.indentBy.repeat(t2);
      }
      function Gt(t2) {
        return !(!t2.startsWith(this.options.attributeNamePrefix) || t2 === this.options.textNodeName) && t2.substr(this.attrPrefixLen);
      }
      kt.prototype.build = function(t2) {
        if (this.options.preserveOrder) return Ct(t2, this.options);
        {
          Array.isArray(t2) && this.options.arrayNodeName && this.options.arrayNodeName.length > 1 && (t2 = { [this.options.arrayNodeName]: t2 });
          const e2 = new R();
          return this.j2x(t2, 0, e2).val;
        }
      }, kt.prototype.j2x = function(t2, e2, n2) {
        let i2 = "", s2 = "";
        if (this.options.maxNestedTags && n2.getDepth() >= this.options.maxNestedTags) throw new Error("Maximum nested tags exceeded");
        const r2 = this.options.jPath ? n2.toString() : n2, o2 = this.checkStopNode(n2);
        for (let a2 in t2) if (Object.prototype.hasOwnProperty.call(t2, a2)) if (void 0 === t2[a2]) this.isAttribute(a2) && (s2 += "");
        else if (null === t2[a2]) this.isAttribute(a2) || a2 === this.options.cdataPropName ? s2 += "" : "?" === a2[0] ? s2 += this.indentate(e2) + "<" + a2 + "?" + this.tagEndChar : s2 += this.indentate(e2) + "<" + a2 + "/" + this.tagEndChar;
        else if (t2[a2] instanceof Date) s2 += this.buildTextValNode(t2[a2], a2, "", e2, n2);
        else if ("object" != typeof t2[a2]) {
          const h2 = this.isAttribute(a2);
          if (h2 && !this.ignoreAttributesFn(h2, r2)) i2 += this.buildAttrPairStr(h2, "" + t2[a2], o2);
          else if (!h2) if (a2 === this.options.textNodeName) {
            let e3 = this.options.tagValueProcessor(a2, "" + t2[a2]);
            s2 += this.replaceEntitiesValue(e3);
          } else {
            n2.push(a2);
            const i3 = this.checkStopNode(n2);
            if (n2.pop(), i3) {
              const n3 = "" + t2[a2];
              s2 += "" === n3 ? this.indentate(e2) + "<" + a2 + this.closeTag(a2) + this.tagEndChar : this.indentate(e2) + "<" + a2 + ">" + n3 + "</" + a2 + this.tagEndChar;
            } else s2 += this.buildTextValNode(t2[a2], a2, "", e2, n2);
          }
        } else if (Array.isArray(t2[a2])) {
          const i3 = t2[a2].length;
          let r3 = "", o3 = "";
          for (let h2 = 0; h2 < i3; h2++) {
            const i4 = t2[a2][h2];
            if (void 0 === i4) ;
            else if (null === i4) "?" === a2[0] ? s2 += this.indentate(e2) + "<" + a2 + "?" + this.tagEndChar : s2 += this.indentate(e2) + "<" + a2 + "/" + this.tagEndChar;
            else if ("object" == typeof i4) if (this.options.oneListGroup) {
              n2.push(a2);
              const t3 = this.j2x(i4, e2 + 1, n2);
              n2.pop(), r3 += t3.val, this.options.attributesGroupName && i4.hasOwnProperty(this.options.attributesGroupName) && (o3 += t3.attrStr);
            } else r3 += this.processTextOrObjNode(i4, a2, e2, n2);
            else if (this.options.oneListGroup) {
              let t3 = this.options.tagValueProcessor(a2, i4);
              t3 = this.replaceEntitiesValue(t3), r3 += t3;
            } else {
              n2.push(a2);
              const t3 = this.checkStopNode(n2);
              if (n2.pop(), t3) {
                const t4 = "" + i4;
                r3 += "" === t4 ? this.indentate(e2) + "<" + a2 + this.closeTag(a2) + this.tagEndChar : this.indentate(e2) + "<" + a2 + ">" + t4 + "</" + a2 + this.tagEndChar;
              } else r3 += this.buildTextValNode(i4, a2, "", e2, n2);
            }
          }
          this.options.oneListGroup && (r3 = this.buildObjectNode(r3, a2, o3, e2)), s2 += r3;
        } else if (this.options.attributesGroupName && a2 === this.options.attributesGroupName) {
          const e3 = Object.keys(t2[a2]), n3 = e3.length;
          for (let s3 = 0; s3 < n3; s3++) i2 += this.buildAttrPairStr(e3[s3], "" + t2[a2][e3[s3]], o2);
        } else s2 += this.processTextOrObjNode(t2[a2], a2, e2, n2);
        return { attrStr: i2, val: s2 };
      }, kt.prototype.buildAttrPairStr = function(t2, e2, n2) {
        return n2 || (e2 = this.options.attributeValueProcessor(t2, "" + e2), e2 = this.replaceEntitiesValue(e2)), this.options.suppressBooleanAttributes && "true" === e2 ? " " + t2 : " " + t2 + '="' + e2 + '"';
      }, kt.prototype.extractAttributes = function(t2) {
        if (!t2 || "object" != typeof t2) return null;
        const e2 = {};
        let n2 = false;
        if (this.options.attributesGroupName && t2[this.options.attributesGroupName]) {
          const i2 = t2[this.options.attributesGroupName];
          for (let t3 in i2) Object.prototype.hasOwnProperty.call(i2, t3) && (e2[t3.startsWith(this.options.attributeNamePrefix) ? t3.substring(this.options.attributeNamePrefix.length) : t3] = i2[t3], n2 = true);
        } else for (let i2 in t2) {
          if (!Object.prototype.hasOwnProperty.call(t2, i2)) continue;
          const s2 = this.isAttribute(i2);
          s2 && (e2[s2] = t2[i2], n2 = true);
        }
        return n2 ? e2 : null;
      }, kt.prototype.buildRawContent = function(t2) {
        if ("string" == typeof t2) return t2;
        if ("object" != typeof t2 || null === t2) return String(t2);
        if (void 0 !== t2[this.options.textNodeName]) return t2[this.options.textNodeName];
        let e2 = "";
        for (let n2 in t2) {
          if (!Object.prototype.hasOwnProperty.call(t2, n2)) continue;
          if (this.isAttribute(n2)) continue;
          if (this.options.attributesGroupName && n2 === this.options.attributesGroupName) continue;
          const i2 = t2[n2];
          if (n2 === this.options.textNodeName) e2 += i2;
          else if (Array.isArray(i2)) {
            for (let t3 of i2) if ("string" == typeof t3 || "number" == typeof t3) e2 += "<".concat(n2, ">").concat(t3, "</").concat(n2, ">");
            else if ("object" == typeof t3 && null !== t3) {
              const i3 = this.buildRawContent(t3), s2 = this.buildAttributesForStopNode(t3);
              e2 += "" === i3 ? "<".concat(n2).concat(s2, "/>") : "<".concat(n2).concat(s2, ">").concat(i3, "</").concat(n2, ">");
            }
          } else if ("object" == typeof i2 && null !== i2) {
            const t3 = this.buildRawContent(i2), s2 = this.buildAttributesForStopNode(i2);
            e2 += "" === t3 ? "<".concat(n2).concat(s2, "/>") : "<".concat(n2).concat(s2, ">").concat(t3, "</").concat(n2, ">");
          } else e2 += "<".concat(n2, ">").concat(i2, "</").concat(n2, ">");
        }
        return e2;
      }, kt.prototype.buildAttributesForStopNode = function(t2) {
        if (!t2 || "object" != typeof t2) return "";
        let e2 = "";
        if (this.options.attributesGroupName && t2[this.options.attributesGroupName]) {
          const n2 = t2[this.options.attributesGroupName];
          for (let t3 in n2) {
            if (!Object.prototype.hasOwnProperty.call(n2, t3)) continue;
            const i2 = t3.startsWith(this.options.attributeNamePrefix) ? t3.substring(this.options.attributeNamePrefix.length) : t3, s2 = n2[t3];
            true === s2 && this.options.suppressBooleanAttributes ? e2 += " " + i2 : e2 += " " + i2 + '="' + s2 + '"';
          }
        } else for (let n2 in t2) {
          if (!Object.prototype.hasOwnProperty.call(t2, n2)) continue;
          const i2 = this.isAttribute(n2);
          if (i2) {
            const s2 = t2[n2];
            true === s2 && this.options.suppressBooleanAttributes ? e2 += " " + i2 : e2 += " " + i2 + '="' + s2 + '"';
          }
        }
        return e2;
      }, kt.prototype.buildObjectNode = function(t2, e2, n2, i2) {
        if ("" === t2) return "?" === e2[0] ? this.indentate(i2) + "<" + e2 + n2 + "?" + this.tagEndChar : this.indentate(i2) + "<" + e2 + n2 + this.closeTag(e2) + this.tagEndChar;
        {
          let s2 = "</" + e2 + this.tagEndChar, r2 = "";
          return "?" === e2[0] && (r2 = "?", s2 = ""), !n2 && "" !== n2 || -1 !== t2.indexOf("<") ? false !== this.options.commentPropName && e2 === this.options.commentPropName && 0 === r2.length ? this.indentate(i2) + "<!--".concat(t2, "-->") + this.newLine : this.indentate(i2) + "<" + e2 + n2 + r2 + this.tagEndChar + t2 + this.indentate(i2) + s2 : this.indentate(i2) + "<" + e2 + n2 + r2 + ">" + t2 + s2;
        }
      }, kt.prototype.closeTag = function(t2) {
        let e2 = "";
        return -1 !== this.options.unpairedTags.indexOf(t2) ? this.options.suppressUnpairedNode || (e2 = "/") : e2 = this.options.suppressEmptyNode ? "/" : "></".concat(t2), e2;
      }, kt.prototype.checkStopNode = function(t2) {
        if (!this.stopNodeExpressions || 0 === this.stopNodeExpressions.length) return false;
        for (let e2 = 0; e2 < this.stopNodeExpressions.length; e2++) if (t2.matches(this.stopNodeExpressions[e2])) return true;
        return false;
      }, kt.prototype.buildTextValNode = function(t2, e2, n2, i2, s2) {
        if (false !== this.options.cdataPropName && e2 === this.options.cdataPropName) {
          const e3 = String(t2).replace(/\]\]>/g, "]]]]><![CDATA[>");
          return this.indentate(i2) + "<![CDATA[".concat(e3, "]]>") + this.newLine;
        }
        if (false !== this.options.commentPropName && e2 === this.options.commentPropName) {
          const e3 = String(t2).replace(/--/g, "- -").replace(/-$/, "- ");
          return this.indentate(i2) + "<!--".concat(e3, "-->") + this.newLine;
        }
        if ("?" === e2[0]) return this.indentate(i2) + "<" + e2 + n2 + "?" + this.tagEndChar;
        {
          let s3 = this.options.tagValueProcessor(e2, t2);
          return s3 = this.replaceEntitiesValue(s3), "" === s3 ? this.indentate(i2) + "<" + e2 + n2 + this.closeTag(e2) + this.tagEndChar : this.indentate(i2) + "<" + e2 + n2 + ">" + s3 + "</" + e2 + this.tagEndChar;
        }
      }, kt.prototype.replaceEntitiesValue = function(t2) {
        if (t2 && t2.length > 0 && this.options.processEntities) for (let e2 = 0; e2 < this.options.entities.length; e2++) {
          const n2 = this.options.entities[e2];
          t2 = t2.replace(n2.regex, n2.val);
        }
        return t2;
      };
      const Bt = kt, Ut = { validate: l };
      module2.exports = e;
    })();
  }
});

// lib/soap.js
var require_soap = __commonJS({
  "lib/soap.js"(exports2, module2) {
    "use strict";
    var http2 = require("http");
    var XMLParser = require_fxp().XMLParser;
    var CONTROL_PATH = "/MediaRenderer/RenderingControl/Control";
    var SERVICE_TYPE = "urn:schemas-upnp-org:service:RenderingControl:1";
    var parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "@_",
      parseAttributeValue: true
    });
    var REQUEST_TIMEOUT_MS = 5e3;
    function httpRequest(options, body) {
      return new Promise(function(resolve, reject) {
        var req = http2.request(options, function(res) {
          var chunks = [];
          res.on("data", function(chunk) {
            chunks.push(chunk);
          });
          res.on("end", function() {
            resolve({
              statusCode: res.statusCode,
              headers: res.headers,
              body: Buffer.concat(chunks).toString()
            });
          });
        });
        req.on("error", reject);
        req.setTimeout(REQUEST_TIMEOUT_MS, function() {
          req.abort();
          reject(new Error("request timed out after " + REQUEST_TIMEOUT_MS + "ms"));
        });
        if (body) req.write(body);
        req.end();
      });
    }
    function soapEnvelope(action, args) {
      var argsXml = Object.keys(args).map(function(k) {
        return "<" + k + ">" + args[k] + "</" + k + ">";
      }).join("");
      return '<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:' + action + ' xmlns:u="' + SERVICE_TYPE + '">' + argsXml + "</u:" + action + "></s:Body></s:Envelope>";
    }
    function soapRequest(device, action, args) {
      var body = soapEnvelope(action, args);
      var opts = {
        hostname: device.ip,
        port: device.port,
        path: CONTROL_PATH,
        method: "POST",
        headers: {
          "Content-Type": 'text/xml; charset="utf-8"',
          "SOAPACTION": '"' + SERVICE_TYPE + "#" + action + '"',
          "Content-Length": Buffer.byteLength(body, "utf8")
        }
      };
      return httpRequest(opts, body);
    }
    async function setVolume(device, volume) {
      var res = await soapRequest(device, "SetVolume", {
        InstanceID: 0,
        Channel: "Master",
        DesiredVolume: volume
      });
      if (res.statusCode !== 200) {
        throw new Error("SetVolume failed HTTP " + res.statusCode + ": " + res.body.slice(0, 300));
      }
    }
    async function getVolume2(device) {
      var res = await soapRequest(device, "GetVolume", {
        InstanceID: 0,
        Channel: "Master"
      });
      if (res.statusCode !== 200) {
        throw new Error("GetVolume failed HTTP " + res.statusCode + ": " + res.body.slice(0, 300));
      }
      var parsed = parser.parse(res.body);
      var envelope = parsed["s:Envelope"] || parsed["SOAP-ENV:Envelope"] || {};
      var body = envelope["s:Body"] || envelope["SOAP-ENV:Body"] || {};
      var response = body["u:GetVolumeResponse"] || {};
      var vol = parseInt(response["CurrentVolume"], 10);
      if (isNaN(vol)) {
        throw new Error("GetVolume: could not parse CurrentVolume from: " + res.body.slice(0, 300));
      }
      return vol;
    }
    var AV_CONTROL_PATH = "/MediaRenderer/AVTransport/Control";
    var AV_SERVICE_TYPE = "urn:schemas-upnp-org:service:AVTransport:1";
    async function getTransportState2(device) {
      var body = '<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:GetTransportInfo xmlns:u="' + AV_SERVICE_TYPE + '"><InstanceID>0</InstanceID></u:GetTransportInfo></s:Body></s:Envelope>';
      var res = await httpRequest({
        hostname: device.ip,
        port: device.port,
        path: AV_CONTROL_PATH,
        method: "POST",
        headers: {
          "Content-Type": 'text/xml; charset="utf-8"',
          "SOAPACTION": '"' + AV_SERVICE_TYPE + '#GetTransportInfo"',
          "Content-Length": Buffer.byteLength(body, "utf8")
        }
      }, body);
      if (res.statusCode !== 200) return null;
      var m = res.body.match(/<CurrentTransportState>([A-Z_]+)<\/CurrentTransportState>/);
      return m ? m[1] : null;
    }
    module2.exports = {
      setVolume,
      getVolume: getVolume2,
      getTransportState: getTransportState2
    };
  }
});

// lib/parse.js
var require_parse = __commonJS({
  "lib/parse.js"(exports2, module2) {
    "use strict";
    var XMLParser = require_fxp().XMLParser;
    var parserOpts = {
      ignoreAttributes: false,
      attributeNamePrefix: "@_",
      parseAttributeValue: true,
      processEntities: true
      // decodes &lt; &gt; &amp; inside LastChange value
    };
    var parser = new XMLParser(parserOpts);
    function parseLastChange2(notifyBody) {
      var outer = parser.parse(notifyBody);
      var propertyset = outer["e:propertyset"] || outer["propertyset"] || {};
      var property = propertyset["e:property"] || propertyset["property"] || {};
      if (Array.isArray(property)) {
        var found = null;
        for (var i = 0; i < property.length; i++) {
          if (property[i]["LastChange"] !== void 0) {
            found = property[i];
            break;
          }
        }
        property = found || {};
      }
      var lastChange = property["LastChange"];
      if (lastChange === void 0 || lastChange === "") {
        return [];
      }
      var inner = parser.parse(String(lastChange));
      var Event2 = inner["Event"] || {};
      var instance = Event2["InstanceID"] || {};
      var entries = [];
      var keys = Object.keys(instance);
      for (var k = 0; k < keys.length; k++) {
        var key = keys[k];
        if (key[0] === "@") continue;
        var items = instance[key];
        if (!Array.isArray(items)) items = [items];
        for (var j = 0; j < items.length; j++) {
          var item = items[j];
          if (typeof item !== "object" || item === null) continue;
          entries.push({
            name: key,
            channel: item["@_channel"] !== void 0 ? String(item["@_channel"]) : null,
            val: item["@_val"] !== void 0 ? item["@_val"] : null
          });
        }
      }
      return entries;
    }
    module2.exports = { parseLastChange: parseLastChange2 };
  }
});

// lib/input.js
var require_input = __commonJS({
  "lib/input.js"(exports2, module2) {
    "use strict";
    var fs2 = require("fs");
    var EV_KEY = 1;
    var KEY_MUTE = 113;
    var KEY_VOLUMEDOWN = 114;
    var KEY_VOLUMEUP = 115;
    var STRUCT_SIZE = 16;
    function codeToDirection(code) {
      if (code === KEY_VOLUMEUP) return "up";
      if (code === KEY_VOLUMEDOWN) return "down";
      if (code === KEY_MUTE) return "mute";
      return null;
    }
    function detectInputDevice2() {
      return new Promise(function(resolve) {
        fs2.readFile("/proc/bus/input/devices", "utf8", function(err, data) {
          if (err) {
            console.warn("[input] cannot read /proc/bus/input/devices:", err.message);
            resolve({ path: null, candidates: [] });
            return;
          }
          var blocks = data.split(/\n\n+/);
          var candidates = [];
          for (var i = 0; i < blocks.length; i++) {
            var block = blocks[i];
            var hMatch = block.match(/H:\s*Handlers=([^\n]+)/);
            if (!hMatch) continue;
            var eventMatch = hMatch[1].match(/\b(event\d+)\b/);
            if (!eventMatch) continue;
            var eventPath = "/dev/input/" + eventMatch[1];
            var evMatch = block.match(/B:\s*EV=([0-9a-fA-F]+)/);
            if (!evMatch) continue;
            if (!(parseInt(evMatch[1], 16) & 2)) continue;
            var nMatch = block.match(/N:\s*Name="([^"]+)"/);
            var name = nMatch ? nMatch[1] : "";
            var hasVolKeys = false;
            var keyLine = block.match(/B:\s*KEY=([0-9a-fA-F ]+)/);
            if (keyLine) {
              var keyParts = keyLine[1].trim().split(" ");
              if (keyParts.length >= 2) {
                var padded = ("0000000000000000" + keyParts[keyParts.length - 2]).slice(-16);
                var upper32 = parseInt(padded.slice(0, 8), 16);
                hasVolKeys = (upper32 & 917504) !== 0;
              }
            }
            var priority = hasVolKeys ? 3 : /rcu|remote|magic/i.test(name) ? 2 : /keyboard|kbd/i.test(name) ? 1 : 0;
            candidates.push({ path: eventPath, name, priority, hasVolKeys });
          }
          candidates.sort(function(a, b) {
            if (b.priority !== a.priority) return b.priority - a.priority;
            return a.path.localeCompare(b.path);
          });
          resolve({
            path: candidates.length > 0 ? candidates[0].path : null,
            candidates
          });
        });
      });
    }
    function openInputDevice(devicePath, onEvent, onError) {
      var buf = Buffer.alloc(STRUCT_SIZE);
      var closed = false;
      var fd = null;
      var readCount = 0;
      fs2.open(devicePath, "r", function(err, openedFd) {
        if (err) {
          onError(err);
          return;
        }
        fd = openedFd;
        console.log("[input] opened", devicePath, "(fd=" + fd + ") \u2014 waiting for key events...");
        readNext();
      });
      function readNext() {
        if (closed) return;
        fs2.read(fd, buf, 0, STRUCT_SIZE, null, function(err, bytesRead) {
          if (closed) return;
          if (err) {
            onError(err);
            return;
          }
          var recvAt = process.hrtime();
          readCount++;
          if (bytesRead === 0) {
            console.warn("[input] read returned 0 bytes (unexpected EOF) after", readCount, "reads");
            readNext();
            return;
          }
          if (bytesRead !== STRUCT_SIZE) {
            console.warn("[input] partial read:", bytesRead, "bytes (expected", STRUCT_SIZE + ")");
            readNext();
            return;
          }
          var kernelSec = buf.readUInt32LE(0);
          var kernelUsec = buf.readUInt32LE(4);
          var type = buf.readUInt16LE(8);
          var code = buf.readUInt16LE(10);
          var value = buf.readInt32LE(12);
          if (type === EV_KEY && (value === 1 || value === 2)) {
            var direction = codeToDirection(code);
            if (direction !== null) {
              onEvent({
                direction,
                value,
                kernelSec,
                kernelUsec,
                recvAt
              });
            }
          }
          readNext();
        });
      }
      function close() {
        closed = true;
        if (fd !== null) {
          fs2.close(fd, function() {
          });
          fd = null;
        }
      }
      return { close };
    }
    function openInputDeviceViaChild(devicePath, onEvent, onError) {
      var spawn = require("child_process").spawn;
      var closed = false;
      var partial = null;
      var child = null;
      var restarts = 0;
      var everEmitted = false;
      var MAX_DEAD_RESTARTS = 5;
      function onData(chunk) {
        var recvAt = process.hrtime();
        everEmitted = true;
        restarts = 0;
        var data = partial ? Buffer.concat([partial, chunk]) : chunk;
        partial = null;
        var offset = 0;
        while (offset + STRUCT_SIZE <= data.length) {
          var kernelSec = data.readUInt32LE(offset + 0);
          var kernelUsec = data.readUInt32LE(offset + 4);
          var type = data.readUInt16LE(offset + 8);
          var code = data.readUInt16LE(offset + 10);
          var value = data.readInt32LE(offset + 12);
          offset += STRUCT_SIZE;
          if (type === EV_KEY && (value === 1 || value === 2)) {
            var direction = codeToDirection(code);
            if (direction !== null) {
              onEvent({
                direction,
                value,
                kernelSec,
                kernelUsec,
                recvAt
              });
            }
          }
        }
        if (offset < data.length) {
          partial = data.slice(offset);
        }
      }
      function start() {
        child = spawn("dd", ["if=" + devicePath, "bs=16"]);
        child.on("error", function(e) {
          if (!closed) onError(e);
        });
        child.on("exit", function(code) {
          if (closed) return;
          restarts += 1;
          if (!everEmitted && restarts > MAX_DEAD_RESTARTS) {
            if (restarts === MAX_DEAD_RESTARTS + 1) {
              console.warn(
                "[input] giving up on",
                devicePath,
                "\u2014 never produced an event in",
                restarts,
                "attempts"
              );
            }
            return;
          }
          var delay2 = Math.min(200 * restarts, 5e3);
          console.warn(
            "[input] reader for",
            devicePath,
            "exited with code",
            code,
            "\u2014 restarting in",
            delay2 + "ms (restart #" + restarts + ")"
          );
          setTimeout(function() {
            if (!closed) start();
          }, delay2);
        });
        child.stdout.on("data", onData);
      }
      start();
      function close() {
        closed = true;
        if (child) child.kill();
      }
      return { close };
    }
    function openInputDevicesMulti2(devicePaths, onEvent, onError) {
      var handles = [];
      devicePaths.forEach(function(devicePath) {
        var handle = openInputDeviceViaChild(devicePath, function(event) {
          event.sourceDev = devicePath;
          onEvent(event);
        }, function(err) {
          console.warn("[input] error on", devicePath + ":", err.message);
        });
        handles.push(handle);
      });
      return {
        close: function() {
          handles.forEach(function(h) {
            h.close();
          });
        }
      };
    }
    module2.exports = {
      openInputDevice,
      openInputDeviceViaChild,
      openInputDevicesMulti: openInputDevicesMulti2,
      detectInputDevice: detectInputDevice2
    };
  }
});

// lib/correlator.js
var require_correlator = __commonJS({
  "lib/correlator.js"(exports2, module2) {
    "use strict";
    var DEFAULT_WINDOW_MS = 3e3;
    function Correlator2(opts) {
      opts = opts || {};
      this.windowMs = opts.windowMs || DEFAULT_WINDOW_MS;
      this.pendingKeys = [];
      this.lastVol = null;
      this.lastMuted = null;
    }
    Correlator2.prototype.recordKeypress = function(direction, recvAt) {
      this.pendingKeys.push({ direction, recvAt });
      this._pruneOldKeys();
    };
    Correlator2.prototype.recordGena = function(vol, muted, recvAt) {
      this._pruneOldKeys();
      var direction = this._inferDirection(vol, muted);
      if (vol !== null) this.lastVol = vol;
      if (muted !== null) this.lastMuted = muted;
      if (direction === null) {
        console.log("[corr] GENA vol=" + vol + " muted=" + muted + " | direction unknown (no prior state or no delta)");
        return;
      }
      var matchIdx = -1;
      for (var i = 0; i < this.pendingKeys.length; i++) {
        if (this.pendingKeys[i].direction === direction) {
          matchIdx = i;
          break;
        }
      }
      if (matchIdx === -1) {
        console.log("[corr] GENA vol=" + vol + " dir=" + direction + " | no matching keypress \u2014 external or catch-up");
        return;
      }
      var key = this.pendingKeys.splice(matchIdx, 1)[0];
      var latencyMs = hrtimeDiffMs(key.recvAt, recvAt);
      var queueDepth = this.pendingKeys.length;
      console.log(
        "[corr]",
        "dir=" + direction,
        "| key\u2192gena=" + latencyMs.toFixed(2) + "ms",
        "| vol=" + vol,
        "| unmatched_keys=" + queueDepth + (queueDepth > 0 ? " \u26A0 hold-repeat lag" : "")
      );
    };
    Correlator2.prototype._inferDirection = function(vol, muted) {
      if (muted !== null && this.lastMuted !== null && muted !== this.lastMuted) {
        return "mute";
      }
      if (vol !== null && this.lastVol !== null) {
        if (vol > this.lastVol) return "up";
        if (vol < this.lastVol) return "down";
      }
      return null;
    };
    Correlator2.prototype._pruneOldKeys = function() {
      var windowMs = this.windowMs;
      var now = process.hrtime();
      var remaining = [];
      for (var i = 0; i < this.pendingKeys.length; i++) {
        var ageMs = hrtimeDiffMs(this.pendingKeys[i].recvAt, now);
        if (ageMs <= windowMs) {
          remaining.push(this.pendingKeys[i]);
        } else {
          console.warn("[corr] dropping unmatched keypress dir=" + this.pendingKeys[i].direction + " age=" + ageMs.toFixed(0) + "ms \u2014 no GENA event arrived");
        }
      }
      this.pendingKeys = remaining;
    };
    function hrtimeDiffMs(from, to) {
      var ns = (to[0] - from[0]) * 1e9 + (to[1] - from[1]);
      return ns / 1e6;
    }
    module2.exports = { Correlator: Correlator2 };
  }
});

// node_modules/ip/lib/ip.js
var require_ip = __commonJS({
  "node_modules/ip/lib/ip.js"(exports2) {
    var ip = exports2;
    var { Buffer: Buffer2 } = require("buffer");
    var os2 = require("os");
    ip.toBuffer = function(ip2, buff, offset) {
      offset = ~~offset;
      var result;
      if (this.isV4Format(ip2)) {
        result = buff || new Buffer2(offset + 4);
        ip2.split(/\./g).map((byte) => {
          result[offset++] = parseInt(byte, 10) & 255;
        });
      } else if (this.isV6Format(ip2)) {
        var sections = ip2.split(":", 8);
        var i;
        for (i = 0; i < sections.length; i++) {
          var isv4 = this.isV4Format(sections[i]);
          var v4Buffer;
          if (isv4) {
            v4Buffer = this.toBuffer(sections[i]);
            sections[i] = v4Buffer.slice(0, 2).toString("hex");
          }
          if (v4Buffer && ++i < 8) {
            sections.splice(i, 0, v4Buffer.slice(2, 4).toString("hex"));
          }
        }
        if (sections[0] === "") {
          while (sections.length < 8) sections.unshift("0");
        } else if (sections[sections.length - 1] === "") {
          while (sections.length < 8) sections.push("0");
        } else if (sections.length < 8) {
          for (i = 0; i < sections.length && sections[i] !== ""; i++) ;
          var argv = [i, 1];
          for (i = 9 - sections.length; i > 0; i--) {
            argv.push("0");
          }
          sections.splice.apply(sections, argv);
        }
        result = buff || new Buffer2(offset + 16);
        for (i = 0; i < sections.length; i++) {
          var word = parseInt(sections[i], 16);
          result[offset++] = word >> 8 & 255;
          result[offset++] = word & 255;
        }
      }
      if (!result) {
        throw Error("Invalid ip address: ".concat(ip2));
      }
      return result;
    };
    ip.toString = function(buff, offset, length) {
      offset = ~~offset;
      length = length || buff.length - offset;
      var result = [];
      var i;
      if (length === 4) {
        for (i = 0; i < length; i++) {
          result.push(buff[offset + i]);
        }
        result = result.join(".");
      } else if (length === 16) {
        for (i = 0; i < length; i += 2) {
          result.push(buff.readUInt16BE(offset + i).toString(16));
        }
        result = result.join(":");
        result = result.replace(/(^|:)0(:0)*:0(:|$)/, "$1::$3");
        result = result.replace(/:{3,4}/, "::");
      }
      return result;
    };
    var ipv4Regex = /^(\d{1,3}\.){3,3}\d{1,3}$/;
    var ipv6Regex = /^(::)?(((\d{1,3}\.){3}(\d{1,3}){1})?([0-9a-f]){0,4}:{0,2}){1,8}(::)?$/i;
    ip.isV4Format = function(ip2) {
      return ipv4Regex.test(ip2);
    };
    ip.isV6Format = function(ip2) {
      return ipv6Regex.test(ip2);
    };
    function _normalizeFamily(family) {
      if (family === 4) {
        return "ipv4";
      }
      if (family === 6) {
        return "ipv6";
      }
      return family ? family.toLowerCase() : "ipv4";
    }
    ip.fromPrefixLen = function(prefixlen, family) {
      if (prefixlen > 32) {
        family = "ipv6";
      } else {
        family = _normalizeFamily(family);
      }
      var len = 4;
      if (family === "ipv6") {
        len = 16;
      }
      var buff = new Buffer2(len);
      for (var i = 0, n = buff.length; i < n; ++i) {
        var bits = 8;
        if (prefixlen < 8) {
          bits = prefixlen;
        }
        prefixlen -= bits;
        buff[i] = ~(255 >> bits) & 255;
      }
      return ip.toString(buff);
    };
    ip.mask = function(addr, mask) {
      addr = ip.toBuffer(addr);
      mask = ip.toBuffer(mask);
      var result = new Buffer2(Math.max(addr.length, mask.length));
      var i;
      if (addr.length === mask.length) {
        for (i = 0; i < addr.length; i++) {
          result[i] = addr[i] & mask[i];
        }
      } else if (mask.length === 4) {
        for (i = 0; i < mask.length; i++) {
          result[i] = addr[addr.length - 4 + i] & mask[i];
        }
      } else {
        for (i = 0; i < result.length - 6; i++) {
          result[i] = 0;
        }
        result[10] = 255;
        result[11] = 255;
        for (i = 0; i < addr.length; i++) {
          result[i + 12] = addr[i] & mask[i + 12];
        }
        i += 12;
      }
      for (; i < result.length; i++) {
        result[i] = 0;
      }
      return ip.toString(result);
    };
    ip.cidr = function(cidrString) {
      var cidrParts = cidrString.split("/");
      var addr = cidrParts[0];
      if (cidrParts.length !== 2) {
        throw new Error("invalid CIDR subnet: ".concat(addr));
      }
      var mask = ip.fromPrefixLen(parseInt(cidrParts[1], 10));
      return ip.mask(addr, mask);
    };
    ip.subnet = function(addr, mask) {
      var networkAddress = ip.toLong(ip.mask(addr, mask));
      var maskBuffer = ip.toBuffer(mask);
      var maskLength = 0;
      for (var i = 0; i < maskBuffer.length; i++) {
        if (maskBuffer[i] === 255) {
          maskLength += 8;
        } else {
          var octet = maskBuffer[i] & 255;
          while (octet) {
            octet = octet << 1 & 255;
            maskLength++;
          }
        }
      }
      var numberOfAddresses = Math.pow(2, 32 - maskLength);
      return {
        networkAddress: ip.fromLong(networkAddress),
        firstAddress: numberOfAddresses <= 2 ? ip.fromLong(networkAddress) : ip.fromLong(networkAddress + 1),
        lastAddress: numberOfAddresses <= 2 ? ip.fromLong(networkAddress + numberOfAddresses - 1) : ip.fromLong(networkAddress + numberOfAddresses - 2),
        broadcastAddress: ip.fromLong(networkAddress + numberOfAddresses - 1),
        subnetMask: mask,
        subnetMaskLength: maskLength,
        numHosts: numberOfAddresses <= 2 ? numberOfAddresses : numberOfAddresses - 2,
        length: numberOfAddresses,
        contains(other) {
          return networkAddress === ip.toLong(ip.mask(other, mask));
        }
      };
    };
    ip.cidrSubnet = function(cidrString) {
      var cidrParts = cidrString.split("/");
      var addr = cidrParts[0];
      if (cidrParts.length !== 2) {
        throw new Error("invalid CIDR subnet: ".concat(addr));
      }
      var mask = ip.fromPrefixLen(parseInt(cidrParts[1], 10));
      return ip.subnet(addr, mask);
    };
    ip.not = function(addr) {
      var buff = ip.toBuffer(addr);
      for (var i = 0; i < buff.length; i++) {
        buff[i] = 255 ^ buff[i];
      }
      return ip.toString(buff);
    };
    ip.or = function(a, b) {
      var i;
      a = ip.toBuffer(a);
      b = ip.toBuffer(b);
      if (a.length === b.length) {
        for (i = 0; i < a.length; ++i) {
          a[i] |= b[i];
        }
        return ip.toString(a);
      }
      var buff = a;
      var other = b;
      if (b.length > a.length) {
        buff = b;
        other = a;
      }
      var offset = buff.length - other.length;
      for (i = offset; i < buff.length; ++i) {
        buff[i] |= other[i - offset];
      }
      return ip.toString(buff);
    };
    ip.isEqual = function(a, b) {
      var i;
      a = ip.toBuffer(a);
      b = ip.toBuffer(b);
      if (a.length === b.length) {
        for (i = 0; i < a.length; i++) {
          if (a[i] !== b[i]) return false;
        }
        return true;
      }
      if (b.length === 4) {
        var t = b;
        b = a;
        a = t;
      }
      for (i = 0; i < 10; i++) {
        if (b[i] !== 0) return false;
      }
      var word = b.readUInt16BE(10);
      if (word !== 0 && word !== 65535) return false;
      for (i = 0; i < 4; i++) {
        if (a[i] !== b[i + 12]) return false;
      }
      return true;
    };
    ip.isPrivate = function(addr) {
      if (ip.isLoopback(addr)) {
        return true;
      }
      if (!ip.isV6Format(addr)) {
        const ipl = ip.normalizeToLong(addr);
        if (ipl < 0) {
          throw new Error("invalid ipv4 address");
        }
        addr = ip.fromLong(ipl);
      }
      return /^(::f{4}:)?10\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$/i.test(addr) || /^(::f{4}:)?192\.168\.([0-9]{1,3})\.([0-9]{1,3})$/i.test(addr) || /^(::f{4}:)?172\.(1[6-9]|2\d|30|31)\.([0-9]{1,3})\.([0-9]{1,3})$/i.test(addr) || /^(::f{4}:)?169\.254\.([0-9]{1,3})\.([0-9]{1,3})$/i.test(addr) || /^f[cd][0-9a-f]{2}:/i.test(addr) || /^fe80:/i.test(addr) || /^::1$/.test(addr) || /^::$/.test(addr);
    };
    ip.isPublic = function(addr) {
      return !ip.isPrivate(addr);
    };
    ip.isLoopback = function(addr) {
      if (!/\./.test(addr) && !/:/.test(addr)) {
        addr = ip.fromLong(Number(addr));
      }
      return /^(::f{4}:)?127\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})/.test(addr) || /^0177\./.test(addr) || /^0x7f\./i.test(addr) || /^fe80::1$/i.test(addr) || /^::1$/.test(addr) || /^::$/.test(addr);
    };
    ip.loopback = function(family) {
      family = _normalizeFamily(family);
      if (family !== "ipv4" && family !== "ipv6") {
        throw new Error("family must be ipv4 or ipv6");
      }
      return family === "ipv4" ? "127.0.0.1" : "fe80::1";
    };
    ip.address = function(name, family) {
      var interfaces = os2.networkInterfaces();
      family = _normalizeFamily(family);
      if (name && name !== "private" && name !== "public") {
        var res = interfaces[name].filter((details) => {
          var itemFamily = _normalizeFamily(details.family);
          return itemFamily === family;
        });
        if (res.length === 0) {
          return void 0;
        }
        return res[0].address;
      }
      var all = Object.keys(interfaces).map((nic) => {
        var addresses = interfaces[nic].filter((details) => {
          details.family = _normalizeFamily(details.family);
          if (details.family !== family || ip.isLoopback(details.address)) {
            return false;
          }
          if (!name) {
            return true;
          }
          return name === "public" ? ip.isPrivate(details.address) : ip.isPublic(details.address);
        });
        return addresses.length ? addresses[0].address : void 0;
      }).filter(Boolean);
      return !all.length ? ip.loopback(family) : all[0];
    };
    ip.toLong = function(ip2) {
      var ipl = 0;
      ip2.split(".").forEach((octet) => {
        ipl <<= 8;
        ipl += parseInt(octet);
      });
      return ipl >>> 0;
    };
    ip.fromLong = function(ipl) {
      return "".concat(ipl >>> 24, ".").concat(ipl >> 16 & 255, ".").concat(ipl >> 8 & 255, ".").concat(ipl & 255);
    };
    ip.normalizeToLong = function(addr) {
      const parts = addr.split(".").map((part) => {
        if (part.startsWith("0x") || part.startsWith("0X")) {
          return parseInt(part, 16);
        } else if (part.startsWith("0") && part !== "0" && /^[0-7]+$/.test(part)) {
          return parseInt(part, 8);
        } else if (/^[1-9]\d*$/.test(part) || part === "0") {
          return parseInt(part, 10);
        } else {
          return NaN;
        }
      });
      if (parts.some(isNaN)) return -1;
      let val = 0;
      const n = parts.length;
      switch (n) {
        case 1:
          val = parts[0];
          break;
        case 2:
          if (parts[0] > 255 || parts[1] > 16777215) return -1;
          val = parts[0] << 24 | parts[1] & 16777215;
          break;
        case 3:
          if (parts[0] > 255 || parts[1] > 255 || parts[2] > 65535) return -1;
          val = parts[0] << 24 | parts[1] << 16 | parts[2] & 65535;
          break;
        case 4:
          if (parts.some((part) => part > 255)) return -1;
          val = parts[0] << 24 | parts[1] << 16 | parts[2] << 8 | parts[3];
          break;
        default:
          return -1;
      }
      return val >>> 0;
    };
  }
});

// node_modules/ms/index.js
var require_ms = __commonJS({
  "node_modules/ms/index.js"(exports2, module2) {
    var s = 1e3;
    var m = s * 60;
    var h = m * 60;
    var d = h * 24;
    var w = d * 7;
    var y = d * 365.25;
    module2.exports = function(val, options) {
      options = options || {};
      var type = typeof val;
      if (type === "string" && val.length > 0) {
        return parse(val);
      } else if (type === "number" && isFinite(val)) {
        return options.long ? fmtLong(val) : fmtShort(val);
      }
      throw new Error(
        "val is not a non-empty string or a valid number. val=" + JSON.stringify(val)
      );
    };
    function parse(str) {
      str = String(str);
      if (str.length > 100) {
        return;
      }
      var match = /^(-?(?:\d+)?\.?\d+) *(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)?$/i.exec(
        str
      );
      if (!match) {
        return;
      }
      var n = parseFloat(match[1]);
      var type = (match[2] || "ms").toLowerCase();
      switch (type) {
        case "years":
        case "year":
        case "yrs":
        case "yr":
        case "y":
          return n * y;
        case "weeks":
        case "week":
        case "w":
          return n * w;
        case "days":
        case "day":
        case "d":
          return n * d;
        case "hours":
        case "hour":
        case "hrs":
        case "hr":
        case "h":
          return n * h;
        case "minutes":
        case "minute":
        case "mins":
        case "min":
        case "m":
          return n * m;
        case "seconds":
        case "second":
        case "secs":
        case "sec":
        case "s":
          return n * s;
        case "milliseconds":
        case "millisecond":
        case "msecs":
        case "msec":
        case "ms":
          return n;
        default:
          return void 0;
      }
    }
    function fmtShort(ms) {
      var msAbs = Math.abs(ms);
      if (msAbs >= d) {
        return Math.round(ms / d) + "d";
      }
      if (msAbs >= h) {
        return Math.round(ms / h) + "h";
      }
      if (msAbs >= m) {
        return Math.round(ms / m) + "m";
      }
      if (msAbs >= s) {
        return Math.round(ms / s) + "s";
      }
      return ms + "ms";
    }
    function fmtLong(ms) {
      var msAbs = Math.abs(ms);
      if (msAbs >= d) {
        return plural(ms, msAbs, d, "day");
      }
      if (msAbs >= h) {
        return plural(ms, msAbs, h, "hour");
      }
      if (msAbs >= m) {
        return plural(ms, msAbs, m, "minute");
      }
      if (msAbs >= s) {
        return plural(ms, msAbs, s, "second");
      }
      return ms + " ms";
    }
    function plural(ms, msAbs, n, name) {
      var isPlural = msAbs >= n * 1.5;
      return Math.round(ms / n) + " " + name + (isPlural ? "s" : "");
    }
  }
});

// node_modules/debug/src/common.js
var require_common = __commonJS({
  "node_modules/debug/src/common.js"(exports2, module2) {
    "use strict";
    function setup(env2) {
      createDebug.debug = createDebug;
      createDebug.default = createDebug;
      createDebug.coerce = coerce;
      createDebug.disable = disable;
      createDebug.enable = enable;
      createDebug.enabled = enabled;
      createDebug.humanize = require_ms();
      Object.keys(env2).forEach(function(key) {
        createDebug[key] = env2[key];
      });
      createDebug.instances = [];
      createDebug.names = [];
      createDebug.skips = [];
      createDebug.formatters = {};
      function selectColor(namespace) {
        var hash = 0;
        for (var i = 0; i < namespace.length; i++) {
          hash = (hash << 5) - hash + namespace.charCodeAt(i);
          hash |= 0;
        }
        return createDebug.colors[Math.abs(hash) % createDebug.colors.length];
      }
      createDebug.selectColor = selectColor;
      function createDebug(namespace) {
        var prevTime;
        function debug() {
          if (!debug.enabled) {
            return;
          }
          for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
            args[_key] = arguments[_key];
          }
          var self2 = debug;
          var curr = Number(/* @__PURE__ */ new Date());
          var ms = curr - (prevTime || curr);
          self2.diff = ms;
          self2.prev = prevTime;
          self2.curr = curr;
          prevTime = curr;
          args[0] = createDebug.coerce(args[0]);
          if (typeof args[0] !== "string") {
            args.unshift("%O");
          }
          var index = 0;
          args[0] = args[0].replace(/%([a-zA-Z%])/g, function(match, format) {
            if (match === "%%") {
              return match;
            }
            index++;
            var formatter = createDebug.formatters[format];
            if (typeof formatter === "function") {
              var val = args[index];
              match = formatter.call(self2, val);
              args.splice(index, 1);
              index--;
            }
            return match;
          });
          createDebug.formatArgs.call(self2, args);
          var logFn = self2.log || createDebug.log;
          logFn.apply(self2, args);
        }
        debug.namespace = namespace;
        debug.enabled = createDebug.enabled(namespace);
        debug.useColors = createDebug.useColors();
        debug.color = selectColor(namespace);
        debug.destroy = destroy;
        debug.extend = extend;
        if (typeof createDebug.init === "function") {
          createDebug.init(debug);
        }
        createDebug.instances.push(debug);
        return debug;
      }
      function destroy() {
        var index = createDebug.instances.indexOf(this);
        if (index !== -1) {
          createDebug.instances.splice(index, 1);
          return true;
        }
        return false;
      }
      function extend(namespace, delimiter) {
        return createDebug(this.namespace + (typeof delimiter === "undefined" ? ":" : delimiter) + namespace);
      }
      function enable(namespaces) {
        createDebug.save(namespaces);
        createDebug.names = [];
        createDebug.skips = [];
        var i;
        var split = (typeof namespaces === "string" ? namespaces : "").split(/[\s,]+/);
        var len = split.length;
        for (i = 0; i < len; i++) {
          if (!split[i]) {
            continue;
          }
          namespaces = split[i].replace(/\*/g, ".*?");
          if (namespaces[0] === "-") {
            createDebug.skips.push(new RegExp("^" + namespaces.substr(1) + "$"));
          } else {
            createDebug.names.push(new RegExp("^" + namespaces + "$"));
          }
        }
        for (i = 0; i < createDebug.instances.length; i++) {
          var instance = createDebug.instances[i];
          instance.enabled = createDebug.enabled(instance.namespace);
        }
      }
      function disable() {
        createDebug.enable("");
      }
      function enabled(name) {
        if (name[name.length - 1] === "*") {
          return true;
        }
        var i;
        var len;
        for (i = 0, len = createDebug.skips.length; i < len; i++) {
          if (createDebug.skips[i].test(name)) {
            return false;
          }
        }
        for (i = 0, len = createDebug.names.length; i < len; i++) {
          if (createDebug.names[i].test(name)) {
            return true;
          }
        }
        return false;
      }
      function coerce(val) {
        if (val instanceof Error) {
          return val.stack || val.message;
        }
        return val;
      }
      createDebug.enable(createDebug.load());
      return createDebug;
    }
    module2.exports = setup;
  }
});

// node_modules/debug/src/browser.js
var require_browser = __commonJS({
  "node_modules/debug/src/browser.js"(exports2, module2) {
    "use strict";
    function _typeof(obj2) {
      if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") {
        _typeof = function _typeof2(obj3) {
          return typeof obj3;
        };
      } else {
        _typeof = function _typeof2(obj3) {
          return obj3 && typeof Symbol === "function" && obj3.constructor === Symbol && obj3 !== Symbol.prototype ? "symbol" : typeof obj3;
        };
      }
      return _typeof(obj2);
    }
    exports2.log = log;
    exports2.formatArgs = formatArgs;
    exports2.save = save;
    exports2.load = load;
    exports2.useColors = useColors;
    exports2.storage = localstorage();
    exports2.colors = ["#0000CC", "#0000FF", "#0033CC", "#0033FF", "#0066CC", "#0066FF", "#0099CC", "#0099FF", "#00CC00", "#00CC33", "#00CC66", "#00CC99", "#00CCCC", "#00CCFF", "#3300CC", "#3300FF", "#3333CC", "#3333FF", "#3366CC", "#3366FF", "#3399CC", "#3399FF", "#33CC00", "#33CC33", "#33CC66", "#33CC99", "#33CCCC", "#33CCFF", "#6600CC", "#6600FF", "#6633CC", "#6633FF", "#66CC00", "#66CC33", "#9900CC", "#9900FF", "#9933CC", "#9933FF", "#99CC00", "#99CC33", "#CC0000", "#CC0033", "#CC0066", "#CC0099", "#CC00CC", "#CC00FF", "#CC3300", "#CC3333", "#CC3366", "#CC3399", "#CC33CC", "#CC33FF", "#CC6600", "#CC6633", "#CC9900", "#CC9933", "#CCCC00", "#CCCC33", "#FF0000", "#FF0033", "#FF0066", "#FF0099", "#FF00CC", "#FF00FF", "#FF3300", "#FF3333", "#FF3366", "#FF3399", "#FF33CC", "#FF33FF", "#FF6600", "#FF6633", "#FF9900", "#FF9933", "#FFCC00", "#FFCC33"];
    function useColors() {
      if (typeof window !== "undefined" && window.process && (window.process.type === "renderer" || window.process.__nwjs)) {
        return true;
      }
      if (typeof navigator !== "undefined" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/(edge|trident)\/(\d+)/)) {
        return false;
      }
      return typeof document !== "undefined" && document.documentElement && document.documentElement.style && document.documentElement.style.WebkitAppearance || // Is firebug? http://stackoverflow.com/a/398120/376773
      typeof window !== "undefined" && window.console && (window.console.firebug || window.console.exception && window.console.table) || // Is firefox >= v31?
      // https://developer.mozilla.org/en-US/docs/Tools/Web_Console#Styling_messages
      typeof navigator !== "undefined" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/firefox\/(\d+)/) && parseInt(RegExp.$1, 10) >= 31 || // Double check webkit in userAgent just in case we are in a worker
      typeof navigator !== "undefined" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/applewebkit\/(\d+)/);
    }
    function formatArgs(args) {
      args[0] = (this.useColors ? "%c" : "") + this.namespace + (this.useColors ? " %c" : " ") + args[0] + (this.useColors ? "%c " : " ") + "+" + module2.exports.humanize(this.diff);
      if (!this.useColors) {
        return;
      }
      var c = "color: " + this.color;
      args.splice(1, 0, c, "color: inherit");
      var index = 0;
      var lastC = 0;
      args[0].replace(/%[a-zA-Z%]/g, function(match) {
        if (match === "%%") {
          return;
        }
        index++;
        if (match === "%c") {
          lastC = index;
        }
      });
      args.splice(lastC, 0, c);
    }
    function log() {
      var _console;
      return (typeof console === "undefined" ? "undefined" : _typeof(console)) === "object" && console.log && (_console = console).log.apply(_console, arguments);
    }
    function save(namespaces) {
      try {
        if (namespaces) {
          exports2.storage.setItem("debug", namespaces);
        } else {
          exports2.storage.removeItem("debug");
        }
      } catch (error) {
      }
    }
    function load() {
      var r;
      try {
        r = exports2.storage.getItem("debug");
      } catch (error) {
      }
      if (!r && typeof process !== "undefined" && "env" in process) {
        r = process.env.DEBUG;
      }
      return r;
    }
    function localstorage() {
      try {
        return localStorage;
      } catch (error) {
      }
    }
    module2.exports = require_common()(exports2);
    var formatters = module2.exports.formatters;
    formatters.j = function(v) {
      try {
        return JSON.stringify(v);
      } catch (error) {
        return "[UnexpectedJSONParseError]: " + error.message;
      }
    };
  }
});

// node_modules/debug/src/node.js
var require_node = __commonJS({
  "node_modules/debug/src/node.js"(exports2, module2) {
    "use strict";
    var tty = require("tty");
    var util = require("util");
    exports2.init = init;
    exports2.log = log;
    exports2.formatArgs = formatArgs;
    exports2.save = save;
    exports2.load = load;
    exports2.useColors = useColors;
    exports2.colors = [6, 2, 3, 4, 5, 1];
    try {
      supportsColor = require("supports-color");
      if (supportsColor && (supportsColor.stderr || supportsColor).level >= 2) {
        exports2.colors = [20, 21, 26, 27, 32, 33, 38, 39, 40, 41, 42, 43, 44, 45, 56, 57, 62, 63, 68, 69, 74, 75, 76, 77, 78, 79, 80, 81, 92, 93, 98, 99, 112, 113, 128, 129, 134, 135, 148, 149, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 178, 179, 184, 185, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 214, 215, 220, 221];
      }
    } catch (error) {
    }
    var supportsColor;
    exports2.inspectOpts = Object.keys(process.env).filter(function(key) {
      return /^debug_/i.test(key);
    }).reduce(function(obj2, key) {
      var prop = key.substring(6).toLowerCase().replace(/_([a-z])/g, function(_, k) {
        return k.toUpperCase();
      });
      var val = process.env[key];
      if (/^(yes|on|true|enabled)$/i.test(val)) {
        val = true;
      } else if (/^(no|off|false|disabled)$/i.test(val)) {
        val = false;
      } else if (val === "null") {
        val = null;
      } else {
        val = Number(val);
      }
      obj2[prop] = val;
      return obj2;
    }, {});
    function useColors() {
      return "colors" in exports2.inspectOpts ? Boolean(exports2.inspectOpts.colors) : tty.isatty(process.stderr.fd);
    }
    function formatArgs(args) {
      var name = this.namespace, useColors2 = this.useColors;
      if (useColors2) {
        var c = this.color;
        var colorCode = "\x1B[3" + (c < 8 ? c : "8;5;" + c);
        var prefix = "  ".concat(colorCode, ";1m").concat(name, " \x1B[0m");
        args[0] = prefix + args[0].split("\n").join("\n" + prefix);
        args.push(colorCode + "m+" + module2.exports.humanize(this.diff) + "\x1B[0m");
      } else {
        args[0] = getDate() + name + " " + args[0];
      }
    }
    function getDate() {
      if (exports2.inspectOpts.hideDate) {
        return "";
      }
      return (/* @__PURE__ */ new Date()).toISOString() + " ";
    }
    function log() {
      return process.stderr.write(util.format.apply(util, arguments) + "\n");
    }
    function save(namespaces) {
      if (namespaces) {
        process.env.DEBUG = namespaces;
      } else {
        delete process.env.DEBUG;
      }
    }
    function load() {
      return process.env.DEBUG;
    }
    function init(debug) {
      debug.inspectOpts = {};
      var keys = Object.keys(exports2.inspectOpts);
      for (var i = 0; i < keys.length; i++) {
        debug.inspectOpts[keys[i]] = exports2.inspectOpts[keys[i]];
      }
    }
    module2.exports = require_common()(exports2);
    var formatters = module2.exports.formatters;
    formatters.o = function(v) {
      this.inspectOpts.colors = this.useColors;
      return util.inspect(v, this.inspectOpts).split("\n").map(function(str) {
        return str.trim();
      }).join(" ");
    };
    formatters.O = function(v) {
      this.inspectOpts.colors = this.useColors;
      return util.inspect(v, this.inspectOpts);
    };
  }
});

// node_modules/debug/src/index.js
var require_src = __commonJS({
  "node_modules/debug/src/index.js"(exports2, module2) {
    "use strict";
    if (typeof process === "undefined" || process.type === "renderer" || process.browser === true || process.__nwjs) {
      module2.exports = require_browser();
    } else {
      module2.exports = require_node();
    }
  }
});

// node_modules/async/dist/async.js
var require_async = __commonJS({
  "node_modules/async/dist/async.js"(exports2, module2) {
    (function(global2, factory) {
      typeof exports2 === "object" && typeof module2 !== "undefined" ? factory(exports2) : typeof define === "function" && define.amd ? define(["exports"], factory) : factory(global2.async = global2.async || {});
    })(exports2, (function(exports3) {
      "use strict";
      function slice(arrayLike, start) {
        start = start | 0;
        var newLen = Math.max(arrayLike.length - start, 0);
        var newArr = Array(newLen);
        for (var idx = 0; idx < newLen; idx++) {
          newArr[idx] = arrayLike[start + idx];
        }
        return newArr;
      }
      var apply = function(fn) {
        var args = slice(arguments, 1);
        return function() {
          var callArgs = slice(arguments);
          return fn.apply(null, args.concat(callArgs));
        };
      };
      var initialParams = function(fn) {
        return function() {
          var args = slice(arguments);
          var callback = args.pop();
          fn.call(this, args, callback);
        };
      };
      function isObject2(value) {
        var type = typeof value;
        return value != null && (type == "object" || type == "function");
      }
      var hasSetImmediate = typeof setImmediate === "function" && setImmediate;
      var hasNextTick = typeof process === "object" && typeof process.nextTick === "function";
      function fallback(fn) {
        setTimeout(fn, 0);
      }
      function wrap(defer) {
        return function(fn) {
          var args = slice(arguments, 1);
          defer(function() {
            fn.apply(null, args);
          });
        };
      }
      var _defer;
      if (hasSetImmediate) {
        _defer = setImmediate;
      } else if (hasNextTick) {
        _defer = process.nextTick;
      } else {
        _defer = fallback;
      }
      var setImmediate$1 = wrap(_defer);
      function asyncify(func) {
        return initialParams(function(args, callback) {
          var result;
          try {
            result = func.apply(this, args);
          } catch (e) {
            return callback(e);
          }
          if (isObject2(result) && typeof result.then === "function") {
            result.then(function(value) {
              invokeCallback(callback, null, value);
            }, function(err) {
              invokeCallback(callback, err.message ? err : new Error(err));
            });
          } else {
            callback(null, result);
          }
        });
      }
      function invokeCallback(callback, error, value) {
        try {
          callback(error, value);
        } catch (e) {
          setImmediate$1(rethrow, e);
        }
      }
      function rethrow(error) {
        throw error;
      }
      var supportsSymbol = typeof Symbol === "function";
      function isAsync(fn) {
        return supportsSymbol && fn[Symbol.toStringTag] === "AsyncFunction";
      }
      function wrapAsync(asyncFn) {
        return isAsync(asyncFn) ? asyncify(asyncFn) : asyncFn;
      }
      function applyEach$1(eachfn) {
        return function(fns) {
          var args = slice(arguments, 1);
          var go = initialParams(function(args2, callback) {
            var that = this;
            return eachfn(fns, function(fn, cb) {
              wrapAsync(fn).apply(that, args2.concat(cb));
            }, callback);
          });
          if (args.length) {
            return go.apply(this, args);
          } else {
            return go;
          }
        };
      }
      var freeGlobal = typeof global == "object" && global && global.Object === Object && global;
      var freeSelf = typeof self == "object" && self && self.Object === Object && self;
      var root = freeGlobal || freeSelf || Function("return this")();
      var Symbol$1 = root.Symbol;
      var objectProto = Object.prototype;
      var hasOwnProperty = objectProto.hasOwnProperty;
      var nativeObjectToString = objectProto.toString;
      var symToStringTag$1 = Symbol$1 ? Symbol$1.toStringTag : void 0;
      function getRawTag(value) {
        var isOwn = hasOwnProperty.call(value, symToStringTag$1), tag = value[symToStringTag$1];
        try {
          value[symToStringTag$1] = void 0;
          var unmasked = true;
        } catch (e) {
        }
        var result = nativeObjectToString.call(value);
        if (unmasked) {
          if (isOwn) {
            value[symToStringTag$1] = tag;
          } else {
            delete value[symToStringTag$1];
          }
        }
        return result;
      }
      var objectProto$1 = Object.prototype;
      var nativeObjectToString$1 = objectProto$1.toString;
      function objectToString(value) {
        return nativeObjectToString$1.call(value);
      }
      var nullTag = "[object Null]";
      var undefinedTag = "[object Undefined]";
      var symToStringTag = Symbol$1 ? Symbol$1.toStringTag : void 0;
      function baseGetTag(value) {
        if (value == null) {
          return value === void 0 ? undefinedTag : nullTag;
        }
        return symToStringTag && symToStringTag in Object(value) ? getRawTag(value) : objectToString(value);
      }
      var asyncTag = "[object AsyncFunction]";
      var funcTag = "[object Function]";
      var genTag = "[object GeneratorFunction]";
      var proxyTag = "[object Proxy]";
      function isFunction(value) {
        if (!isObject2(value)) {
          return false;
        }
        var tag = baseGetTag(value);
        return tag == funcTag || tag == genTag || tag == asyncTag || tag == proxyTag;
      }
      var MAX_SAFE_INTEGER = 9007199254740991;
      function isLength(value) {
        return typeof value == "number" && value > -1 && value % 1 == 0 && value <= MAX_SAFE_INTEGER;
      }
      function isArrayLike(value) {
        return value != null && isLength(value.length) && !isFunction(value);
      }
      var breakLoop = {};
      function noop() {
      }
      function once(fn) {
        return function() {
          if (fn === null) return;
          var callFn = fn;
          fn = null;
          callFn.apply(this, arguments);
        };
      }
      var iteratorSymbol = typeof Symbol === "function" && Symbol.iterator;
      var getIterator = function(coll) {
        return iteratorSymbol && coll[iteratorSymbol] && coll[iteratorSymbol]();
      };
      function baseTimes(n, iteratee) {
        var index2 = -1, result = Array(n);
        while (++index2 < n) {
          result[index2] = iteratee(index2);
        }
        return result;
      }
      function isObjectLike(value) {
        return value != null && typeof value == "object";
      }
      var argsTag = "[object Arguments]";
      function baseIsArguments(value) {
        return isObjectLike(value) && baseGetTag(value) == argsTag;
      }
      var objectProto$3 = Object.prototype;
      var hasOwnProperty$2 = objectProto$3.hasOwnProperty;
      var propertyIsEnumerable = objectProto$3.propertyIsEnumerable;
      var isArguments = baseIsArguments(/* @__PURE__ */ (function() {
        return arguments;
      })()) ? baseIsArguments : function(value) {
        return isObjectLike(value) && hasOwnProperty$2.call(value, "callee") && !propertyIsEnumerable.call(value, "callee");
      };
      var isArray = Array.isArray;
      function stubFalse() {
        return false;
      }
      var freeExports = typeof exports3 == "object" && exports3 && !exports3.nodeType && exports3;
      var freeModule = freeExports && typeof module2 == "object" && module2 && !module2.nodeType && module2;
      var moduleExports = freeModule && freeModule.exports === freeExports;
      var Buffer2 = moduleExports ? root.Buffer : void 0;
      var nativeIsBuffer = Buffer2 ? Buffer2.isBuffer : void 0;
      var isBuffer = nativeIsBuffer || stubFalse;
      var MAX_SAFE_INTEGER$1 = 9007199254740991;
      var reIsUint = /^(?:0|[1-9]\d*)$/;
      function isIndex(value, length) {
        var type = typeof value;
        length = length == null ? MAX_SAFE_INTEGER$1 : length;
        return !!length && (type == "number" || type != "symbol" && reIsUint.test(value)) && (value > -1 && value % 1 == 0 && value < length);
      }
      var argsTag$1 = "[object Arguments]";
      var arrayTag = "[object Array]";
      var boolTag = "[object Boolean]";
      var dateTag = "[object Date]";
      var errorTag = "[object Error]";
      var funcTag$1 = "[object Function]";
      var mapTag = "[object Map]";
      var numberTag = "[object Number]";
      var objectTag = "[object Object]";
      var regexpTag = "[object RegExp]";
      var setTag = "[object Set]";
      var stringTag = "[object String]";
      var weakMapTag = "[object WeakMap]";
      var arrayBufferTag = "[object ArrayBuffer]";
      var dataViewTag = "[object DataView]";
      var float32Tag = "[object Float32Array]";
      var float64Tag = "[object Float64Array]";
      var int8Tag = "[object Int8Array]";
      var int16Tag = "[object Int16Array]";
      var int32Tag = "[object Int32Array]";
      var uint8Tag = "[object Uint8Array]";
      var uint8ClampedTag = "[object Uint8ClampedArray]";
      var uint16Tag = "[object Uint16Array]";
      var uint32Tag = "[object Uint32Array]";
      var typedArrayTags = {};
      typedArrayTags[float32Tag] = typedArrayTags[float64Tag] = typedArrayTags[int8Tag] = typedArrayTags[int16Tag] = typedArrayTags[int32Tag] = typedArrayTags[uint8Tag] = typedArrayTags[uint8ClampedTag] = typedArrayTags[uint16Tag] = typedArrayTags[uint32Tag] = true;
      typedArrayTags[argsTag$1] = typedArrayTags[arrayTag] = typedArrayTags[arrayBufferTag] = typedArrayTags[boolTag] = typedArrayTags[dataViewTag] = typedArrayTags[dateTag] = typedArrayTags[errorTag] = typedArrayTags[funcTag$1] = typedArrayTags[mapTag] = typedArrayTags[numberTag] = typedArrayTags[objectTag] = typedArrayTags[regexpTag] = typedArrayTags[setTag] = typedArrayTags[stringTag] = typedArrayTags[weakMapTag] = false;
      function baseIsTypedArray(value) {
        return isObjectLike(value) && isLength(value.length) && !!typedArrayTags[baseGetTag(value)];
      }
      function baseUnary(func) {
        return function(value) {
          return func(value);
        };
      }
      var freeExports$1 = typeof exports3 == "object" && exports3 && !exports3.nodeType && exports3;
      var freeModule$1 = freeExports$1 && typeof module2 == "object" && module2 && !module2.nodeType && module2;
      var moduleExports$1 = freeModule$1 && freeModule$1.exports === freeExports$1;
      var freeProcess = moduleExports$1 && freeGlobal.process;
      var nodeUtil = (function() {
        try {
          var types = freeModule$1 && freeModule$1.require && freeModule$1.require("util").types;
          if (types) {
            return types;
          }
          return freeProcess && freeProcess.binding && freeProcess.binding("util");
        } catch (e) {
        }
      })();
      var nodeIsTypedArray = nodeUtil && nodeUtil.isTypedArray;
      var isTypedArray = nodeIsTypedArray ? baseUnary(nodeIsTypedArray) : baseIsTypedArray;
      var objectProto$2 = Object.prototype;
      var hasOwnProperty$1 = objectProto$2.hasOwnProperty;
      function arrayLikeKeys(value, inherited) {
        var isArr = isArray(value), isArg = !isArr && isArguments(value), isBuff = !isArr && !isArg && isBuffer(value), isType = !isArr && !isArg && !isBuff && isTypedArray(value), skipIndexes = isArr || isArg || isBuff || isType, result = skipIndexes ? baseTimes(value.length, String) : [], length = result.length;
        for (var key in value) {
          if ((inherited || hasOwnProperty$1.call(value, key)) && !(skipIndexes && // Safari 9 has enumerable `arguments.length` in strict mode.
          (key == "length" || // Node.js 0.10 has enumerable non-index properties on buffers.
          isBuff && (key == "offset" || key == "parent") || // PhantomJS 2 has enumerable non-index properties on typed arrays.
          isType && (key == "buffer" || key == "byteLength" || key == "byteOffset") || // Skip index properties.
          isIndex(key, length)))) {
            result.push(key);
          }
        }
        return result;
      }
      var objectProto$5 = Object.prototype;
      function isPrototype(value) {
        var Ctor = value && value.constructor, proto = typeof Ctor == "function" && Ctor.prototype || objectProto$5;
        return value === proto;
      }
      function overArg(func, transform2) {
        return function(arg) {
          return func(transform2(arg));
        };
      }
      var nativeKeys = overArg(Object.keys, Object);
      var objectProto$4 = Object.prototype;
      var hasOwnProperty$3 = objectProto$4.hasOwnProperty;
      function baseKeys(object) {
        if (!isPrototype(object)) {
          return nativeKeys(object);
        }
        var result = [];
        for (var key in Object(object)) {
          if (hasOwnProperty$3.call(object, key) && key != "constructor") {
            result.push(key);
          }
        }
        return result;
      }
      function keys(object) {
        return isArrayLike(object) ? arrayLikeKeys(object) : baseKeys(object);
      }
      function createArrayIterator(coll) {
        var i = -1;
        var len = coll.length;
        return function next() {
          return ++i < len ? { value: coll[i], key: i } : null;
        };
      }
      function createES2015Iterator(iterator2) {
        var i = -1;
        return function next() {
          var item = iterator2.next();
          if (item.done)
            return null;
          i++;
          return { value: item.value, key: i };
        };
      }
      function createObjectIterator(obj2) {
        var okeys = keys(obj2);
        var i = -1;
        var len = okeys.length;
        return function next() {
          var key = okeys[++i];
          if (key === "__proto__") {
            return next();
          }
          return i < len ? { value: obj2[key], key } : null;
        };
      }
      function iterator(coll) {
        if (isArrayLike(coll)) {
          return createArrayIterator(coll);
        }
        var iterator2 = getIterator(coll);
        return iterator2 ? createES2015Iterator(iterator2) : createObjectIterator(coll);
      }
      function onlyOnce(fn) {
        return function() {
          if (fn === null) throw new Error("Callback was already called.");
          var callFn = fn;
          fn = null;
          callFn.apply(this, arguments);
        };
      }
      function _eachOfLimit(limit) {
        return function(obj2, iteratee, callback) {
          callback = once(callback || noop);
          if (limit <= 0 || !obj2) {
            return callback(null);
          }
          var nextElem = iterator(obj2);
          var done = false;
          var running = 0;
          var looping = false;
          function iterateeCallback(err, value) {
            running -= 1;
            if (err) {
              done = true;
              callback(err);
            } else if (value === breakLoop || done && running <= 0) {
              done = true;
              return callback(null);
            } else if (!looping) {
              replenish();
            }
          }
          function replenish() {
            looping = true;
            while (running < limit && !done) {
              var elem = nextElem();
              if (elem === null) {
                done = true;
                if (running <= 0) {
                  callback(null);
                }
                return;
              }
              running += 1;
              iteratee(elem.value, elem.key, onlyOnce(iterateeCallback));
            }
            looping = false;
          }
          replenish();
        };
      }
      function eachOfLimit(coll, limit, iteratee, callback) {
        _eachOfLimit(limit)(coll, wrapAsync(iteratee), callback);
      }
      function doLimit(fn, limit) {
        return function(iterable, iteratee, callback) {
          return fn(iterable, limit, iteratee, callback);
        };
      }
      function eachOfArrayLike(coll, iteratee, callback) {
        callback = once(callback || noop);
        var index2 = 0, completed = 0, length = coll.length;
        if (length === 0) {
          callback(null);
        }
        function iteratorCallback(err, value) {
          if (err) {
            callback(err);
          } else if (++completed === length || value === breakLoop) {
            callback(null);
          }
        }
        for (; index2 < length; index2++) {
          iteratee(coll[index2], index2, onlyOnce(iteratorCallback));
        }
      }
      var eachOfGeneric = doLimit(eachOfLimit, Infinity);
      var eachOf = function(coll, iteratee, callback) {
        var eachOfImplementation = isArrayLike(coll) ? eachOfArrayLike : eachOfGeneric;
        eachOfImplementation(coll, wrapAsync(iteratee), callback);
      };
      function doParallel(fn) {
        return function(obj2, iteratee, callback) {
          return fn(eachOf, obj2, wrapAsync(iteratee), callback);
        };
      }
      function _asyncMap(eachfn, arr, iteratee, callback) {
        callback = callback || noop;
        arr = arr || [];
        var results = [];
        var counter = 0;
        var _iteratee = wrapAsync(iteratee);
        eachfn(arr, function(value, _, callback2) {
          var index2 = counter++;
          _iteratee(value, function(err, v) {
            results[index2] = v;
            callback2(err);
          });
        }, function(err) {
          callback(err, results);
        });
      }
      var map = doParallel(_asyncMap);
      var applyEach = applyEach$1(map);
      function doParallelLimit(fn) {
        return function(obj2, limit, iteratee, callback) {
          return fn(_eachOfLimit(limit), obj2, wrapAsync(iteratee), callback);
        };
      }
      var mapLimit = doParallelLimit(_asyncMap);
      var mapSeries = doLimit(mapLimit, 1);
      var applyEachSeries = applyEach$1(mapSeries);
      function arrayEach(array, iteratee) {
        var index2 = -1, length = array == null ? 0 : array.length;
        while (++index2 < length) {
          if (iteratee(array[index2], index2, array) === false) {
            break;
          }
        }
        return array;
      }
      function createBaseFor(fromRight) {
        return function(object, iteratee, keysFunc) {
          var index2 = -1, iterable = Object(object), props = keysFunc(object), length = props.length;
          while (length--) {
            var key = props[fromRight ? length : ++index2];
            if (iteratee(iterable[key], key, iterable) === false) {
              break;
            }
          }
          return object;
        };
      }
      var baseFor = createBaseFor();
      function baseForOwn(object, iteratee) {
        return object && baseFor(object, iteratee, keys);
      }
      function baseFindIndex(array, predicate, fromIndex, fromRight) {
        var length = array.length, index2 = fromIndex + (fromRight ? 1 : -1);
        while (fromRight ? index2-- : ++index2 < length) {
          if (predicate(array[index2], index2, array)) {
            return index2;
          }
        }
        return -1;
      }
      function baseIsNaN(value) {
        return value !== value;
      }
      function strictIndexOf(array, value, fromIndex) {
        var index2 = fromIndex - 1, length = array.length;
        while (++index2 < length) {
          if (array[index2] === value) {
            return index2;
          }
        }
        return -1;
      }
      function baseIndexOf(array, value, fromIndex) {
        return value === value ? strictIndexOf(array, value, fromIndex) : baseFindIndex(array, baseIsNaN, fromIndex);
      }
      var auto = function(tasks, concurrency, callback) {
        if (typeof concurrency === "function") {
          callback = concurrency;
          concurrency = null;
        }
        callback = once(callback || noop);
        var keys$$1 = keys(tasks);
        var numTasks = keys$$1.length;
        if (!numTasks) {
          return callback(null);
        }
        if (!concurrency) {
          concurrency = numTasks;
        }
        var results = {};
        var runningTasks = 0;
        var hasError = false;
        var listeners = /* @__PURE__ */ Object.create(null);
        var readyTasks = [];
        var readyToCheck = [];
        var uncheckedDependencies = {};
        baseForOwn(tasks, function(task, key) {
          if (!isArray(task)) {
            enqueueTask(key, [task]);
            readyToCheck.push(key);
            return;
          }
          var dependencies = task.slice(0, task.length - 1);
          var remainingDependencies = dependencies.length;
          if (remainingDependencies === 0) {
            enqueueTask(key, task);
            readyToCheck.push(key);
            return;
          }
          uncheckedDependencies[key] = remainingDependencies;
          arrayEach(dependencies, function(dependencyName) {
            if (!tasks[dependencyName]) {
              throw new Error("async.auto task `" + key + "` has a non-existent dependency `" + dependencyName + "` in " + dependencies.join(", "));
            }
            addListener(dependencyName, function() {
              remainingDependencies--;
              if (remainingDependencies === 0) {
                enqueueTask(key, task);
              }
            });
          });
        });
        checkForDeadlocks();
        processQueue();
        function enqueueTask(key, task) {
          readyTasks.push(function() {
            runTask(key, task);
          });
        }
        function processQueue() {
          if (readyTasks.length === 0 && runningTasks === 0) {
            return callback(null, results);
          }
          while (readyTasks.length && runningTasks < concurrency) {
            var run = readyTasks.shift();
            run();
          }
        }
        function addListener(taskName, fn) {
          var taskListeners = listeners[taskName];
          if (!taskListeners) {
            taskListeners = listeners[taskName] = [];
          }
          taskListeners.push(fn);
        }
        function taskComplete(taskName) {
          var taskListeners = listeners[taskName] || [];
          arrayEach(taskListeners, function(fn) {
            fn();
          });
          processQueue();
        }
        function runTask(key, task) {
          if (hasError) return;
          var taskCallback = onlyOnce(function(err, result) {
            runningTasks--;
            if (arguments.length > 2) {
              result = slice(arguments, 1);
            }
            if (err) {
              var safeResults = {};
              baseForOwn(results, function(val, rkey) {
                safeResults[rkey] = val;
              });
              safeResults[key] = result;
              hasError = true;
              listeners = /* @__PURE__ */ Object.create(null);
              callback(err, safeResults);
            } else {
              results[key] = result;
              taskComplete(key);
            }
          });
          runningTasks++;
          var taskFn = wrapAsync(task[task.length - 1]);
          if (task.length > 1) {
            taskFn(results, taskCallback);
          } else {
            taskFn(taskCallback);
          }
        }
        function checkForDeadlocks() {
          var currentTask;
          var counter = 0;
          while (readyToCheck.length) {
            currentTask = readyToCheck.pop();
            counter++;
            arrayEach(getDependents(currentTask), function(dependent) {
              if (--uncheckedDependencies[dependent] === 0) {
                readyToCheck.push(dependent);
              }
            });
          }
          if (counter !== numTasks) {
            throw new Error(
              "async.auto cannot execute tasks due to a recursive dependency"
            );
          }
        }
        function getDependents(taskName) {
          var result = [];
          baseForOwn(tasks, function(task, key) {
            if (isArray(task) && baseIndexOf(task, taskName, 0) >= 0) {
              result.push(key);
            }
          });
          return result;
        }
      };
      function arrayMap(array, iteratee) {
        var index2 = -1, length = array == null ? 0 : array.length, result = Array(length);
        while (++index2 < length) {
          result[index2] = iteratee(array[index2], index2, array);
        }
        return result;
      }
      var symbolTag = "[object Symbol]";
      function isSymbol(value) {
        return typeof value == "symbol" || isObjectLike(value) && baseGetTag(value) == symbolTag;
      }
      var INFINITY = 1 / 0;
      var symbolProto = Symbol$1 ? Symbol$1.prototype : void 0;
      var symbolToString = symbolProto ? symbolProto.toString : void 0;
      function baseToString(value) {
        if (typeof value == "string") {
          return value;
        }
        if (isArray(value)) {
          return arrayMap(value, baseToString) + "";
        }
        if (isSymbol(value)) {
          return symbolToString ? symbolToString.call(value) : "";
        }
        var result = value + "";
        return result == "0" && 1 / value == -INFINITY ? "-0" : result;
      }
      function baseSlice(array, start, end) {
        var index2 = -1, length = array.length;
        if (start < 0) {
          start = -start > length ? 0 : length + start;
        }
        end = end > length ? length : end;
        if (end < 0) {
          end += length;
        }
        length = start > end ? 0 : end - start >>> 0;
        start >>>= 0;
        var result = Array(length);
        while (++index2 < length) {
          result[index2] = array[index2 + start];
        }
        return result;
      }
      function castSlice(array, start, end) {
        var length = array.length;
        end = end === void 0 ? length : end;
        return !start && end >= length ? array : baseSlice(array, start, end);
      }
      function charsEndIndex(strSymbols, chrSymbols) {
        var index2 = strSymbols.length;
        while (index2-- && baseIndexOf(chrSymbols, strSymbols[index2], 0) > -1) {
        }
        return index2;
      }
      function charsStartIndex(strSymbols, chrSymbols) {
        var index2 = -1, length = strSymbols.length;
        while (++index2 < length && baseIndexOf(chrSymbols, strSymbols[index2], 0) > -1) {
        }
        return index2;
      }
      function asciiToArray(string) {
        return string.split("");
      }
      var rsAstralRange = "\\ud800-\\udfff";
      var rsComboMarksRange = "\\u0300-\\u036f";
      var reComboHalfMarksRange = "\\ufe20-\\ufe2f";
      var rsComboSymbolsRange = "\\u20d0-\\u20ff";
      var rsComboRange = rsComboMarksRange + reComboHalfMarksRange + rsComboSymbolsRange;
      var rsVarRange = "\\ufe0e\\ufe0f";
      var rsZWJ = "\\u200d";
      var reHasUnicode = RegExp("[" + rsZWJ + rsAstralRange + rsComboRange + rsVarRange + "]");
      function hasUnicode(string) {
        return reHasUnicode.test(string);
      }
      var rsAstralRange$1 = "\\ud800-\\udfff";
      var rsComboMarksRange$1 = "\\u0300-\\u036f";
      var reComboHalfMarksRange$1 = "\\ufe20-\\ufe2f";
      var rsComboSymbolsRange$1 = "\\u20d0-\\u20ff";
      var rsComboRange$1 = rsComboMarksRange$1 + reComboHalfMarksRange$1 + rsComboSymbolsRange$1;
      var rsVarRange$1 = "\\ufe0e\\ufe0f";
      var rsAstral = "[" + rsAstralRange$1 + "]";
      var rsCombo = "[" + rsComboRange$1 + "]";
      var rsFitz = "\\ud83c[\\udffb-\\udfff]";
      var rsModifier = "(?:" + rsCombo + "|" + rsFitz + ")";
      var rsNonAstral = "[^" + rsAstralRange$1 + "]";
      var rsRegional = "(?:\\ud83c[\\udde6-\\uddff]){2}";
      var rsSurrPair = "[\\ud800-\\udbff][\\udc00-\\udfff]";
      var rsZWJ$1 = "\\u200d";
      var reOptMod = rsModifier + "?";
      var rsOptVar = "[" + rsVarRange$1 + "]?";
      var rsOptJoin = "(?:" + rsZWJ$1 + "(?:" + [rsNonAstral, rsRegional, rsSurrPair].join("|") + ")" + rsOptVar + reOptMod + ")*";
      var rsSeq = rsOptVar + reOptMod + rsOptJoin;
      var rsSymbol = "(?:" + [rsNonAstral + rsCombo + "?", rsCombo, rsRegional, rsSurrPair, rsAstral].join("|") + ")";
      var reUnicode = RegExp(rsFitz + "(?=" + rsFitz + ")|" + rsSymbol + rsSeq, "g");
      function unicodeToArray(string) {
        return string.match(reUnicode) || [];
      }
      function stringToArray(string) {
        return hasUnicode(string) ? unicodeToArray(string) : asciiToArray(string);
      }
      function toString(value) {
        return value == null ? "" : baseToString(value);
      }
      var reTrim = /^\s+|\s+$/g;
      function trim(string, chars, guard) {
        string = toString(string);
        if (string && (guard || chars === void 0)) {
          return string.replace(reTrim, "");
        }
        if (!string || !(chars = baseToString(chars))) {
          return string;
        }
        var strSymbols = stringToArray(string), chrSymbols = stringToArray(chars), start = charsStartIndex(strSymbols, chrSymbols), end = charsEndIndex(strSymbols, chrSymbols) + 1;
        return castSlice(strSymbols, start, end).join("");
      }
      var FN_ARGS = /^(?:async\s+)?(function)?\s*[^\(]*\(\s*([^\)]*)\)/m;
      var FN_ARG_SPLIT = /,/;
      var FN_ARG = /(=.+)?(\s*)$/;
      var STRIP_COMMENTS = /((\/\/.*$)|(\/\*[\s\S]*?\*\/))/mg;
      function parseParams(func) {
        func = func.toString().replace(STRIP_COMMENTS, "");
        func = func.match(FN_ARGS)[2].replace(" ", "");
        func = func ? func.split(FN_ARG_SPLIT) : [];
        func = func.map(function(arg) {
          return trim(arg.replace(FN_ARG, ""));
        });
        return func;
      }
      function autoInject(tasks, callback) {
        var newTasks = {};
        baseForOwn(tasks, function(taskFn, key) {
          var params;
          var fnIsAsync = isAsync(taskFn);
          var hasNoDeps = !fnIsAsync && taskFn.length === 1 || fnIsAsync && taskFn.length === 0;
          if (isArray(taskFn)) {
            params = taskFn.slice(0, -1);
            taskFn = taskFn[taskFn.length - 1];
            newTasks[key] = params.concat(params.length > 0 ? newTask : taskFn);
          } else if (hasNoDeps) {
            newTasks[key] = taskFn;
          } else {
            params = parseParams(taskFn);
            if (taskFn.length === 0 && !fnIsAsync && params.length === 0) {
              throw new Error("autoInject task functions require explicit parameters.");
            }
            if (!fnIsAsync) params.pop();
            newTasks[key] = params.concat(newTask);
          }
          function newTask(results, taskCb) {
            var newArgs = arrayMap(params, function(name) {
              return results[name];
            });
            newArgs.push(taskCb);
            wrapAsync(taskFn).apply(null, newArgs);
          }
        });
        auto(newTasks, callback);
      }
      function DLL() {
        this.head = this.tail = null;
        this.length = 0;
      }
      function setInitial(dll, node) {
        dll.length = 1;
        dll.head = dll.tail = node;
      }
      DLL.prototype.removeLink = function(node) {
        if (node.prev) node.prev.next = node.next;
        else this.head = node.next;
        if (node.next) node.next.prev = node.prev;
        else this.tail = node.prev;
        node.prev = node.next = null;
        this.length -= 1;
        return node;
      };
      DLL.prototype.empty = function() {
        while (this.head) this.shift();
        return this;
      };
      DLL.prototype.insertAfter = function(node, newNode) {
        newNode.prev = node;
        newNode.next = node.next;
        if (node.next) node.next.prev = newNode;
        else this.tail = newNode;
        node.next = newNode;
        this.length += 1;
      };
      DLL.prototype.insertBefore = function(node, newNode) {
        newNode.prev = node.prev;
        newNode.next = node;
        if (node.prev) node.prev.next = newNode;
        else this.head = newNode;
        node.prev = newNode;
        this.length += 1;
      };
      DLL.prototype.unshift = function(node) {
        if (this.head) this.insertBefore(this.head, node);
        else setInitial(this, node);
      };
      DLL.prototype.push = function(node) {
        if (this.tail) this.insertAfter(this.tail, node);
        else setInitial(this, node);
      };
      DLL.prototype.shift = function() {
        return this.head && this.removeLink(this.head);
      };
      DLL.prototype.pop = function() {
        return this.tail && this.removeLink(this.tail);
      };
      DLL.prototype.toArray = function() {
        var arr = Array(this.length);
        var curr = this.head;
        for (var idx = 0; idx < this.length; idx++) {
          arr[idx] = curr.data;
          curr = curr.next;
        }
        return arr;
      };
      DLL.prototype.remove = function(testFn) {
        var curr = this.head;
        while (!!curr) {
          var next = curr.next;
          if (testFn(curr)) {
            this.removeLink(curr);
          }
          curr = next;
        }
        return this;
      };
      function queue(worker, concurrency, payload) {
        if (concurrency == null) {
          concurrency = 1;
        } else if (concurrency === 0) {
          throw new Error("Concurrency must not be zero");
        }
        var _worker = wrapAsync(worker);
        var numRunning = 0;
        var workersList = [];
        var processingScheduled = false;
        function _insert(data, insertAtFront, callback) {
          if (callback != null && typeof callback !== "function") {
            throw new Error("task callback must be a function");
          }
          q.started = true;
          if (!isArray(data)) {
            data = [data];
          }
          if (data.length === 0 && q.idle()) {
            return setImmediate$1(function() {
              q.drain();
            });
          }
          for (var i = 0, l = data.length; i < l; i++) {
            var item = {
              data: data[i],
              callback: callback || noop
            };
            if (insertAtFront) {
              q._tasks.unshift(item);
            } else {
              q._tasks.push(item);
            }
          }
          if (!processingScheduled) {
            processingScheduled = true;
            setImmediate$1(function() {
              processingScheduled = false;
              q.process();
            });
          }
        }
        function _next(tasks) {
          return function(err) {
            numRunning -= 1;
            for (var i = 0, l = tasks.length; i < l; i++) {
              var task = tasks[i];
              var index2 = baseIndexOf(workersList, task, 0);
              if (index2 === 0) {
                workersList.shift();
              } else if (index2 > 0) {
                workersList.splice(index2, 1);
              }
              task.callback.apply(task, arguments);
              if (err != null) {
                q.error(err, task.data);
              }
            }
            if (numRunning <= q.concurrency - q.buffer) {
              q.unsaturated();
            }
            if (q.idle()) {
              q.drain();
            }
            q.process();
          };
        }
        var isProcessing = false;
        var q = {
          _tasks: new DLL(),
          concurrency,
          payload,
          saturated: noop,
          unsaturated: noop,
          buffer: concurrency / 4,
          empty: noop,
          drain: noop,
          error: noop,
          started: false,
          paused: false,
          push: function(data, callback) {
            _insert(data, false, callback);
          },
          kill: function() {
            q.drain = noop;
            q._tasks.empty();
          },
          unshift: function(data, callback) {
            _insert(data, true, callback);
          },
          remove: function(testFn) {
            q._tasks.remove(testFn);
          },
          process: function() {
            if (isProcessing) {
              return;
            }
            isProcessing = true;
            while (!q.paused && numRunning < q.concurrency && q._tasks.length) {
              var tasks = [], data = [];
              var l = q._tasks.length;
              if (q.payload) l = Math.min(l, q.payload);
              for (var i = 0; i < l; i++) {
                var node = q._tasks.shift();
                tasks.push(node);
                workersList.push(node);
                data.push(node.data);
              }
              numRunning += 1;
              if (q._tasks.length === 0) {
                q.empty();
              }
              if (numRunning === q.concurrency) {
                q.saturated();
              }
              var cb = onlyOnce(_next(tasks));
              _worker(data, cb);
            }
            isProcessing = false;
          },
          length: function() {
            return q._tasks.length;
          },
          running: function() {
            return numRunning;
          },
          workersList: function() {
            return workersList;
          },
          idle: function() {
            return q._tasks.length + numRunning === 0;
          },
          pause: function() {
            q.paused = true;
          },
          resume: function() {
            if (q.paused === false) {
              return;
            }
            q.paused = false;
            setImmediate$1(q.process);
          }
        };
        return q;
      }
      function cargo(worker, payload) {
        return queue(worker, 1, payload);
      }
      var eachOfSeries = doLimit(eachOfLimit, 1);
      function reduce(coll, memo, iteratee, callback) {
        callback = once(callback || noop);
        var _iteratee = wrapAsync(iteratee);
        eachOfSeries(coll, function(x, i, callback2) {
          _iteratee(memo, x, function(err, v) {
            memo = v;
            callback2(err);
          });
        }, function(err) {
          callback(err, memo);
        });
      }
      function seq() {
        var _functions = arrayMap(arguments, wrapAsync);
        return function() {
          var args = slice(arguments);
          var that = this;
          var cb = args[args.length - 1];
          if (typeof cb == "function") {
            args.pop();
          } else {
            cb = noop;
          }
          reduce(
            _functions,
            args,
            function(newargs, fn, cb2) {
              fn.apply(that, newargs.concat(function(err) {
                var nextargs = slice(arguments, 1);
                cb2(err, nextargs);
              }));
            },
            function(err, results) {
              cb.apply(that, [err].concat(results));
            }
          );
        };
      }
      var compose = function() {
        return seq.apply(null, slice(arguments).reverse());
      };
      var _concat = Array.prototype.concat;
      var concatLimit = function(coll, limit, iteratee, callback) {
        callback = callback || noop;
        var _iteratee = wrapAsync(iteratee);
        mapLimit(coll, limit, function(val, callback2) {
          _iteratee(val, function(err) {
            if (err) return callback2(err);
            return callback2(null, slice(arguments, 1));
          });
        }, function(err, mapResults) {
          var result = [];
          for (var i = 0; i < mapResults.length; i++) {
            if (mapResults[i]) {
              result = _concat.apply(result, mapResults[i]);
            }
          }
          return callback(err, result);
        });
      };
      var concat = doLimit(concatLimit, Infinity);
      var concatSeries = doLimit(concatLimit, 1);
      var constant = function() {
        var values = slice(arguments);
        var args = [null].concat(values);
        return function() {
          var callback = arguments[arguments.length - 1];
          return callback.apply(this, args);
        };
      };
      function identity(value) {
        return value;
      }
      function _createTester(check, getResult) {
        return function(eachfn, arr, iteratee, cb) {
          cb = cb || noop;
          var testPassed = false;
          var testResult;
          eachfn(arr, function(value, _, callback) {
            iteratee(value, function(err, result) {
              if (err) {
                callback(err);
              } else if (check(result) && !testResult) {
                testPassed = true;
                testResult = getResult(true, value);
                callback(null, breakLoop);
              } else {
                callback();
              }
            });
          }, function(err) {
            if (err) {
              cb(err);
            } else {
              cb(null, testPassed ? testResult : getResult(false));
            }
          });
        };
      }
      function _findGetResult(v, x) {
        return x;
      }
      var detect = doParallel(_createTester(identity, _findGetResult));
      var detectLimit = doParallelLimit(_createTester(identity, _findGetResult));
      var detectSeries = doLimit(detectLimit, 1);
      function consoleFunc(name) {
        return function(fn) {
          var args = slice(arguments, 1);
          args.push(function(err) {
            var args2 = slice(arguments, 1);
            if (typeof console === "object") {
              if (err) {
                if (console.error) {
                  console.error(err);
                }
              } else if (console[name]) {
                arrayEach(args2, function(x) {
                  console[name](x);
                });
              }
            }
          });
          wrapAsync(fn).apply(null, args);
        };
      }
      var dir = consoleFunc("dir");
      function doDuring(fn, test, callback) {
        callback = onlyOnce(callback || noop);
        var _fn = wrapAsync(fn);
        var _test = wrapAsync(test);
        function next(err) {
          if (err) return callback(err);
          var args = slice(arguments, 1);
          args.push(check);
          _test.apply(this, args);
        }
        function check(err, truth) {
          if (err) return callback(err);
          if (!truth) return callback(null);
          _fn(next);
        }
        check(null, true);
      }
      function doWhilst(iteratee, test, callback) {
        callback = onlyOnce(callback || noop);
        var _iteratee = wrapAsync(iteratee);
        var next = function(err) {
          if (err) return callback(err);
          var args = slice(arguments, 1);
          if (test.apply(this, args)) return _iteratee(next);
          callback.apply(null, [null].concat(args));
        };
        _iteratee(next);
      }
      function doUntil(iteratee, test, callback) {
        doWhilst(iteratee, function() {
          return !test.apply(this, arguments);
        }, callback);
      }
      function during(test, fn, callback) {
        callback = onlyOnce(callback || noop);
        var _fn = wrapAsync(fn);
        var _test = wrapAsync(test);
        function next(err) {
          if (err) return callback(err);
          _test(check);
        }
        function check(err, truth) {
          if (err) return callback(err);
          if (!truth) return callback(null);
          _fn(next);
        }
        _test(check);
      }
      function _withoutIndex(iteratee) {
        return function(value, index2, callback) {
          return iteratee(value, callback);
        };
      }
      function eachLimit(coll, iteratee, callback) {
        eachOf(coll, _withoutIndex(wrapAsync(iteratee)), callback);
      }
      function eachLimit$1(coll, limit, iteratee, callback) {
        _eachOfLimit(limit)(coll, _withoutIndex(wrapAsync(iteratee)), callback);
      }
      var eachSeries = doLimit(eachLimit$1, 1);
      function ensureAsync(fn) {
        if (isAsync(fn)) return fn;
        return initialParams(function(args, callback) {
          var sync = true;
          args.push(function() {
            var innerArgs = arguments;
            if (sync) {
              setImmediate$1(function() {
                callback.apply(null, innerArgs);
              });
            } else {
              callback.apply(null, innerArgs);
            }
          });
          fn.apply(this, args);
          sync = false;
        });
      }
      function notId(v) {
        return !v;
      }
      var every = doParallel(_createTester(notId, notId));
      var everyLimit = doParallelLimit(_createTester(notId, notId));
      var everySeries = doLimit(everyLimit, 1);
      function baseProperty(key) {
        return function(object) {
          return object == null ? void 0 : object[key];
        };
      }
      function filterArray(eachfn, arr, iteratee, callback) {
        var truthValues = new Array(arr.length);
        eachfn(arr, function(x, index2, callback2) {
          iteratee(x, function(err, v) {
            truthValues[index2] = !!v;
            callback2(err);
          });
        }, function(err) {
          if (err) return callback(err);
          var results = [];
          for (var i = 0; i < arr.length; i++) {
            if (truthValues[i]) results.push(arr[i]);
          }
          callback(null, results);
        });
      }
      function filterGeneric(eachfn, coll, iteratee, callback) {
        var results = [];
        eachfn(coll, function(x, index2, callback2) {
          iteratee(x, function(err, v) {
            if (err) {
              callback2(err);
            } else {
              if (v) {
                results.push({ index: index2, value: x });
              }
              callback2();
            }
          });
        }, function(err) {
          if (err) {
            callback(err);
          } else {
            callback(null, arrayMap(results.sort(function(a, b) {
              return a.index - b.index;
            }), baseProperty("value")));
          }
        });
      }
      function _filter(eachfn, coll, iteratee, callback) {
        var filter2 = isArrayLike(coll) ? filterArray : filterGeneric;
        filter2(eachfn, coll, wrapAsync(iteratee), callback || noop);
      }
      var filter = doParallel(_filter);
      var filterLimit = doParallelLimit(_filter);
      var filterSeries = doLimit(filterLimit, 1);
      function forever(fn, errback) {
        var done = onlyOnce(errback || noop);
        var task = wrapAsync(ensureAsync(fn));
        function next(err) {
          if (err) return done(err);
          task(next);
        }
        next();
      }
      var groupByLimit = function(coll, limit, iteratee, callback) {
        callback = callback || noop;
        var _iteratee = wrapAsync(iteratee);
        mapLimit(coll, limit, function(val, callback2) {
          _iteratee(val, function(err, key) {
            if (err) return callback2(err);
            return callback2(null, { key, val });
          });
        }, function(err, mapResults) {
          var result = {};
          var hasOwnProperty2 = Object.prototype.hasOwnProperty;
          for (var i = 0; i < mapResults.length; i++) {
            if (mapResults[i]) {
              var key = mapResults[i].key;
              var val = mapResults[i].val;
              if (hasOwnProperty2.call(result, key)) {
                result[key].push(val);
              } else {
                result[key] = [val];
              }
            }
          }
          return callback(err, result);
        });
      };
      var groupBy = doLimit(groupByLimit, Infinity);
      var groupBySeries = doLimit(groupByLimit, 1);
      var log = consoleFunc("log");
      function mapValuesLimit(obj2, limit, iteratee, callback) {
        callback = once(callback || noop);
        var newObj = {};
        var _iteratee = wrapAsync(iteratee);
        eachOfLimit(obj2, limit, function(val, key, next) {
          _iteratee(val, key, function(err, result) {
            if (err) return next(err);
            newObj[key] = result;
            next();
          });
        }, function(err) {
          callback(err, newObj);
        });
      }
      var mapValues = doLimit(mapValuesLimit, Infinity);
      var mapValuesSeries = doLimit(mapValuesLimit, 1);
      function has(obj2, key) {
        return key in obj2;
      }
      function memoize(fn, hasher) {
        var memo = /* @__PURE__ */ Object.create(null);
        var queues = /* @__PURE__ */ Object.create(null);
        hasher = hasher || identity;
        var _fn = wrapAsync(fn);
        var memoized = initialParams(function memoized2(args, callback) {
          var key = hasher.apply(null, args);
          if (has(memo, key)) {
            setImmediate$1(function() {
              callback.apply(null, memo[key]);
            });
          } else if (has(queues, key)) {
            queues[key].push(callback);
          } else {
            queues[key] = [callback];
            _fn.apply(null, args.concat(function() {
              var args2 = slice(arguments);
              memo[key] = args2;
              var q = queues[key];
              delete queues[key];
              for (var i = 0, l = q.length; i < l; i++) {
                q[i].apply(null, args2);
              }
            }));
          }
        });
        memoized.memo = memo;
        memoized.unmemoized = fn;
        return memoized;
      }
      var _defer$1;
      if (hasNextTick) {
        _defer$1 = process.nextTick;
      } else if (hasSetImmediate) {
        _defer$1 = setImmediate;
      } else {
        _defer$1 = fallback;
      }
      var nextTick = wrap(_defer$1);
      function _parallel(eachfn, tasks, callback) {
        callback = callback || noop;
        var results = isArrayLike(tasks) ? [] : {};
        eachfn(tasks, function(task, key, callback2) {
          wrapAsync(task)(function(err, result) {
            if (arguments.length > 2) {
              result = slice(arguments, 1);
            }
            results[key] = result;
            callback2(err);
          });
        }, function(err) {
          callback(err, results);
        });
      }
      function parallelLimit(tasks, callback) {
        _parallel(eachOf, tasks, callback);
      }
      function parallelLimit$1(tasks, limit, callback) {
        _parallel(_eachOfLimit(limit), tasks, callback);
      }
      var queue$1 = function(worker, concurrency) {
        var _worker = wrapAsync(worker);
        return queue(function(items, cb) {
          _worker(items[0], cb);
        }, concurrency, 1);
      };
      var priorityQueue = function(worker, concurrency) {
        var q = queue$1(worker, concurrency);
        q.push = function(data, priority, callback) {
          if (callback == null) callback = noop;
          if (typeof callback !== "function") {
            throw new Error("task callback must be a function");
          }
          q.started = true;
          if (!isArray(data)) {
            data = [data];
          }
          if (data.length === 0) {
            return setImmediate$1(function() {
              q.drain();
            });
          }
          priority = priority || 0;
          var nextNode = q._tasks.head;
          while (nextNode && priority >= nextNode.priority) {
            nextNode = nextNode.next;
          }
          for (var i = 0, l = data.length; i < l; i++) {
            var item = {
              data: data[i],
              priority,
              callback
            };
            if (nextNode) {
              q._tasks.insertBefore(nextNode, item);
            } else {
              q._tasks.push(item);
            }
          }
          setImmediate$1(q.process);
        };
        delete q.unshift;
        return q;
      };
      function race(tasks, callback) {
        callback = once(callback || noop);
        if (!isArray(tasks)) return callback(new TypeError("First argument to race must be an array of functions"));
        if (!tasks.length) return callback();
        for (var i = 0, l = tasks.length; i < l; i++) {
          wrapAsync(tasks[i])(callback);
        }
      }
      function reduceRight(array, memo, iteratee, callback) {
        var reversed = slice(array).reverse();
        reduce(reversed, memo, iteratee, callback);
      }
      function reflect(fn) {
        var _fn = wrapAsync(fn);
        return initialParams(function reflectOn(args, reflectCallback) {
          args.push(function callback(error, cbArg) {
            if (error) {
              reflectCallback(null, { error });
            } else {
              var value;
              if (arguments.length <= 2) {
                value = cbArg;
              } else {
                value = slice(arguments, 1);
              }
              reflectCallback(null, { value });
            }
          });
          return _fn.apply(this, args);
        });
      }
      function reflectAll(tasks) {
        var results;
        if (isArray(tasks)) {
          results = arrayMap(tasks, reflect);
        } else {
          results = {};
          baseForOwn(tasks, function(task, key) {
            results[key] = reflect.call(this, task);
          });
        }
        return results;
      }
      function reject$1(eachfn, arr, iteratee, callback) {
        _filter(eachfn, arr, function(value, cb) {
          iteratee(value, function(err, v) {
            cb(err, !v);
          });
        }, callback);
      }
      var reject = doParallel(reject$1);
      var rejectLimit = doParallelLimit(reject$1);
      var rejectSeries = doLimit(rejectLimit, 1);
      function constant$1(value) {
        return function() {
          return value;
        };
      }
      function retry(opts, task, callback) {
        var DEFAULT_TIMES = 5;
        var DEFAULT_INTERVAL = 0;
        var options = {
          times: DEFAULT_TIMES,
          intervalFunc: constant$1(DEFAULT_INTERVAL)
        };
        function parseTimes(acc, t) {
          if (typeof t === "object") {
            acc.times = +t.times || DEFAULT_TIMES;
            acc.intervalFunc = typeof t.interval === "function" ? t.interval : constant$1(+t.interval || DEFAULT_INTERVAL);
            acc.errorFilter = t.errorFilter;
          } else if (typeof t === "number" || typeof t === "string") {
            acc.times = +t || DEFAULT_TIMES;
          } else {
            throw new Error("Invalid arguments for async.retry");
          }
        }
        if (arguments.length < 3 && typeof opts === "function") {
          callback = task || noop;
          task = opts;
        } else {
          parseTimes(options, opts);
          callback = callback || noop;
        }
        if (typeof task !== "function") {
          throw new Error("Invalid arguments for async.retry");
        }
        var _task = wrapAsync(task);
        var attempt = 1;
        function retryAttempt() {
          _task(function(err) {
            if (err && attempt++ < options.times && (typeof options.errorFilter != "function" || options.errorFilter(err))) {
              setTimeout(retryAttempt, options.intervalFunc(attempt));
            } else {
              callback.apply(null, arguments);
            }
          });
        }
        retryAttempt();
      }
      var retryable = function(opts, task) {
        if (!task) {
          task = opts;
          opts = null;
        }
        var _task = wrapAsync(task);
        return initialParams(function(args, callback) {
          function taskFn(cb) {
            _task.apply(null, args.concat(cb));
          }
          if (opts) retry(opts, taskFn, callback);
          else retry(taskFn, callback);
        });
      };
      function series(tasks, callback) {
        _parallel(eachOfSeries, tasks, callback);
      }
      var some = doParallel(_createTester(Boolean, identity));
      var someLimit = doParallelLimit(_createTester(Boolean, identity));
      var someSeries = doLimit(someLimit, 1);
      function sortBy(coll, iteratee, callback) {
        var _iteratee = wrapAsync(iteratee);
        map(coll, function(x, callback2) {
          _iteratee(x, function(err, criteria) {
            if (err) return callback2(err);
            callback2(null, { value: x, criteria });
          });
        }, function(err, results) {
          if (err) return callback(err);
          callback(null, arrayMap(results.sort(comparator), baseProperty("value")));
        });
        function comparator(left, right) {
          var a = left.criteria, b = right.criteria;
          return a < b ? -1 : a > b ? 1 : 0;
        }
      }
      function timeout(asyncFn, milliseconds, info) {
        var fn = wrapAsync(asyncFn);
        return initialParams(function(args, callback) {
          var timedOut = false;
          var timer;
          function timeoutCallback() {
            var name = asyncFn.name || "anonymous";
            var error = new Error('Callback function "' + name + '" timed out.');
            error.code = "ETIMEDOUT";
            if (info) {
              error.info = info;
            }
            timedOut = true;
            callback(error);
          }
          args.push(function() {
            if (!timedOut) {
              callback.apply(null, arguments);
              clearTimeout(timer);
            }
          });
          timer = setTimeout(timeoutCallback, milliseconds);
          fn.apply(null, args);
        });
      }
      var nativeCeil = Math.ceil;
      var nativeMax = Math.max;
      function baseRange(start, end, step, fromRight) {
        var index2 = -1, length = nativeMax(nativeCeil((end - start) / (step || 1)), 0), result = Array(length);
        while (length--) {
          result[fromRight ? length : ++index2] = start;
          start += step;
        }
        return result;
      }
      function timeLimit(count, limit, iteratee, callback) {
        var _iteratee = wrapAsync(iteratee);
        mapLimit(baseRange(0, count, 1), limit, _iteratee, callback);
      }
      var times = doLimit(timeLimit, Infinity);
      var timesSeries = doLimit(timeLimit, 1);
      function transform(coll, accumulator, iteratee, callback) {
        if (arguments.length <= 3) {
          callback = iteratee;
          iteratee = accumulator;
          accumulator = isArray(coll) ? [] : {};
        }
        callback = once(callback || noop);
        var _iteratee = wrapAsync(iteratee);
        eachOf(coll, function(v, k, cb) {
          _iteratee(accumulator, v, k, cb);
        }, function(err) {
          callback(err, accumulator);
        });
      }
      function tryEach(tasks, callback) {
        var error = null;
        var result;
        callback = callback || noop;
        eachSeries(tasks, function(task, callback2) {
          wrapAsync(task)(function(err, res) {
            if (arguments.length > 2) {
              result = slice(arguments, 1);
            } else {
              result = res;
            }
            error = err;
            callback2(!err);
          });
        }, function() {
          callback(error, result);
        });
      }
      function unmemoize(fn) {
        return function() {
          return (fn.unmemoized || fn).apply(null, arguments);
        };
      }
      function whilst(test, iteratee, callback) {
        callback = onlyOnce(callback || noop);
        var _iteratee = wrapAsync(iteratee);
        if (!test()) return callback(null);
        var next = function(err) {
          if (err) return callback(err);
          if (test()) return _iteratee(next);
          var args = slice(arguments, 1);
          callback.apply(null, [null].concat(args));
        };
        _iteratee(next);
      }
      function until(test, iteratee, callback) {
        whilst(function() {
          return !test.apply(this, arguments);
        }, iteratee, callback);
      }
      var waterfall = function(tasks, callback) {
        callback = once(callback || noop);
        if (!isArray(tasks)) return callback(new Error("First argument to waterfall must be an array of functions"));
        if (!tasks.length) return callback();
        var taskIndex = 0;
        function nextTask(args) {
          var task = wrapAsync(tasks[taskIndex++]);
          args.push(onlyOnce(next));
          task.apply(null, args);
        }
        function next(err) {
          if (err || taskIndex === tasks.length) {
            return callback.apply(null, arguments);
          }
          nextTask(slice(arguments, 1));
        }
        nextTask([]);
      };
      var index = {
        apply,
        applyEach,
        applyEachSeries,
        asyncify,
        auto,
        autoInject,
        cargo,
        compose,
        concat,
        concatLimit,
        concatSeries,
        constant,
        detect,
        detectLimit,
        detectSeries,
        dir,
        doDuring,
        doUntil,
        doWhilst,
        during,
        each: eachLimit,
        eachLimit: eachLimit$1,
        eachOf,
        eachOfLimit,
        eachOfSeries,
        eachSeries,
        ensureAsync,
        every,
        everyLimit,
        everySeries,
        filter,
        filterLimit,
        filterSeries,
        forever,
        groupBy,
        groupByLimit,
        groupBySeries,
        log,
        map,
        mapLimit,
        mapSeries,
        mapValues,
        mapValuesLimit,
        mapValuesSeries,
        memoize,
        nextTick,
        parallel: parallelLimit,
        parallelLimit: parallelLimit$1,
        priorityQueue,
        queue: queue$1,
        race,
        reduce,
        reduceRight,
        reflect,
        reflectAll,
        reject,
        rejectLimit,
        rejectSeries,
        retry,
        retryable,
        seq,
        series,
        setImmediate: setImmediate$1,
        some,
        someLimit,
        someSeries,
        sortBy,
        timeout,
        times,
        timesLimit: timeLimit,
        timesSeries,
        transform,
        tryEach,
        unmemoize,
        until,
        waterfall,
        whilst,
        // aliases
        all: every,
        allLimit: everyLimit,
        allSeries: everySeries,
        any: some,
        anyLimit: someLimit,
        anySeries: someSeries,
        find: detect,
        findLimit: detectLimit,
        findSeries: detectSeries,
        forEach: eachLimit,
        forEachSeries: eachSeries,
        forEachLimit: eachLimit$1,
        forEachOf: eachOf,
        forEachOfSeries: eachOfSeries,
        forEachOfLimit: eachOfLimit,
        inject: reduce,
        foldl: reduce,
        foldr: reduceRight,
        select: filter,
        selectLimit: filterLimit,
        selectSeries: filterSeries,
        wrapSync: asyncify
      };
      exports3["default"] = index;
      exports3.apply = apply;
      exports3.applyEach = applyEach;
      exports3.applyEachSeries = applyEachSeries;
      exports3.asyncify = asyncify;
      exports3.auto = auto;
      exports3.autoInject = autoInject;
      exports3.cargo = cargo;
      exports3.compose = compose;
      exports3.concat = concat;
      exports3.concatLimit = concatLimit;
      exports3.concatSeries = concatSeries;
      exports3.constant = constant;
      exports3.detect = detect;
      exports3.detectLimit = detectLimit;
      exports3.detectSeries = detectSeries;
      exports3.dir = dir;
      exports3.doDuring = doDuring;
      exports3.doUntil = doUntil;
      exports3.doWhilst = doWhilst;
      exports3.during = during;
      exports3.each = eachLimit;
      exports3.eachLimit = eachLimit$1;
      exports3.eachOf = eachOf;
      exports3.eachOfLimit = eachOfLimit;
      exports3.eachOfSeries = eachOfSeries;
      exports3.eachSeries = eachSeries;
      exports3.ensureAsync = ensureAsync;
      exports3.every = every;
      exports3.everyLimit = everyLimit;
      exports3.everySeries = everySeries;
      exports3.filter = filter;
      exports3.filterLimit = filterLimit;
      exports3.filterSeries = filterSeries;
      exports3.forever = forever;
      exports3.groupBy = groupBy;
      exports3.groupByLimit = groupByLimit;
      exports3.groupBySeries = groupBySeries;
      exports3.log = log;
      exports3.map = map;
      exports3.mapLimit = mapLimit;
      exports3.mapSeries = mapSeries;
      exports3.mapValues = mapValues;
      exports3.mapValuesLimit = mapValuesLimit;
      exports3.mapValuesSeries = mapValuesSeries;
      exports3.memoize = memoize;
      exports3.nextTick = nextTick;
      exports3.parallel = parallelLimit;
      exports3.parallelLimit = parallelLimit$1;
      exports3.priorityQueue = priorityQueue;
      exports3.queue = queue$1;
      exports3.race = race;
      exports3.reduce = reduce;
      exports3.reduceRight = reduceRight;
      exports3.reflect = reflect;
      exports3.reflectAll = reflectAll;
      exports3.reject = reject;
      exports3.rejectLimit = rejectLimit;
      exports3.rejectSeries = rejectSeries;
      exports3.retry = retry;
      exports3.retryable = retryable;
      exports3.seq = seq;
      exports3.series = series;
      exports3.setImmediate = setImmediate$1;
      exports3.some = some;
      exports3.someLimit = someLimit;
      exports3.someSeries = someSeries;
      exports3.sortBy = sortBy;
      exports3.timeout = timeout;
      exports3.times = times;
      exports3.timesLimit = timeLimit;
      exports3.timesSeries = timesSeries;
      exports3.transform = transform;
      exports3.tryEach = tryEach;
      exports3.unmemoize = unmemoize;
      exports3.until = until;
      exports3.waterfall = waterfall;
      exports3.whilst = whilst;
      exports3.all = every;
      exports3.allLimit = everyLimit;
      exports3.allSeries = everySeries;
      exports3.any = some;
      exports3.anyLimit = someLimit;
      exports3.anySeries = someSeries;
      exports3.find = detect;
      exports3.findLimit = detectLimit;
      exports3.findSeries = detectSeries;
      exports3.forEach = eachLimit;
      exports3.forEachSeries = eachSeries;
      exports3.forEachLimit = eachLimit$1;
      exports3.forEachOf = eachOf;
      exports3.forEachOfSeries = eachOfSeries;
      exports3.forEachOfLimit = eachOfLimit;
      exports3.inject = reduce;
      exports3.foldl = reduce;
      exports3.foldr = reduceRight;
      exports3.select = filter;
      exports3.selectLimit = filterLimit;
      exports3.selectSeries = filterSeries;
      exports3.wrapSync = asyncify;
      Object.defineProperty(exports3, "__esModule", { value: true });
    }));
  }
});

// node_modules/extend/index.js
var require_extend = __commonJS({
  "node_modules/extend/index.js"(exports2, module2) {
    "use strict";
    var hasOwn = Object.prototype.hasOwnProperty;
    var toStr = Object.prototype.toString;
    var defineProperty = Object.defineProperty;
    var gOPD = Object.getOwnPropertyDescriptor;
    var isArray = function isArray2(arr) {
      if (typeof Array.isArray === "function") {
        return Array.isArray(arr);
      }
      return toStr.call(arr) === "[object Array]";
    };
    var isPlainObject = function isPlainObject2(obj2) {
      if (!obj2 || toStr.call(obj2) !== "[object Object]") {
        return false;
      }
      var hasOwnConstructor = hasOwn.call(obj2, "constructor");
      var hasIsPrototypeOf = obj2.constructor && obj2.constructor.prototype && hasOwn.call(obj2.constructor.prototype, "isPrototypeOf");
      if (obj2.constructor && !hasOwnConstructor && !hasIsPrototypeOf) {
        return false;
      }
      var key;
      for (key in obj2) {
      }
      return typeof key === "undefined" || hasOwn.call(obj2, key);
    };
    var setProperty = function setProperty2(target, options) {
      if (defineProperty && options.name === "__proto__") {
        defineProperty(target, options.name, {
          enumerable: true,
          configurable: true,
          value: options.newValue,
          writable: true
        });
      } else {
        target[options.name] = options.newValue;
      }
    };
    var getProperty = function getProperty2(obj2, name) {
      if (name === "__proto__") {
        if (!hasOwn.call(obj2, name)) {
          return void 0;
        } else if (gOPD) {
          return gOPD(obj2, name).value;
        }
      }
      return obj2[name];
    };
    module2.exports = function extend() {
      var options, name, src, copy, copyIsArray, clone;
      var target = arguments[0];
      var i = 1;
      var length = arguments.length;
      var deep = false;
      if (typeof target === "boolean") {
        deep = target;
        target = arguments[1] || {};
        i = 2;
      }
      if (target == null || typeof target !== "object" && typeof target !== "function") {
        target = {};
      }
      for (; i < length; ++i) {
        options = arguments[i];
        if (options != null) {
          for (name in options) {
            src = getProperty(target, name);
            copy = getProperty(options, name);
            if (target !== copy) {
              if (deep && copy && (isPlainObject(copy) || (copyIsArray = isArray(copy)))) {
                if (copyIsArray) {
                  copyIsArray = false;
                  clone = src && isArray(src) ? src : [];
                } else {
                  clone = src && isPlainObject(src) ? src : {};
                }
                setProperty(target, { name, newValue: extend(deep, clone, copy) });
              } else if (typeof copy !== "undefined") {
                setProperty(target, { name, newValue: copy });
              }
            }
          }
        }
      }
      return target;
    };
  }
});

// node_modules/node-ssdp/lib/ssdpHeader.js
var require_ssdpHeader = __commonJS({
  "node_modules/node-ssdp/lib/ssdpHeader.js"(exports2, module2) {
    var extend = require_extend();
    var random = require("crypto").randomBytes;
    function SsdpHeader(method, headers, isResponse) {
      this._method = method;
      this._headers = extend({}, headers || {});
      this._isResponse = isResponse;
      this._id = random(8).toString("hex");
      if (!this._method) throw new Error("SSDP header requires method.");
    }
    SsdpHeader.prototype.id = function() {
      return this._id;
    };
    SsdpHeader.prototype.setHeader = function(key, value) {
      if (key !== void 0 && value !== void 0) {
        this._headers[key] = value;
      }
    };
    SsdpHeader.prototype.setHeaders = function(headers) {
      if (headers && headers.toString && headers.toString() === "[object Object]") {
        extend(this._headers, headers);
      }
    };
    SsdpHeader.prototype.isResponse = function() {
      return !!this._isResponse;
    };
    SsdpHeader.prototype.overrideLocationOnSend = function() {
      this._overrideLocationOnSend = true;
    };
    SsdpHeader.prototype.isOverrideLocationOnSend = function() {
      return !!this._overrideLocationOnSend;
    };
    SsdpHeader.prototype.toString = function(extraHeaders) {
      extraHeaders = extraHeaders || {};
      var headers = extend({}, this._headers);
      extend(headers, extraHeaders);
      var message = [];
      var method = this._method.toUpperCase();
      if (this.isResponse()) {
        message.push("HTTP/1.1 " + method);
      } else {
        message.push(method + " * HTTP/1.1");
      }
      Object.keys(headers).forEach(function(header) {
        message.push(header + ": " + headers[header]);
      });
      message.push("\r\n");
      return message.join("\r\n");
    };
    SsdpHeader.prototype.toBuffer = function(extraHeaders) {
      return Buffer.from(this.toString(extraHeaders), "ascii");
    };
    module2.exports = SsdpHeader;
  }
});

// node_modules/node-ssdp/lib/const.js
var require_const = __commonJS({
  "node_modules/node-ssdp/lib/const.js"(exports2, module2) {
    "use strict";
    module2.exports = {
      SSDP_ALIVE: "ssdp:alive",
      SSDP_BYE: "ssdp:byebye",
      SSDP_ALL: "ssdp:all",
      ADVERTISE_ALIVE: "advertise-alive",
      ADVERTISE_BYE: "advertise-bye",
      NOTIFY: "notify",
      M_SEARCH: "m-search",
      SSDP_DEFAULT_IP: "239.255.255.250",
      SSDP_DEFAULT_PORT: 1900
    };
  }
});

// node_modules/node-ssdp/package.json
var require_package = __commonJS({
  "node_modules/node-ssdp/package.json"(exports2, module2) {
    module2.exports = {
      name: "node-ssdp",
      description: "A node.js SSDP client and server library.",
      keywords: [
        "ssdp",
        "multicast",
        "media",
        "device",
        "upnp",
        "chromecast",
        "iot"
      ],
      version: "4.0.1",
      author: "Ilya Shaisultanov <ilya.shaisultanov@gmail.com>",
      dependencies: {
        async: "^2.6.0",
        bluebird: "^3.5.1",
        debug: "^3.1.0",
        extend: "^3.0.1",
        ip: "^1.1.5"
      },
      repository: {
        type: "git",
        url: "git+ssh://git@github.com/diversario/node-ssdp.git"
      },
      bugs: {
        url: "https://github.com/diversario/node-ssdp/issues"
      },
      main: "./index",
      engines: {
        node: ">=0.10.0"
      },
      devDependencies: {
        chai: "^4.1.2",
        istanbul: "^0.4.5",
        mocha: "^5.0.5",
        "mocha-istanbul": "^0.3.0",
        sinon: "^4.5.0"
      },
      scripts: {
        test: "mocha --exit --recursive test",
        coverage: "istanbul cover node_modules/.bin/_mocha -- test/lib --recursive"
      },
      homepage: "https://github.com/diversario/node-ssdp#readme",
      directories: {
        example: "example",
        test: "test"
      },
      license: "MIT"
    };
  }
});

// node_modules/node-ssdp/lib/index.js
var require_lib = __commonJS({
  "node_modules/node-ssdp/lib/index.js"(exports2, module2) {
    "use strict";
    var dgram = require("dgram");
    var EE = require("events").EventEmitter;
    var util = require("util");
    var ip = require_ip();
    var debug = require_src();
    var os2 = require("os");
    var async = require_async();
    var extend = require_extend();
    var SsdpHeader = require_ssdpHeader();
    var httpHeader = /HTTP\/\d{1}\.\d{1} \d+ .*/;
    var ssdpHeader = /^([^:]+):\s*(.*)$/;
    var c = require_const();
    var nodeVersion = process.version.substr(1);
    var moduleVersion = require_package().version;
    var moduleName = require_package().name;
    function SSDP(opts) {
      var self2 = this;
      if (!(this instanceof SSDP)) return new SSDP(opts);
      this._subclass = this._subclass || "node-ssdp:base";
      opts = opts || {};
      this._logger = opts.customLogger || debug(this._subclass);
      EE.call(self2);
      this._init(opts);
    }
    util.inherits(SSDP, EE);
    SSDP.prototype._init = function(opts) {
      this._ssdpSig = opts.ssdpSig || getSsdpSignature();
      this._explicitSocketBind = opts.explicitSocketBind;
      this._interfaces = opts.interfaces;
      this._reuseAddr = opts.reuseAddr === void 0 ? true : opts.reuseAddr;
      this._ssdpIp = opts.ssdpIp || c.SSDP_DEFAULT_IP;
      this._ssdpPort = opts.ssdpPort || c.SSDP_DEFAULT_PORT;
      this._ssdpTtl = opts.ssdpTtl || 4;
      this._sourcePort = opts.sourcePort || 0;
      this._adInterval = opts.adInterval || 1e4;
      this._ttl = opts.ttl || 1800;
      if (typeof opts.location === "function") {
        Object.defineProperty(this, "_location", {
          enumerable: true,
          get: opts.location
        });
      } else if (typeof opts.location === "object") {
        this._locationProtocol = opts.location.protocol || "http://";
        this._locationPort = opts.location.port;
        this._locationPath = opts.location.path;
      } else {
        this._location = opts.location || "http://" + ip.address() + ":10293/upnp/desc.html";
      }
      this._ssdpServerHost = this._ssdpIp + ":" + this._ssdpPort;
      this._usns = {};
      this._udn = opts.udn || "uuid:f40c2981-7329-40b7-8b04-27f187aecfb5";
      this._extraHeaders = opts.headers || {};
      this._allowWildcards = opts.allowWildcards;
      this._suppressRootDeviceAdvertisements = opts.suppressRootDeviceAdvertisements;
    };
    SSDP.prototype._createSockets = function() {
      var interfaces = os2.networkInterfaces(), self2 = this;
      this.sockets = {};
      Object.keys(interfaces).forEach(function(iName) {
        if (!self2._interfaces || self2._interfaces.indexOf(iName) > -1) {
          self2._logger("discovering all IPs from interface %s", iName);
          interfaces[iName].forEach(function(ipInfo) {
            if (ipInfo.internal == false && ipInfo.family == "IPv4") {
              self2._logger("Will use interface %s", iName);
              var socket;
              if (parseFloat(process.version.replace(/\w/, "")) >= 0.12) {
                socket = dgram.createSocket({ type: "udp4", reuseAddr: self2._reuseAddr });
              } else {
                socket = dgram.createSocket("udp4");
              }
              if (socket) {
                socket.unref();
                self2.sockets[ipInfo.address] = socket;
              }
            }
          });
        }
      });
      if (Object.keys(this.sockets) == 0) {
        throw new Error("No sockets available, cannot start.");
      }
    };
    SSDP.prototype._stop = function() {
      var self2 = this;
      if (!this.sockets) {
        this._logger("Already stopped.");
        return;
      }
      Object.keys(this.sockets).forEach(function(ipAddress) {
        var socket = self2.sockets[ipAddress];
        socket && socket.close();
        self2._logger("Stopped socket on %s", ipAddress);
      });
      this.sockets = null;
      this._socketBound = this._started = false;
    };
    SSDP.prototype._start = function(cb) {
      var self2 = this;
      if (self2._started) {
        self2._logger("Already started.");
        return;
      }
      if (!this.sockets) {
        this._createSockets();
      }
      self2._started = true;
      var interfaces = Object.keys(this.sockets);
      async.each(interfaces, function(iface, next) {
        var socket = self2.sockets[iface];
        socket.on("error", function onSocketError(err) {
          self2._logger("Socker error: %s", err.message);
        });
        socket.on("message", function onSocketMessage(msg, rinfo) {
          self2._parseMessage(msg, rinfo);
        });
        socket.on("listening", function onSocketListening() {
          var addr = socket.address();
          self2._logger("SSDP listening: %o", { address: "http://" + addr.address + ":" + addr.port, "interface": iface });
          try {
            addMembership();
          } catch (e) {
            if (e.code === "ENODEV" || e.code === "EADDRNOTAVAIL") {
              self2._logger("Interface %s is not present to add multicast group membership. Scheduling a retry. Error: %s", addr, e.message);
              delay2().then(addMembership).catch((e2) => {
                throw e2;
              });
            } else {
              throw e;
            }
          }
          function addMembership() {
            socket.addMembership(self2._ssdpIp, iface);
            socket.setMulticastTTL(self2._ssdpTtl);
          }
          function delay2() {
            return new Promise((resolve) => {
              setTimeout(resolve, 5e3);
            });
          }
        });
        if (self2._explicitSocketBind) {
          socket.bind(self2._sourcePort, iface, next);
        } else {
          socket.bind(self2._sourcePort, next);
        }
      }, cb);
    };
    SSDP.prototype._parseMessage = function(msg, rinfo) {
      msg = msg.toString();
      var type = msg.split("\r\n").shift();
      if (httpHeader.test(type)) {
        this._parseResponse(msg, rinfo);
      } else {
        this._parseCommand(msg, rinfo);
      }
    };
    SSDP.prototype._parseCommand = function parseCommand(msg, rinfo) {
      var method = this._getMethod(msg), headers = this._getHeaders(msg);
      switch (method) {
        case c.NOTIFY:
          this._notify(headers, msg, rinfo);
          break;
        case c.M_SEARCH:
          this._msearch(headers, msg, rinfo);
          break;
        default:
          this._logger("Unhandled command: %o", { "message": msg, "rinfo": rinfo });
      }
    };
    SSDP.prototype._notify = function(headers, msg, rinfo) {
      if (!headers.NTS) {
        this._logger("Missing NTS header: %o", headers);
        return;
      }
      switch (headers.NTS.toLowerCase()) {
        // Device coming to life.
        case c.SSDP_ALIVE:
          this.emit(c.ADVERTISE_ALIVE, headers, rinfo);
          break;
        // Device shutting down.
        case c.SSDP_BYE:
          this.emit(c.ADVERTISE_BYE, headers, rinfo);
          break;
        default:
          this._logger("Unhandled NOTIFY event: %o", { "message": msg, "rinfo": rinfo });
      }
    };
    SSDP.prototype._msearch = function(headers, msg, rinfo) {
      this._logger("SSDP M-SEARCH event: %o", { "ST": headers.ST, "address": rinfo.address, "port": rinfo.port });
      if (!headers.MAN || !headers.MX || !headers.ST) return;
      this._respondToSearch(headers.ST, rinfo);
    };
    SSDP.prototype._respondToSearch = function(serviceType, rinfo) {
      var self2 = this, peer_addr = rinfo.address, peer_port = rinfo.port, stRegex, acceptor;
      if (serviceType[0] == '"' && serviceType[serviceType.length - 1] == '"') {
        serviceType = serviceType.slice(1, -1);
      }
      if (self2._allowWildcards) {
        stRegex = new RegExp(serviceType.replace(/\*/g, ".*") + "$");
        acceptor = function(usn, serviceType2) {
          return serviceType2 === c.SSDP_ALL || stRegex.test(usn);
        };
      } else {
        acceptor = function(usn, serviceType2) {
          return serviceType2 === c.SSDP_ALL || usn === serviceType2;
        };
      }
      Object.keys(self2._usns).forEach(function(usn) {
        var udn = self2._usns[usn];
        if (self2._allowWildcards) {
          udn = udn.replace(stRegex, serviceType);
        }
        if (acceptor(usn, serviceType)) {
          var header = new SsdpHeader("200 OK", {
            "ST": serviceType === c.SSDP_ALL ? usn : serviceType,
            "USN": udn,
            "CACHE-CONTROL": "max-age=" + self2._ttl,
            "DATE": (/* @__PURE__ */ new Date()).toUTCString(),
            "SERVER": self2._ssdpSig,
            "EXT": ""
          }, true);
          header.setHeaders(self2._extraHeaders);
          if (self2._location) {
            header.setHeader("LOCATION", self2._location);
          } else {
            header.overrideLocationOnSend();
          }
          self2._logger("Sending a 200 OK for an M-SEARCH: %o", { "peer": peer_addr, "port": peer_port });
          self2._send(header, peer_addr, peer_port, function(err, bytes) {
            self2._logger("Sent M-SEARCH response: %o", { "message": header.toString(), id: header.id() });
          });
        }
      });
    };
    SSDP.prototype._parseResponse = function parseResponse(msg, rinfo) {
      this._logger("SSDP response: %o", { "message": msg });
      var headers = this._getHeaders(msg), statusCode = this._getStatusCode(msg);
      this.emit("response", headers, statusCode, rinfo);
    };
    SSDP.prototype.addUSN = function(device) {
      this._usns[device] = this._udn + "::" + device;
    };
    SSDP.prototype._getMethod = function _getMethod(msg) {
      var lines = msg.split("\r\n"), type = lines.shift().split(" "), method = (type[0] || "").toLowerCase();
      return method;
    };
    SSDP.prototype._getStatusCode = function _getStatusCode(msg) {
      var lines = msg.split("\r\n"), type = lines.shift().split(" "), code = parseInt(type[1], 10);
      return code;
    };
    SSDP.prototype._getHeaders = function _getHeaders(msg) {
      var lines = msg.split("\r\n");
      var headers = {};
      lines.forEach(function(line) {
        if (line.length) {
          var pairs = line.match(ssdpHeader);
          if (pairs) headers[pairs[1].toUpperCase()] = pairs[2];
        }
      });
      return headers;
    };
    SSDP.prototype._send = function(message, host, port, cb) {
      var self2 = this;
      if (typeof host === "function") {
        cb = host;
        host = this._ssdpIp;
        port = this._ssdpPort;
      }
      var ipAddresses = Object.keys(this.sockets);
      async.each(ipAddresses, function(ipAddress, next) {
        var socket = self2.sockets[ipAddress];
        var buf;
        if (message instanceof SsdpHeader) {
          if (message.isOverrideLocationOnSend()) {
            var location = self2._locationProtocol + ipAddress + ":" + self2._locationPort + self2._locationPath;
            self2._logger('Setting LOCATION header "%s" on message ID %s', location, message.id());
            buf = message.toBuffer({ "LOCATION": location });
          } else {
            buf = message.toBuffer();
          }
        } else {
          buf = message;
        }
        self2._logger("Sending a message to %s:%s", host, port);
        socket.send(buf, 0, buf.length, port, host, next);
      }, cb);
    };
    function getSsdpSignature() {
      return "node.js/" + nodeVersion + " UPnP/1.1 " + moduleName + "/" + moduleVersion;
    }
    module2.exports = SSDP;
  }
});

// node_modules/bluebird/js/release/es5.js
var require_es5 = __commonJS({
  "node_modules/bluebird/js/release/es5.js"(exports2, module2) {
    var isES5 = (function() {
      "use strict";
      return this === void 0;
    })();
    if (isES5) {
      module2.exports = {
        freeze: Object.freeze,
        defineProperty: Object.defineProperty,
        getDescriptor: Object.getOwnPropertyDescriptor,
        keys: Object.keys,
        names: Object.getOwnPropertyNames,
        getPrototypeOf: Object.getPrototypeOf,
        isArray: Array.isArray,
        isES5,
        propertyIsWritable: function(obj2, prop) {
          var descriptor = Object.getOwnPropertyDescriptor(obj2, prop);
          return !!(!descriptor || descriptor.writable || descriptor.set);
        }
      };
    } else {
      has = {}.hasOwnProperty;
      str = {}.toString;
      proto = {}.constructor.prototype;
      ObjectKeys = function(o) {
        var ret2 = [];
        for (var key in o) {
          if (has.call(o, key)) {
            ret2.push(key);
          }
        }
        return ret2;
      };
      ObjectGetDescriptor = function(o, key) {
        return { value: o[key] };
      };
      ObjectDefineProperty = function(o, key, desc) {
        o[key] = desc.value;
        return o;
      };
      ObjectFreeze = function(obj2) {
        return obj2;
      };
      ObjectGetPrototypeOf = function(obj2) {
        try {
          return Object(obj2).constructor.prototype;
        } catch (e) {
          return proto;
        }
      };
      ArrayIsArray = function(obj2) {
        try {
          return str.call(obj2) === "[object Array]";
        } catch (e) {
          return false;
        }
      };
      module2.exports = {
        isArray: ArrayIsArray,
        keys: ObjectKeys,
        names: ObjectKeys,
        defineProperty: ObjectDefineProperty,
        getDescriptor: ObjectGetDescriptor,
        freeze: ObjectFreeze,
        getPrototypeOf: ObjectGetPrototypeOf,
        isES5,
        propertyIsWritable: function() {
          return true;
        }
      };
    }
    var has;
    var str;
    var proto;
    var ObjectKeys;
    var ObjectGetDescriptor;
    var ObjectDefineProperty;
    var ObjectFreeze;
    var ObjectGetPrototypeOf;
    var ArrayIsArray;
  }
});

// node_modules/bluebird/js/release/util.js
var require_util = __commonJS({
  "node_modules/bluebird/js/release/util.js"(exports, module) {
    "use strict";
    var es5 = require_es5();
    var canEvaluate = typeof navigator == "undefined";
    var errorObj = { e: {} };
    var tryCatchTarget;
    var globalObject = typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : typeof global !== "undefined" ? global : exports !== void 0 ? exports : null;
    function tryCatcher() {
      try {
        var target = tryCatchTarget;
        tryCatchTarget = null;
        return target.apply(this, arguments);
      } catch (e) {
        errorObj.e = e;
        return errorObj;
      }
    }
    function tryCatch(fn) {
      tryCatchTarget = fn;
      return tryCatcher;
    }
    var inherits = function(Child, Parent) {
      var hasProp = {}.hasOwnProperty;
      function T() {
        this.constructor = Child;
        this.constructor$ = Parent;
        for (var propertyName in Parent.prototype) {
          if (hasProp.call(Parent.prototype, propertyName) && propertyName.charAt(propertyName.length - 1) !== "$") {
            this[propertyName + "$"] = Parent.prototype[propertyName];
          }
        }
      }
      T.prototype = Parent.prototype;
      Child.prototype = new T();
      return Child.prototype;
    };
    function isPrimitive(val) {
      return val == null || val === true || val === false || typeof val === "string" || typeof val === "number";
    }
    function isObject(value) {
      return typeof value === "function" || typeof value === "object" && value !== null;
    }
    function maybeWrapAsError(maybeError) {
      if (!isPrimitive(maybeError)) return maybeError;
      return new Error(safeToString(maybeError));
    }
    function withAppended(target, appendee) {
      var len = target.length;
      var ret2 = new Array(len + 1);
      var i;
      for (i = 0; i < len; ++i) {
        ret2[i] = target[i];
      }
      ret2[i] = appendee;
      return ret2;
    }
    function getDataPropertyOrDefault(obj2, key, defaultValue) {
      if (es5.isES5) {
        var desc = Object.getOwnPropertyDescriptor(obj2, key);
        if (desc != null) {
          return desc.get == null && desc.set == null ? desc.value : defaultValue;
        }
      } else {
        return {}.hasOwnProperty.call(obj2, key) ? obj2[key] : void 0;
      }
    }
    function notEnumerableProp(obj2, name, value) {
      if (isPrimitive(obj2)) return obj2;
      var descriptor = {
        value,
        configurable: true,
        enumerable: false,
        writable: true
      };
      es5.defineProperty(obj2, name, descriptor);
      return obj2;
    }
    function thrower(r) {
      throw r;
    }
    var inheritedDataKeys = (function() {
      var excludedPrototypes = [
        Array.prototype,
        Object.prototype,
        Function.prototype
      ];
      var isExcludedProto = function(val) {
        for (var i = 0; i < excludedPrototypes.length; ++i) {
          if (excludedPrototypes[i] === val) {
            return true;
          }
        }
        return false;
      };
      if (es5.isES5) {
        var getKeys = Object.getOwnPropertyNames;
        return function(obj2) {
          var ret2 = [];
          var visitedKeys = /* @__PURE__ */ Object.create(null);
          while (obj2 != null && !isExcludedProto(obj2)) {
            var keys;
            try {
              keys = getKeys(obj2);
            } catch (e) {
              return ret2;
            }
            for (var i = 0; i < keys.length; ++i) {
              var key = keys[i];
              if (visitedKeys[key]) continue;
              visitedKeys[key] = true;
              var desc = Object.getOwnPropertyDescriptor(obj2, key);
              if (desc != null && desc.get == null && desc.set == null) {
                ret2.push(key);
              }
            }
            obj2 = es5.getPrototypeOf(obj2);
          }
          return ret2;
        };
      } else {
        var hasProp = {}.hasOwnProperty;
        return function(obj2) {
          if (isExcludedProto(obj2)) return [];
          var ret2 = [];
          enumeration: for (var key in obj2) {
            if (hasProp.call(obj2, key)) {
              ret2.push(key);
            } else {
              for (var i = 0; i < excludedPrototypes.length; ++i) {
                if (hasProp.call(excludedPrototypes[i], key)) {
                  continue enumeration;
                }
              }
              ret2.push(key);
            }
          }
          return ret2;
        };
      }
    })();
    var thisAssignmentPattern = /this\s*\.\s*\S+\s*=/;
    function isClass(fn) {
      try {
        if (typeof fn === "function") {
          var keys = es5.names(fn.prototype);
          var hasMethods = es5.isES5 && keys.length > 1;
          var hasMethodsOtherThanConstructor = keys.length > 0 && !(keys.length === 1 && keys[0] === "constructor");
          var hasThisAssignmentAndStaticMethods = thisAssignmentPattern.test(fn + "") && es5.names(fn).length > 0;
          if (hasMethods || hasMethodsOtherThanConstructor || hasThisAssignmentAndStaticMethods) {
            return true;
          }
        }
        return false;
      } catch (e) {
        return false;
      }
    }
    function toFastProperties(obj) {
      function FakeConstructor() {
      }
      FakeConstructor.prototype = obj;
      var receiver = new FakeConstructor();
      function ic() {
        return typeof receiver.foo;
      }
      ic();
      ic();
      return obj;
      eval(obj);
    }
    var rident = /^[a-z$_][a-z$_0-9]*$/i;
    function isIdentifier(str) {
      return rident.test(str);
    }
    function filledRange(count, prefix, suffix) {
      var ret2 = new Array(count);
      for (var i = 0; i < count; ++i) {
        ret2[i] = prefix + i + suffix;
      }
      return ret2;
    }
    function safeToString(obj2) {
      try {
        return obj2 + "";
      } catch (e) {
        return "[no string representation]";
      }
    }
    function isError(obj2) {
      return obj2 instanceof Error || obj2 !== null && typeof obj2 === "object" && typeof obj2.message === "string" && typeof obj2.name === "string";
    }
    function markAsOriginatingFromRejection(e) {
      try {
        notEnumerableProp(e, "isOperational", true);
      } catch (ignore) {
      }
    }
    function originatesFromRejection(e) {
      if (e == null) return false;
      return e instanceof Error["__BluebirdErrorTypes__"].OperationalError || e["isOperational"] === true;
    }
    function canAttachTrace(obj2) {
      return isError(obj2) && es5.propertyIsWritable(obj2, "stack");
    }
    var ensureErrorObject = (function() {
      if (!("stack" in new Error())) {
        return function(value) {
          if (canAttachTrace(value)) return value;
          try {
            throw new Error(safeToString(value));
          } catch (err) {
            return err;
          }
        };
      } else {
        return function(value) {
          if (canAttachTrace(value)) return value;
          return new Error(safeToString(value));
        };
      }
    })();
    function classString(obj2) {
      return {}.toString.call(obj2);
    }
    function copyDescriptors(from, to, filter) {
      var keys = es5.names(from);
      for (var i = 0; i < keys.length; ++i) {
        var key = keys[i];
        if (filter(key)) {
          try {
            es5.defineProperty(to, key, es5.getDescriptor(from, key));
          } catch (ignore) {
          }
        }
      }
    }
    var asArray = function(v) {
      if (es5.isArray(v)) {
        return v;
      }
      return null;
    };
    if (typeof Symbol !== "undefined" && Symbol.iterator) {
      ArrayFrom = typeof Array.from === "function" ? function(v) {
        return Array.from(v);
      } : function(v) {
        var ret2 = [];
        var it = v[Symbol.iterator]();
        var itResult;
        while (!(itResult = it.next()).done) {
          ret2.push(itResult.value);
        }
        return ret2;
      };
      asArray = function(v) {
        if (es5.isArray(v)) {
          return v;
        } else if (v != null && typeof v[Symbol.iterator] === "function") {
          return ArrayFrom(v);
        }
        return null;
      };
    }
    var ArrayFrom;
    var isNode = typeof process !== "undefined" && classString(process).toLowerCase() === "[object process]";
    var hasEnvVariables = typeof process !== "undefined" && typeof process.env !== "undefined";
    function env(key) {
      return hasEnvVariables ? process.env[key] : void 0;
    }
    function getNativePromise() {
      if (typeof Promise === "function") {
        try {
          var promise = new Promise(function() {
          });
          if (classString(promise) === "[object Promise]") {
            return Promise;
          }
        } catch (e) {
        }
      }
    }
    var reflectHandler;
    function contextBind(ctx, cb) {
      if (ctx === null || typeof cb !== "function" || cb === reflectHandler) {
        return cb;
      }
      if (ctx.domain !== null) {
        cb = ctx.domain.bind(cb);
      }
      var async = ctx.async;
      if (async !== null) {
        var old = cb;
        cb = function() {
          var $_len = arguments.length + 2;
          var args = new Array($_len);
          for (var $_i = 2; $_i < $_len; ++$_i) {
            args[$_i] = arguments[$_i - 2];
          }
          ;
          args[0] = old;
          args[1] = this;
          return async.runInAsyncScope.apply(async, args);
        };
      }
      return cb;
    }
    var ret = {
      setReflectHandler: function(fn) {
        reflectHandler = fn;
      },
      isClass,
      isIdentifier,
      inheritedDataKeys,
      getDataPropertyOrDefault,
      thrower,
      isArray: es5.isArray,
      asArray,
      notEnumerableProp,
      isPrimitive,
      isObject,
      isError,
      canEvaluate,
      errorObj,
      tryCatch,
      inherits,
      withAppended,
      maybeWrapAsError,
      toFastProperties,
      filledRange,
      toString: safeToString,
      canAttachTrace,
      ensureErrorObject,
      originatesFromRejection,
      markAsOriginatingFromRejection,
      classString,
      copyDescriptors,
      isNode,
      hasEnvVariables,
      env,
      global: globalObject,
      getNativePromise,
      contextBind
    };
    ret.isRecentNode = ret.isNode && (function() {
      var version;
      if (process.versions && process.versions.node) {
        version = process.versions.node.split(".").map(Number);
      } else if (process.version) {
        version = process.version.split(".").map(Number);
      }
      return version[0] === 0 && version[1] > 10 || version[0] > 0;
    })();
    ret.nodeSupportsAsyncResource = ret.isNode && (function() {
      var supportsAsync = false;
      try {
        var res = require("async_hooks").AsyncResource;
        supportsAsync = typeof res.prototype.runInAsyncScope === "function";
      } catch (e) {
        supportsAsync = false;
      }
      return supportsAsync;
    })();
    if (ret.isNode) ret.toFastProperties(process);
    try {
      throw new Error();
    } catch (e) {
      ret.lastLineError = e;
    }
    module.exports = ret;
  }
});

// node_modules/bluebird/js/release/schedule.js
var require_schedule = __commonJS({
  "node_modules/bluebird/js/release/schedule.js"(exports2, module2) {
    "use strict";
    var util = require_util();
    var schedule;
    var noAsyncScheduler = function() {
      throw new Error("No async scheduler available\n\n    See http://goo.gl/MqrFmX\n");
    };
    var NativePromise = util.getNativePromise();
    if (util.isNode && typeof MutationObserver === "undefined") {
      GlobalSetImmediate = global.setImmediate;
      ProcessNextTick = process.nextTick;
      schedule = util.isRecentNode ? function(fn) {
        GlobalSetImmediate.call(global, fn);
      } : function(fn) {
        ProcessNextTick.call(process, fn);
      };
    } else if (typeof NativePromise === "function" && typeof NativePromise.resolve === "function") {
      nativePromise = NativePromise.resolve();
      schedule = function(fn) {
        nativePromise.then(fn);
      };
    } else if (typeof MutationObserver !== "undefined" && !(typeof window !== "undefined" && window.navigator && (window.navigator.standalone || window.cordova)) && "classList" in document.documentElement) {
      schedule = (function() {
        var div = document.createElement("div");
        var opts = { attributes: true };
        var toggleScheduled = false;
        var div2 = document.createElement("div");
        var o2 = new MutationObserver(function() {
          div.classList.toggle("foo");
          toggleScheduled = false;
        });
        o2.observe(div2, opts);
        var scheduleToggle = function() {
          if (toggleScheduled) return;
          toggleScheduled = true;
          div2.classList.toggle("foo");
        };
        return function schedule2(fn) {
          var o = new MutationObserver(function() {
            o.disconnect();
            fn();
          });
          o.observe(div, opts);
          scheduleToggle();
        };
      })();
    } else if (typeof setImmediate !== "undefined") {
      schedule = function(fn) {
        setImmediate(fn);
      };
    } else if (typeof setTimeout !== "undefined") {
      schedule = function(fn) {
        setTimeout(fn, 0);
      };
    } else {
      schedule = noAsyncScheduler;
    }
    var GlobalSetImmediate;
    var ProcessNextTick;
    var nativePromise;
    module2.exports = schedule;
  }
});

// node_modules/bluebird/js/release/queue.js
var require_queue = __commonJS({
  "node_modules/bluebird/js/release/queue.js"(exports2, module2) {
    "use strict";
    function arrayMove(src, srcIndex, dst, dstIndex, len) {
      for (var j = 0; j < len; ++j) {
        dst[j + dstIndex] = src[j + srcIndex];
        src[j + srcIndex] = void 0;
      }
    }
    function Queue(capacity) {
      this._capacity = capacity;
      this._length = 0;
      this._front = 0;
    }
    Queue.prototype._willBeOverCapacity = function(size) {
      return this._capacity < size;
    };
    Queue.prototype._pushOne = function(arg) {
      var length = this.length();
      this._checkCapacity(length + 1);
      var i = this._front + length & this._capacity - 1;
      this[i] = arg;
      this._length = length + 1;
    };
    Queue.prototype.push = function(fn, receiver2, arg) {
      var length = this.length() + 3;
      if (this._willBeOverCapacity(length)) {
        this._pushOne(fn);
        this._pushOne(receiver2);
        this._pushOne(arg);
        return;
      }
      var j = this._front + length - 3;
      this._checkCapacity(length);
      var wrapMask = this._capacity - 1;
      this[j + 0 & wrapMask] = fn;
      this[j + 1 & wrapMask] = receiver2;
      this[j + 2 & wrapMask] = arg;
      this._length = length;
    };
    Queue.prototype.shift = function() {
      var front = this._front, ret2 = this[front];
      this[front] = void 0;
      this._front = front + 1 & this._capacity - 1;
      this._length--;
      return ret2;
    };
    Queue.prototype.length = function() {
      return this._length;
    };
    Queue.prototype._checkCapacity = function(size) {
      if (this._capacity < size) {
        this._resizeTo(this._capacity << 1);
      }
    };
    Queue.prototype._resizeTo = function(capacity) {
      var oldCapacity = this._capacity;
      this._capacity = capacity;
      var front = this._front;
      var length = this._length;
      var moveItemsCount = front + length & oldCapacity - 1;
      arrayMove(this, 0, this, oldCapacity, moveItemsCount);
    };
    module2.exports = Queue;
  }
});

// node_modules/bluebird/js/release/async.js
var require_async2 = __commonJS({
  "node_modules/bluebird/js/release/async.js"(exports2, module2) {
    "use strict";
    var firstLineError;
    try {
      throw new Error();
    } catch (e) {
      firstLineError = e;
    }
    var schedule = require_schedule();
    var Queue = require_queue();
    function Async() {
      this._customScheduler = false;
      this._isTickUsed = false;
      this._lateQueue = new Queue(16);
      this._normalQueue = new Queue(16);
      this._haveDrainedQueues = false;
      var self2 = this;
      this.drainQueues = function() {
        self2._drainQueues();
      };
      this._schedule = schedule;
    }
    Async.prototype.setScheduler = function(fn) {
      var prev = this._schedule;
      this._schedule = fn;
      this._customScheduler = true;
      return prev;
    };
    Async.prototype.hasCustomScheduler = function() {
      return this._customScheduler;
    };
    Async.prototype.haveItemsQueued = function() {
      return this._isTickUsed || this._haveDrainedQueues;
    };
    Async.prototype.fatalError = function(e, isNode2) {
      if (isNode2) {
        process.stderr.write("Fatal " + (e instanceof Error ? e.stack : e) + "\n");
        process.exit(2);
      } else {
        this.throwLater(e);
      }
    };
    Async.prototype.throwLater = function(fn, arg) {
      if (arguments.length === 1) {
        arg = fn;
        fn = function() {
          throw arg;
        };
      }
      if (typeof setTimeout !== "undefined") {
        setTimeout(function() {
          fn(arg);
        }, 0);
      } else try {
        this._schedule(function() {
          fn(arg);
        });
      } catch (e) {
        throw new Error("No async scheduler available\n\n    See http://goo.gl/MqrFmX\n");
      }
    };
    function AsyncInvokeLater(fn, receiver2, arg) {
      this._lateQueue.push(fn, receiver2, arg);
      this._queueTick();
    }
    function AsyncInvoke(fn, receiver2, arg) {
      this._normalQueue.push(fn, receiver2, arg);
      this._queueTick();
    }
    function AsyncSettlePromises(promise) {
      this._normalQueue._pushOne(promise);
      this._queueTick();
    }
    Async.prototype.invokeLater = AsyncInvokeLater;
    Async.prototype.invoke = AsyncInvoke;
    Async.prototype.settlePromises = AsyncSettlePromises;
    function _drainQueue(queue) {
      while (queue.length() > 0) {
        _drainQueueStep(queue);
      }
    }
    function _drainQueueStep(queue) {
      var fn = queue.shift();
      if (typeof fn !== "function") {
        fn._settlePromises();
      } else {
        var receiver2 = queue.shift();
        var arg = queue.shift();
        fn.call(receiver2, arg);
      }
    }
    Async.prototype._drainQueues = function() {
      _drainQueue(this._normalQueue);
      this._reset();
      this._haveDrainedQueues = true;
      _drainQueue(this._lateQueue);
    };
    Async.prototype._queueTick = function() {
      if (!this._isTickUsed) {
        this._isTickUsed = true;
        this._schedule(this.drainQueues);
      }
    };
    Async.prototype._reset = function() {
      this._isTickUsed = false;
    };
    module2.exports = Async;
    module2.exports.firstLineError = firstLineError;
  }
});

// node_modules/bluebird/js/release/errors.js
var require_errors = __commonJS({
  "node_modules/bluebird/js/release/errors.js"(exports2, module2) {
    "use strict";
    var es52 = require_es5();
    var Objectfreeze = es52.freeze;
    var util = require_util();
    var inherits2 = util.inherits;
    var notEnumerableProp2 = util.notEnumerableProp;
    function subError(nameProperty, defaultMessage) {
      function SubError(message) {
        if (!(this instanceof SubError)) return new SubError(message);
        notEnumerableProp2(
          this,
          "message",
          typeof message === "string" ? message : defaultMessage
        );
        notEnumerableProp2(this, "name", nameProperty);
        if (Error.captureStackTrace) {
          Error.captureStackTrace(this, this.constructor);
        } else {
          Error.call(this);
        }
      }
      inherits2(SubError, Error);
      return SubError;
    }
    var _TypeError;
    var _RangeError;
    var Warning = subError("Warning", "warning");
    var CancellationError = subError("CancellationError", "cancellation error");
    var TimeoutError = subError("TimeoutError", "timeout error");
    var AggregateError = subError("AggregateError", "aggregate error");
    try {
      _TypeError = TypeError;
      _RangeError = RangeError;
    } catch (e) {
      _TypeError = subError("TypeError", "type error");
      _RangeError = subError("RangeError", "range error");
    }
    var methods = "join pop push shift unshift slice filter forEach some every map indexOf lastIndexOf reduce reduceRight sort reverse".split(" ");
    for (i = 0; i < methods.length; ++i) {
      if (typeof Array.prototype[methods[i]] === "function") {
        AggregateError.prototype[methods[i]] = Array.prototype[methods[i]];
      }
    }
    var i;
    es52.defineProperty(AggregateError.prototype, "length", {
      value: 0,
      configurable: false,
      writable: true,
      enumerable: true
    });
    AggregateError.prototype["isOperational"] = true;
    var level = 0;
    AggregateError.prototype.toString = function() {
      var indent = Array(level * 4 + 1).join(" ");
      var ret2 = "\n" + indent + "AggregateError of:\n";
      level++;
      indent = Array(level * 4 + 1).join(" ");
      for (var i2 = 0; i2 < this.length; ++i2) {
        var str = this[i2] === this ? "[Circular AggregateError]" : this[i2] + "";
        var lines = str.split("\n");
        for (var j = 0; j < lines.length; ++j) {
          lines[j] = indent + lines[j];
        }
        str = lines.join("\n");
        ret2 += str + "\n";
      }
      level--;
      return ret2;
    };
    function OperationalError(message) {
      if (!(this instanceof OperationalError))
        return new OperationalError(message);
      notEnumerableProp2(this, "name", "OperationalError");
      notEnumerableProp2(this, "message", message);
      this.cause = message;
      this["isOperational"] = true;
      if (message instanceof Error) {
        notEnumerableProp2(this, "message", message.message);
        notEnumerableProp2(this, "stack", message.stack);
      } else if (Error.captureStackTrace) {
        Error.captureStackTrace(this, this.constructor);
      }
    }
    inherits2(OperationalError, Error);
    var errorTypes = Error["__BluebirdErrorTypes__"];
    if (!errorTypes) {
      errorTypes = Objectfreeze({
        CancellationError,
        TimeoutError,
        OperationalError,
        RejectionError: OperationalError,
        AggregateError
      });
      es52.defineProperty(Error, "__BluebirdErrorTypes__", {
        value: errorTypes,
        writable: false,
        enumerable: false,
        configurable: false
      });
    }
    module2.exports = {
      Error,
      TypeError: _TypeError,
      RangeError: _RangeError,
      CancellationError: errorTypes.CancellationError,
      OperationalError: errorTypes.OperationalError,
      TimeoutError: errorTypes.TimeoutError,
      AggregateError: errorTypes.AggregateError,
      Warning
    };
  }
});

// node_modules/bluebird/js/release/thenables.js
var require_thenables = __commonJS({
  "node_modules/bluebird/js/release/thenables.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL) {
      var util = require_util();
      var errorObj2 = util.errorObj;
      var isObject2 = util.isObject;
      function tryConvertToPromise(obj2, context) {
        if (isObject2(obj2)) {
          if (obj2 instanceof Promise2) return obj2;
          var then = getThen(obj2);
          if (then === errorObj2) {
            if (context) context._pushContext();
            var ret2 = Promise2.reject(then.e);
            if (context) context._popContext();
            return ret2;
          } else if (typeof then === "function") {
            if (isAnyBluebirdPromise(obj2)) {
              var ret2 = new Promise2(INTERNAL);
              obj2._then(
                ret2._fulfill,
                ret2._reject,
                void 0,
                ret2,
                null
              );
              return ret2;
            }
            return doThenable(obj2, then, context);
          }
        }
        return obj2;
      }
      function doGetThen(obj2) {
        return obj2.then;
      }
      function getThen(obj2) {
        try {
          return doGetThen(obj2);
        } catch (e) {
          errorObj2.e = e;
          return errorObj2;
        }
      }
      var hasProp = {}.hasOwnProperty;
      function isAnyBluebirdPromise(obj2) {
        try {
          return hasProp.call(obj2, "_promise0");
        } catch (e) {
          return false;
        }
      }
      function doThenable(x, then, context) {
        var promise = new Promise2(INTERNAL);
        var ret2 = promise;
        if (context) context._pushContext();
        promise._captureStackTrace();
        if (context) context._popContext();
        var synchronous = true;
        var result = util.tryCatch(then).call(x, resolve, reject);
        synchronous = false;
        if (promise && result === errorObj2) {
          promise._rejectCallback(result.e, true, true);
          promise = null;
        }
        function resolve(value) {
          if (!promise) return;
          promise._resolveCallback(value);
          promise = null;
        }
        function reject(reason) {
          if (!promise) return;
          promise._rejectCallback(reason, synchronous, true);
          promise = null;
        }
        return ret2;
      }
      return tryConvertToPromise;
    };
  }
});

// node_modules/bluebird/js/release/promise_array.js
var require_promise_array = __commonJS({
  "node_modules/bluebird/js/release/promise_array.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL, tryConvertToPromise, apiRejection, Proxyable) {
      var util = require_util();
      var isArray = util.isArray;
      function toResolutionValue(val) {
        switch (val) {
          case -2:
            return [];
          case -3:
            return {};
          case -6:
            return /* @__PURE__ */ new Map();
        }
      }
      function PromiseArray(values) {
        var promise = this._promise = new Promise2(INTERNAL);
        if (values instanceof Promise2) {
          promise._propagateFrom(values, 3);
          values.suppressUnhandledRejections();
        }
        promise._setOnCancel(this);
        this._values = values;
        this._length = 0;
        this._totalResolved = 0;
        this._init(void 0, -2);
      }
      util.inherits(PromiseArray, Proxyable);
      PromiseArray.prototype.length = function() {
        return this._length;
      };
      PromiseArray.prototype.promise = function() {
        return this._promise;
      };
      PromiseArray.prototype._init = function init(_, resolveValueIfEmpty) {
        var values = tryConvertToPromise(this._values, this._promise);
        if (values instanceof Promise2) {
          values = values._target();
          var bitField = values._bitField;
          ;
          this._values = values;
          if ((bitField & 50397184) === 0) {
            this._promise._setAsyncGuaranteed();
            return values._then(
              init,
              this._reject,
              void 0,
              this,
              resolveValueIfEmpty
            );
          } else if ((bitField & 33554432) !== 0) {
            values = values._value();
          } else if ((bitField & 16777216) !== 0) {
            return this._reject(values._reason());
          } else {
            return this._cancel();
          }
        }
        values = util.asArray(values);
        if (values === null) {
          var err = apiRejection(
            "expecting an array or an iterable object but got " + util.classString(values)
          ).reason();
          this._promise._rejectCallback(err, false);
          return;
        }
        if (values.length === 0) {
          if (resolveValueIfEmpty === -5) {
            this._resolveEmptyArray();
          } else {
            this._resolve(toResolutionValue(resolveValueIfEmpty));
          }
          return;
        }
        this._iterate(values);
      };
      PromiseArray.prototype._iterate = function(values) {
        var len = this.getActualLength(values.length);
        this._length = len;
        this._values = this.shouldCopyValues() ? new Array(len) : this._values;
        var result = this._promise;
        var isResolved = false;
        var bitField = null;
        for (var i = 0; i < len; ++i) {
          var maybePromise = tryConvertToPromise(values[i], result);
          if (maybePromise instanceof Promise2) {
            maybePromise = maybePromise._target();
            bitField = maybePromise._bitField;
          } else {
            bitField = null;
          }
          if (isResolved) {
            if (bitField !== null) {
              maybePromise.suppressUnhandledRejections();
            }
          } else if (bitField !== null) {
            if ((bitField & 50397184) === 0) {
              maybePromise._proxy(this, i);
              this._values[i] = maybePromise;
            } else if ((bitField & 33554432) !== 0) {
              isResolved = this._promiseFulfilled(maybePromise._value(), i);
            } else if ((bitField & 16777216) !== 0) {
              isResolved = this._promiseRejected(maybePromise._reason(), i);
            } else {
              isResolved = this._promiseCancelled(i);
            }
          } else {
            isResolved = this._promiseFulfilled(maybePromise, i);
          }
        }
        if (!isResolved) result._setAsyncGuaranteed();
      };
      PromiseArray.prototype._isResolved = function() {
        return this._values === null;
      };
      PromiseArray.prototype._resolve = function(value) {
        this._values = null;
        this._promise._fulfill(value);
      };
      PromiseArray.prototype._cancel = function() {
        if (this._isResolved() || !this._promise._isCancellable()) return;
        this._values = null;
        this._promise._cancel();
      };
      PromiseArray.prototype._reject = function(reason) {
        this._values = null;
        this._promise._rejectCallback(reason, false);
      };
      PromiseArray.prototype._promiseFulfilled = function(value, index) {
        this._values[index] = value;
        var totalResolved = ++this._totalResolved;
        if (totalResolved >= this._length) {
          this._resolve(this._values);
          return true;
        }
        return false;
      };
      PromiseArray.prototype._promiseCancelled = function() {
        this._cancel();
        return true;
      };
      PromiseArray.prototype._promiseRejected = function(reason) {
        this._totalResolved++;
        this._reject(reason);
        return true;
      };
      PromiseArray.prototype._resultCancelled = function() {
        if (this._isResolved()) return;
        var values = this._values;
        this._cancel();
        if (values instanceof Promise2) {
          values.cancel();
        } else {
          for (var i = 0; i < values.length; ++i) {
            if (values[i] instanceof Promise2) {
              values[i].cancel();
            }
          }
        }
      };
      PromiseArray.prototype.shouldCopyValues = function() {
        return true;
      };
      PromiseArray.prototype.getActualLength = function(len) {
        return len;
      };
      return PromiseArray;
    };
  }
});

// node_modules/bluebird/js/release/context.js
var require_context = __commonJS({
  "node_modules/bluebird/js/release/context.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2) {
      var longStackTraces = false;
      var contextStack = [];
      Promise2.prototype._promiseCreated = function() {
      };
      Promise2.prototype._pushContext = function() {
      };
      Promise2.prototype._popContext = function() {
        return null;
      };
      Promise2._peekContext = Promise2.prototype._peekContext = function() {
      };
      function Context() {
        this._trace = new Context.CapturedTrace(peekContext());
      }
      Context.prototype._pushContext = function() {
        if (this._trace !== void 0) {
          this._trace._promiseCreated = null;
          contextStack.push(this._trace);
        }
      };
      Context.prototype._popContext = function() {
        if (this._trace !== void 0) {
          var trace = contextStack.pop();
          var ret2 = trace._promiseCreated;
          trace._promiseCreated = null;
          return ret2;
        }
        return null;
      };
      function createContext() {
        if (longStackTraces) return new Context();
      }
      function peekContext() {
        var lastIndex = contextStack.length - 1;
        if (lastIndex >= 0) {
          return contextStack[lastIndex];
        }
        return void 0;
      }
      Context.CapturedTrace = null;
      Context.create = createContext;
      Context.deactivateLongStackTraces = function() {
      };
      Context.activateLongStackTraces = function() {
        var Promise_pushContext = Promise2.prototype._pushContext;
        var Promise_popContext = Promise2.prototype._popContext;
        var Promise_PeekContext = Promise2._peekContext;
        var Promise_peekContext = Promise2.prototype._peekContext;
        var Promise_promiseCreated = Promise2.prototype._promiseCreated;
        Context.deactivateLongStackTraces = function() {
          Promise2.prototype._pushContext = Promise_pushContext;
          Promise2.prototype._popContext = Promise_popContext;
          Promise2._peekContext = Promise_PeekContext;
          Promise2.prototype._peekContext = Promise_peekContext;
          Promise2.prototype._promiseCreated = Promise_promiseCreated;
          longStackTraces = false;
        };
        longStackTraces = true;
        Promise2.prototype._pushContext = Context.prototype._pushContext;
        Promise2.prototype._popContext = Context.prototype._popContext;
        Promise2._peekContext = Promise2.prototype._peekContext = peekContext;
        Promise2.prototype._promiseCreated = function() {
          var ctx = this._peekContext();
          if (ctx && ctx._promiseCreated == null) ctx._promiseCreated = this;
        };
      };
      return Context;
    };
  }
});

// node_modules/bluebird/js/release/debuggability.js
var require_debuggability = __commonJS({
  "node_modules/bluebird/js/release/debuggability.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, Context, enableAsyncHooks, disableAsyncHooks) {
      var async = Promise2._async;
      var Warning = require_errors().Warning;
      var util = require_util();
      var es52 = require_es5();
      var canAttachTrace2 = util.canAttachTrace;
      var unhandledRejectionHandled;
      var possiblyUnhandledRejection;
      var bluebirdFramePattern = /[\\\/]bluebird[\\\/]js[\\\/](release|debug|instrumented)/;
      var nodeFramePattern = /\((?:timers\.js):\d+:\d+\)/;
      var parseLinePattern = /[\/<\(](.+?):(\d+):(\d+)\)?\s*$/;
      var stackFramePattern = null;
      var formatStack = null;
      var indentStackFrames = false;
      var printWarning;
      var debugging = !!(util.env("BLUEBIRD_DEBUG") != 0 && (util.env("BLUEBIRD_DEBUG") || util.env("NODE_ENV") === "development"));
      var warnings = !!(util.env("BLUEBIRD_WARNINGS") != 0 && (debugging || util.env("BLUEBIRD_WARNINGS")));
      var longStackTraces = !!(util.env("BLUEBIRD_LONG_STACK_TRACES") != 0 && (debugging || util.env("BLUEBIRD_LONG_STACK_TRACES")));
      var wForgottenReturn = util.env("BLUEBIRD_W_FORGOTTEN_RETURN") != 0 && (warnings || !!util.env("BLUEBIRD_W_FORGOTTEN_RETURN"));
      var deferUnhandledRejectionCheck;
      (function() {
        var promises = [];
        function unhandledRejectionCheck() {
          for (var i = 0; i < promises.length; ++i) {
            promises[i]._notifyUnhandledRejection();
          }
          unhandledRejectionClear();
        }
        function unhandledRejectionClear() {
          promises.length = 0;
        }
        deferUnhandledRejectionCheck = function(promise) {
          promises.push(promise);
          setTimeout(unhandledRejectionCheck, 1);
        };
        es52.defineProperty(Promise2, "_unhandledRejectionCheck", {
          value: unhandledRejectionCheck
        });
        es52.defineProperty(Promise2, "_unhandledRejectionClear", {
          value: unhandledRejectionClear
        });
      })();
      Promise2.prototype.suppressUnhandledRejections = function() {
        var target = this._target();
        target._bitField = target._bitField & ~1048576 | 524288;
      };
      Promise2.prototype._ensurePossibleRejectionHandled = function() {
        if ((this._bitField & 524288) !== 0) return;
        this._setRejectionIsUnhandled();
        deferUnhandledRejectionCheck(this);
      };
      Promise2.prototype._notifyUnhandledRejectionIsHandled = function() {
        fireRejectionEvent(
          "rejectionHandled",
          unhandledRejectionHandled,
          void 0,
          this
        );
      };
      Promise2.prototype._setReturnedNonUndefined = function() {
        this._bitField = this._bitField | 268435456;
      };
      Promise2.prototype._returnedNonUndefined = function() {
        return (this._bitField & 268435456) !== 0;
      };
      Promise2.prototype._notifyUnhandledRejection = function() {
        if (this._isRejectionUnhandled()) {
          var reason = this._settledValue();
          this._setUnhandledRejectionIsNotified();
          fireRejectionEvent(
            "unhandledRejection",
            possiblyUnhandledRejection,
            reason,
            this
          );
        }
      };
      Promise2.prototype._setUnhandledRejectionIsNotified = function() {
        this._bitField = this._bitField | 262144;
      };
      Promise2.prototype._unsetUnhandledRejectionIsNotified = function() {
        this._bitField = this._bitField & ~262144;
      };
      Promise2.prototype._isUnhandledRejectionNotified = function() {
        return (this._bitField & 262144) > 0;
      };
      Promise2.prototype._setRejectionIsUnhandled = function() {
        this._bitField = this._bitField | 1048576;
      };
      Promise2.prototype._unsetRejectionIsUnhandled = function() {
        this._bitField = this._bitField & ~1048576;
        if (this._isUnhandledRejectionNotified()) {
          this._unsetUnhandledRejectionIsNotified();
          this._notifyUnhandledRejectionIsHandled();
        }
      };
      Promise2.prototype._isRejectionUnhandled = function() {
        return (this._bitField & 1048576) > 0;
      };
      Promise2.prototype._warn = function(message, shouldUseOwnTrace, promise) {
        return warn(message, shouldUseOwnTrace, promise || this);
      };
      Promise2.onPossiblyUnhandledRejection = function(fn) {
        var context = Promise2._getContext();
        possiblyUnhandledRejection = util.contextBind(context, fn);
      };
      Promise2.onUnhandledRejectionHandled = function(fn) {
        var context = Promise2._getContext();
        unhandledRejectionHandled = util.contextBind(context, fn);
      };
      var disableLongStackTraces = function() {
      };
      Promise2.longStackTraces = function() {
        if (async.haveItemsQueued() && !config.longStackTraces) {
          throw new Error("cannot enable long stack traces after promises have been created\n\n    See http://goo.gl/MqrFmX\n");
        }
        if (!config.longStackTraces && longStackTracesIsSupported()) {
          var Promise_captureStackTrace = Promise2.prototype._captureStackTrace;
          var Promise_attachExtraTrace = Promise2.prototype._attachExtraTrace;
          var Promise_dereferenceTrace = Promise2.prototype._dereferenceTrace;
          config.longStackTraces = true;
          disableLongStackTraces = function() {
            if (async.haveItemsQueued() && !config.longStackTraces) {
              throw new Error("cannot enable long stack traces after promises have been created\n\n    See http://goo.gl/MqrFmX\n");
            }
            Promise2.prototype._captureStackTrace = Promise_captureStackTrace;
            Promise2.prototype._attachExtraTrace = Promise_attachExtraTrace;
            Promise2.prototype._dereferenceTrace = Promise_dereferenceTrace;
            Context.deactivateLongStackTraces();
            config.longStackTraces = false;
          };
          Promise2.prototype._captureStackTrace = longStackTracesCaptureStackTrace;
          Promise2.prototype._attachExtraTrace = longStackTracesAttachExtraTrace;
          Promise2.prototype._dereferenceTrace = longStackTracesDereferenceTrace;
          Context.activateLongStackTraces();
        }
      };
      Promise2.hasLongStackTraces = function() {
        return config.longStackTraces && longStackTracesIsSupported();
      };
      var legacyHandlers = {
        unhandledrejection: {
          before: function() {
            var ret2 = util.global.onunhandledrejection;
            util.global.onunhandledrejection = null;
            return ret2;
          },
          after: function(fn) {
            util.global.onunhandledrejection = fn;
          }
        },
        rejectionhandled: {
          before: function() {
            var ret2 = util.global.onrejectionhandled;
            util.global.onrejectionhandled = null;
            return ret2;
          },
          after: function(fn) {
            util.global.onrejectionhandled = fn;
          }
        }
      };
      var fireDomEvent = (function() {
        var dispatch = function(legacy, e) {
          if (legacy) {
            var fn;
            try {
              fn = legacy.before();
              return !util.global.dispatchEvent(e);
            } finally {
              legacy.after(fn);
            }
          } else {
            return !util.global.dispatchEvent(e);
          }
        };
        try {
          if (typeof CustomEvent === "function") {
            var event = new CustomEvent("CustomEvent");
            util.global.dispatchEvent(event);
            return function(name, event2) {
              name = name.toLowerCase();
              var eventData = {
                detail: event2,
                cancelable: true
              };
              var domEvent = new CustomEvent(name, eventData);
              es52.defineProperty(
                domEvent,
                "promise",
                { value: event2.promise }
              );
              es52.defineProperty(
                domEvent,
                "reason",
                { value: event2.reason }
              );
              return dispatch(legacyHandlers[name], domEvent);
            };
          } else if (typeof Event === "function") {
            var event = new Event("CustomEvent");
            util.global.dispatchEvent(event);
            return function(name, event2) {
              name = name.toLowerCase();
              var domEvent = new Event(name, {
                cancelable: true
              });
              domEvent.detail = event2;
              es52.defineProperty(domEvent, "promise", { value: event2.promise });
              es52.defineProperty(domEvent, "reason", { value: event2.reason });
              return dispatch(legacyHandlers[name], domEvent);
            };
          } else {
            var event = document.createEvent("CustomEvent");
            event.initCustomEvent("testingtheevent", false, true, {});
            util.global.dispatchEvent(event);
            return function(name, event2) {
              name = name.toLowerCase();
              var domEvent = document.createEvent("CustomEvent");
              domEvent.initCustomEvent(
                name,
                false,
                true,
                event2
              );
              return dispatch(legacyHandlers[name], domEvent);
            };
          }
        } catch (e) {
        }
        return function() {
          return false;
        };
      })();
      var fireGlobalEvent = (function() {
        if (util.isNode) {
          return function() {
            return process.emit.apply(process, arguments);
          };
        } else {
          if (!util.global) {
            return function() {
              return false;
            };
          }
          return function(name) {
            var methodName = "on" + name.toLowerCase();
            var method = util.global[methodName];
            if (!method) return false;
            method.apply(util.global, [].slice.call(arguments, 1));
            return true;
          };
        }
      })();
      function generatePromiseLifecycleEventObject(name, promise) {
        return { promise };
      }
      var eventToObjectGenerator = {
        promiseCreated: generatePromiseLifecycleEventObject,
        promiseFulfilled: generatePromiseLifecycleEventObject,
        promiseRejected: generatePromiseLifecycleEventObject,
        promiseResolved: generatePromiseLifecycleEventObject,
        promiseCancelled: generatePromiseLifecycleEventObject,
        promiseChained: function(name, promise, child) {
          return { promise, child };
        },
        warning: function(name, warning) {
          return { warning };
        },
        unhandledRejection: function(name, reason, promise) {
          return { reason, promise };
        },
        rejectionHandled: generatePromiseLifecycleEventObject
      };
      var activeFireEvent = function(name) {
        var globalEventFired = false;
        try {
          globalEventFired = fireGlobalEvent.apply(null, arguments);
        } catch (e) {
          async.throwLater(e);
          globalEventFired = true;
        }
        var domEventFired = false;
        try {
          domEventFired = fireDomEvent(
            name,
            eventToObjectGenerator[name].apply(null, arguments)
          );
        } catch (e) {
          async.throwLater(e);
          domEventFired = true;
        }
        return domEventFired || globalEventFired;
      };
      Promise2.config = function(opts) {
        opts = Object(opts);
        if ("longStackTraces" in opts) {
          if (opts.longStackTraces) {
            Promise2.longStackTraces();
          } else if (!opts.longStackTraces && Promise2.hasLongStackTraces()) {
            disableLongStackTraces();
          }
        }
        if ("warnings" in opts) {
          var warningsOption = opts.warnings;
          config.warnings = !!warningsOption;
          wForgottenReturn = config.warnings;
          if (util.isObject(warningsOption)) {
            if ("wForgottenReturn" in warningsOption) {
              wForgottenReturn = !!warningsOption.wForgottenReturn;
            }
          }
        }
        if ("cancellation" in opts && opts.cancellation && !config.cancellation) {
          if (async.haveItemsQueued()) {
            throw new Error(
              "cannot enable cancellation after promises are in use"
            );
          }
          Promise2.prototype._clearCancellationData = cancellationClearCancellationData;
          Promise2.prototype._propagateFrom = cancellationPropagateFrom;
          Promise2.prototype._onCancel = cancellationOnCancel;
          Promise2.prototype._setOnCancel = cancellationSetOnCancel;
          Promise2.prototype._attachCancellationCallback = cancellationAttachCancellationCallback;
          Promise2.prototype._execute = cancellationExecute;
          propagateFromFunction = cancellationPropagateFrom;
          config.cancellation = true;
        }
        if ("monitoring" in opts) {
          if (opts.monitoring && !config.monitoring) {
            config.monitoring = true;
            Promise2.prototype._fireEvent = activeFireEvent;
          } else if (!opts.monitoring && config.monitoring) {
            config.monitoring = false;
            Promise2.prototype._fireEvent = defaultFireEvent;
          }
        }
        if ("asyncHooks" in opts && util.nodeSupportsAsyncResource) {
          var prev = config.asyncHooks;
          var cur = !!opts.asyncHooks;
          if (prev !== cur) {
            config.asyncHooks = cur;
            if (cur) {
              enableAsyncHooks();
            } else {
              disableAsyncHooks();
            }
          }
        }
        return Promise2;
      };
      function defaultFireEvent() {
        return false;
      }
      Promise2.prototype._fireEvent = defaultFireEvent;
      Promise2.prototype._execute = function(executor, resolve, reject) {
        try {
          executor(resolve, reject);
        } catch (e) {
          return e;
        }
      };
      Promise2.prototype._onCancel = function() {
      };
      Promise2.prototype._setOnCancel = function(handler) {
        ;
      };
      Promise2.prototype._attachCancellationCallback = function(onCancel) {
        ;
      };
      Promise2.prototype._captureStackTrace = function() {
      };
      Promise2.prototype._attachExtraTrace = function() {
      };
      Promise2.prototype._dereferenceTrace = function() {
      };
      Promise2.prototype._clearCancellationData = function() {
      };
      Promise2.prototype._propagateFrom = function(parent, flags) {
        ;
        ;
      };
      function cancellationExecute(executor, resolve, reject) {
        var promise = this;
        try {
          executor(resolve, reject, function(onCancel) {
            if (typeof onCancel !== "function") {
              throw new TypeError("onCancel must be a function, got: " + util.toString(onCancel));
            }
            promise._attachCancellationCallback(onCancel);
          });
        } catch (e) {
          return e;
        }
      }
      function cancellationAttachCancellationCallback(onCancel) {
        if (!this._isCancellable()) return this;
        var previousOnCancel = this._onCancel();
        if (previousOnCancel !== void 0) {
          if (util.isArray(previousOnCancel)) {
            previousOnCancel.push(onCancel);
          } else {
            this._setOnCancel([previousOnCancel, onCancel]);
          }
        } else {
          this._setOnCancel(onCancel);
        }
      }
      function cancellationOnCancel() {
        return this._onCancelField;
      }
      function cancellationSetOnCancel(onCancel) {
        this._onCancelField = onCancel;
      }
      function cancellationClearCancellationData() {
        this._cancellationParent = void 0;
        this._onCancelField = void 0;
      }
      function cancellationPropagateFrom(parent, flags) {
        if ((flags & 1) !== 0) {
          this._cancellationParent = parent;
          var branchesRemainingToCancel = parent._branchesRemainingToCancel;
          if (branchesRemainingToCancel === void 0) {
            branchesRemainingToCancel = 0;
          }
          parent._branchesRemainingToCancel = branchesRemainingToCancel + 1;
        }
        if ((flags & 2) !== 0 && parent._isBound()) {
          this._setBoundTo(parent._boundTo);
        }
      }
      function bindingPropagateFrom(parent, flags) {
        if ((flags & 2) !== 0 && parent._isBound()) {
          this._setBoundTo(parent._boundTo);
        }
      }
      var propagateFromFunction = bindingPropagateFrom;
      function boundValueFunction() {
        var ret2 = this._boundTo;
        if (ret2 !== void 0) {
          if (ret2 instanceof Promise2) {
            if (ret2.isFulfilled()) {
              return ret2.value();
            } else {
              return void 0;
            }
          }
        }
        return ret2;
      }
      function longStackTracesCaptureStackTrace() {
        this._trace = new CapturedTrace(this._peekContext());
      }
      function longStackTracesAttachExtraTrace(error, ignoreSelf) {
        if (canAttachTrace2(error)) {
          var trace = this._trace;
          if (trace !== void 0) {
            if (ignoreSelf) trace = trace._parent;
          }
          if (trace !== void 0) {
            trace.attachExtraTrace(error);
          } else if (!error.__stackCleaned__) {
            var parsed = parseStackAndMessage(error);
            util.notEnumerableProp(
              error,
              "stack",
              parsed.message + "\n" + parsed.stack.join("\n")
            );
            util.notEnumerableProp(error, "__stackCleaned__", true);
          }
        }
      }
      function longStackTracesDereferenceTrace() {
        this._trace = void 0;
      }
      function checkForgottenReturns(returnValue, promiseCreated, name, promise, parent) {
        if (returnValue === void 0 && promiseCreated !== null && wForgottenReturn) {
          if (parent !== void 0 && parent._returnedNonUndefined()) return;
          if ((promise._bitField & 65535) === 0) return;
          if (name) name = name + " ";
          var handlerLine = "";
          var creatorLine = "";
          if (promiseCreated._trace) {
            var traceLines = promiseCreated._trace.stack.split("\n");
            var stack = cleanStack(traceLines);
            for (var i = stack.length - 1; i >= 0; --i) {
              var line = stack[i];
              if (!nodeFramePattern.test(line)) {
                var lineMatches = line.match(parseLinePattern);
                if (lineMatches) {
                  handlerLine = "at " + lineMatches[1] + ":" + lineMatches[2] + ":" + lineMatches[3] + " ";
                }
                break;
              }
            }
            if (stack.length > 0) {
              var firstUserLine = stack[0];
              for (var i = 0; i < traceLines.length; ++i) {
                if (traceLines[i] === firstUserLine) {
                  if (i > 0) {
                    creatorLine = "\n" + traceLines[i - 1];
                  }
                  break;
                }
              }
            }
          }
          var msg = "a promise was created in a " + name + "handler " + handlerLine + "but was not returned from it, see http://goo.gl/rRqMUw" + creatorLine;
          promise._warn(msg, true, promiseCreated);
        }
      }
      function deprecated(name, replacement) {
        var message = name + " is deprecated and will be removed in a future version.";
        if (replacement) message += " Use " + replacement + " instead.";
        return warn(message);
      }
      function warn(message, shouldUseOwnTrace, promise) {
        if (!config.warnings) return;
        var warning = new Warning(message);
        var ctx;
        if (shouldUseOwnTrace) {
          promise._attachExtraTrace(warning);
        } else if (config.longStackTraces && (ctx = Promise2._peekContext())) {
          ctx.attachExtraTrace(warning);
        } else {
          var parsed = parseStackAndMessage(warning);
          warning.stack = parsed.message + "\n" + parsed.stack.join("\n");
        }
        if (!activeFireEvent("warning", warning)) {
          formatAndLogError(warning, "", true);
        }
      }
      function reconstructStack(message, stacks) {
        for (var i = 0; i < stacks.length - 1; ++i) {
          stacks[i].push("From previous event:");
          stacks[i] = stacks[i].join("\n");
        }
        if (i < stacks.length) {
          stacks[i] = stacks[i].join("\n");
        }
        return message + "\n" + stacks.join("\n");
      }
      function removeDuplicateOrEmptyJumps(stacks) {
        for (var i = 0; i < stacks.length; ++i) {
          if (stacks[i].length === 0 || i + 1 < stacks.length && stacks[i][0] === stacks[i + 1][0]) {
            stacks.splice(i, 1);
            i--;
          }
        }
      }
      function removeCommonRoots(stacks) {
        var current = stacks[0];
        for (var i = 1; i < stacks.length; ++i) {
          var prev = stacks[i];
          var currentLastIndex = current.length - 1;
          var currentLastLine = current[currentLastIndex];
          var commonRootMeetPoint = -1;
          for (var j = prev.length - 1; j >= 0; --j) {
            if (prev[j] === currentLastLine) {
              commonRootMeetPoint = j;
              break;
            }
          }
          for (var j = commonRootMeetPoint; j >= 0; --j) {
            var line = prev[j];
            if (current[currentLastIndex] === line) {
              current.pop();
              currentLastIndex--;
            } else {
              break;
            }
          }
          current = prev;
        }
      }
      function cleanStack(stack) {
        var ret2 = [];
        for (var i = 0; i < stack.length; ++i) {
          var line = stack[i];
          var isTraceLine = "    (No stack trace)" === line || stackFramePattern.test(line);
          var isInternalFrame = isTraceLine && shouldIgnore(line);
          if (isTraceLine && !isInternalFrame) {
            if (indentStackFrames && line.charAt(0) !== " ") {
              line = "    " + line;
            }
            ret2.push(line);
          }
        }
        return ret2;
      }
      function stackFramesAsArray(error) {
        var stack = error.stack.replace(/\s+$/g, "").split("\n");
        for (var i = 0; i < stack.length; ++i) {
          var line = stack[i];
          if ("    (No stack trace)" === line || stackFramePattern.test(line)) {
            break;
          }
        }
        if (i > 0 && error.name != "SyntaxError") {
          stack = stack.slice(i);
        }
        return stack;
      }
      function parseStackAndMessage(error) {
        var stack = error.stack;
        var message = error.toString();
        stack = typeof stack === "string" && stack.length > 0 ? stackFramesAsArray(error) : ["    (No stack trace)"];
        return {
          message,
          stack: error.name == "SyntaxError" ? stack : cleanStack(stack)
        };
      }
      function formatAndLogError(error, title, isSoft) {
        if (typeof console !== "undefined") {
          var message;
          if (util.isObject(error)) {
            var stack = error.stack;
            message = title + formatStack(stack, error);
          } else {
            message = title + String(error);
          }
          if (typeof printWarning === "function") {
            printWarning(message, isSoft);
          } else if (typeof console.log === "function" || typeof console.log === "object") {
            console.log(message);
          }
        }
      }
      function fireRejectionEvent(name, localHandler, reason, promise) {
        var localEventFired = false;
        try {
          if (typeof localHandler === "function") {
            localEventFired = true;
            if (name === "rejectionHandled") {
              localHandler(promise);
            } else {
              localHandler(reason, promise);
            }
          }
        } catch (e) {
          async.throwLater(e);
        }
        if (name === "unhandledRejection") {
          if (!activeFireEvent(name, reason, promise) && !localEventFired) {
            formatAndLogError(reason, "Unhandled rejection ");
          }
        } else {
          activeFireEvent(name, promise);
        }
      }
      function formatNonError(obj2) {
        var str;
        if (typeof obj2 === "function") {
          str = "[function " + (obj2.name || "anonymous") + "]";
        } else {
          str = obj2 && typeof obj2.toString === "function" ? obj2.toString() : util.toString(obj2);
          var ruselessToString = /\[object [a-zA-Z0-9$_]+\]/;
          if (ruselessToString.test(str)) {
            try {
              var newStr = JSON.stringify(obj2);
              str = newStr;
            } catch (e) {
            }
          }
          if (str.length === 0) {
            str = "(empty array)";
          }
        }
        return "(<" + snip(str) + ">, no stack trace)";
      }
      function snip(str) {
        var maxChars = 41;
        if (str.length < maxChars) {
          return str;
        }
        return str.substr(0, maxChars - 3) + "...";
      }
      function longStackTracesIsSupported() {
        return typeof captureStackTrace === "function";
      }
      var shouldIgnore = function() {
        return false;
      };
      var parseLineInfoRegex = /[\/<\(]([^:\/]+):(\d+):(?:\d+)\)?\s*$/;
      function parseLineInfo(line) {
        var matches = line.match(parseLineInfoRegex);
        if (matches) {
          return {
            fileName: matches[1],
            line: parseInt(matches[2], 10)
          };
        }
      }
      function setBounds(firstLineError, lastLineError) {
        if (!longStackTracesIsSupported()) return;
        var firstStackLines = (firstLineError.stack || "").split("\n");
        var lastStackLines = (lastLineError.stack || "").split("\n");
        var firstIndex = -1;
        var lastIndex = -1;
        var firstFileName;
        var lastFileName;
        for (var i = 0; i < firstStackLines.length; ++i) {
          var result = parseLineInfo(firstStackLines[i]);
          if (result) {
            firstFileName = result.fileName;
            firstIndex = result.line;
            break;
          }
        }
        for (var i = 0; i < lastStackLines.length; ++i) {
          var result = parseLineInfo(lastStackLines[i]);
          if (result) {
            lastFileName = result.fileName;
            lastIndex = result.line;
            break;
          }
        }
        if (firstIndex < 0 || lastIndex < 0 || !firstFileName || !lastFileName || firstFileName !== lastFileName || firstIndex >= lastIndex) {
          return;
        }
        shouldIgnore = function(line) {
          if (bluebirdFramePattern.test(line)) return true;
          var info = parseLineInfo(line);
          if (info) {
            if (info.fileName === firstFileName && (firstIndex <= info.line && info.line <= lastIndex)) {
              return true;
            }
          }
          return false;
        };
      }
      function CapturedTrace(parent) {
        this._parent = parent;
        this._promisesCreated = 0;
        var length = this._length = 1 + (parent === void 0 ? 0 : parent._length);
        captureStackTrace(this, CapturedTrace);
        if (length > 32) this.uncycle();
      }
      util.inherits(CapturedTrace, Error);
      Context.CapturedTrace = CapturedTrace;
      CapturedTrace.prototype.uncycle = function() {
        var length = this._length;
        if (length < 2) return;
        var nodes = [];
        var stackToIndex = {};
        for (var i = 0, node = this; node !== void 0; ++i) {
          nodes.push(node);
          node = node._parent;
        }
        length = this._length = i;
        for (var i = length - 1; i >= 0; --i) {
          var stack = nodes[i].stack;
          if (stackToIndex[stack] === void 0) {
            stackToIndex[stack] = i;
          }
        }
        for (var i = 0; i < length; ++i) {
          var currentStack = nodes[i].stack;
          var index = stackToIndex[currentStack];
          if (index !== void 0 && index !== i) {
            if (index > 0) {
              nodes[index - 1]._parent = void 0;
              nodes[index - 1]._length = 1;
            }
            nodes[i]._parent = void 0;
            nodes[i]._length = 1;
            var cycleEdgeNode = i > 0 ? nodes[i - 1] : this;
            if (index < length - 1) {
              cycleEdgeNode._parent = nodes[index + 1];
              cycleEdgeNode._parent.uncycle();
              cycleEdgeNode._length = cycleEdgeNode._parent._length + 1;
            } else {
              cycleEdgeNode._parent = void 0;
              cycleEdgeNode._length = 1;
            }
            var currentChildLength = cycleEdgeNode._length + 1;
            for (var j = i - 2; j >= 0; --j) {
              nodes[j]._length = currentChildLength;
              currentChildLength++;
            }
            return;
          }
        }
      };
      CapturedTrace.prototype.attachExtraTrace = function(error) {
        if (error.__stackCleaned__) return;
        this.uncycle();
        var parsed = parseStackAndMessage(error);
        var message = parsed.message;
        var stacks = [parsed.stack];
        var trace = this;
        while (trace !== void 0) {
          stacks.push(cleanStack(trace.stack.split("\n")));
          trace = trace._parent;
        }
        removeCommonRoots(stacks);
        removeDuplicateOrEmptyJumps(stacks);
        util.notEnumerableProp(error, "stack", reconstructStack(message, stacks));
        util.notEnumerableProp(error, "__stackCleaned__", true);
      };
      var captureStackTrace = (function stackDetection() {
        var v8stackFramePattern = /^\s*at\s*/;
        var v8stackFormatter = function(stack, error) {
          if (typeof stack === "string") return stack;
          if (error.name !== void 0 && error.message !== void 0) {
            return error.toString();
          }
          return formatNonError(error);
        };
        if (typeof Error.stackTraceLimit === "number" && typeof Error.captureStackTrace === "function") {
          Error.stackTraceLimit += 6;
          stackFramePattern = v8stackFramePattern;
          formatStack = v8stackFormatter;
          var captureStackTrace2 = Error.captureStackTrace;
          shouldIgnore = function(line) {
            return bluebirdFramePattern.test(line);
          };
          return function(receiver2, ignoreUntil) {
            Error.stackTraceLimit += 6;
            captureStackTrace2(receiver2, ignoreUntil);
            Error.stackTraceLimit -= 6;
          };
        }
        var err = new Error();
        if (typeof err.stack === "string" && err.stack.split("\n")[0].indexOf("stackDetection@") >= 0) {
          stackFramePattern = /@/;
          formatStack = v8stackFormatter;
          indentStackFrames = true;
          return function captureStackTrace3(o) {
            o.stack = new Error().stack;
          };
        }
        var hasStackAfterThrow;
        try {
          throw new Error();
        } catch (e) {
          hasStackAfterThrow = "stack" in e;
        }
        if (!("stack" in err) && hasStackAfterThrow && typeof Error.stackTraceLimit === "number") {
          stackFramePattern = v8stackFramePattern;
          formatStack = v8stackFormatter;
          return function captureStackTrace3(o) {
            Error.stackTraceLimit += 6;
            try {
              throw new Error();
            } catch (e) {
              o.stack = e.stack;
            }
            Error.stackTraceLimit -= 6;
          };
        }
        formatStack = function(stack, error) {
          if (typeof stack === "string") return stack;
          if ((typeof error === "object" || typeof error === "function") && error.name !== void 0 && error.message !== void 0) {
            return error.toString();
          }
          return formatNonError(error);
        };
        return null;
      })([]);
      if (typeof console !== "undefined" && typeof console.warn !== "undefined") {
        printWarning = function(message) {
          console.warn(message);
        };
        if (util.isNode && process.stderr.isTTY) {
          printWarning = function(message, isSoft) {
            var color = isSoft ? "\x1B[33m" : "\x1B[31m";
            console.warn(color + message + "\x1B[0m\n");
          };
        } else if (!util.isNode && typeof new Error().stack === "string") {
          printWarning = function(message, isSoft) {
            console.warn(
              "%c" + message,
              isSoft ? "color: darkorange" : "color: red"
            );
          };
        }
      }
      var config = {
        warnings,
        longStackTraces: false,
        cancellation: false,
        monitoring: false,
        asyncHooks: false
      };
      if (longStackTraces) Promise2.longStackTraces();
      return {
        asyncHooks: function() {
          return config.asyncHooks;
        },
        longStackTraces: function() {
          return config.longStackTraces;
        },
        warnings: function() {
          return config.warnings;
        },
        cancellation: function() {
          return config.cancellation;
        },
        monitoring: function() {
          return config.monitoring;
        },
        propagateFromFunction: function() {
          return propagateFromFunction;
        },
        boundValueFunction: function() {
          return boundValueFunction;
        },
        checkForgottenReturns,
        setBounds,
        warn,
        deprecated,
        CapturedTrace,
        fireDomEvent,
        fireGlobalEvent
      };
    };
  }
});

// node_modules/bluebird/js/release/catch_filter.js
var require_catch_filter = __commonJS({
  "node_modules/bluebird/js/release/catch_filter.js"(exports2, module2) {
    "use strict";
    module2.exports = function(NEXT_FILTER) {
      var util = require_util();
      var getKeys = require_es5().keys;
      var tryCatch2 = util.tryCatch;
      var errorObj2 = util.errorObj;
      function catchFilter(instances, cb, promise) {
        return function(e) {
          var boundTo = promise._boundValue();
          predicateLoop: for (var i = 0; i < instances.length; ++i) {
            var item = instances[i];
            if (item === Error || item != null && item.prototype instanceof Error) {
              if (e instanceof item) {
                return tryCatch2(cb).call(boundTo, e);
              }
            } else if (typeof item === "function") {
              var matchesPredicate = tryCatch2(item).call(boundTo, e);
              if (matchesPredicate === errorObj2) {
                return matchesPredicate;
              } else if (matchesPredicate) {
                return tryCatch2(cb).call(boundTo, e);
              }
            } else if (util.isObject(e)) {
              var keys = getKeys(item);
              for (var j = 0; j < keys.length; ++j) {
                var key = keys[j];
                if (item[key] != e[key]) {
                  continue predicateLoop;
                }
              }
              return tryCatch2(cb).call(boundTo, e);
            }
          }
          return NEXT_FILTER;
        };
      }
      return catchFilter;
    };
  }
});

// node_modules/bluebird/js/release/finally.js
var require_finally = __commonJS({
  "node_modules/bluebird/js/release/finally.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, tryConvertToPromise, NEXT_FILTER) {
      var util = require_util();
      var CancellationError = Promise2.CancellationError;
      var errorObj2 = util.errorObj;
      var catchFilter = require_catch_filter()(NEXT_FILTER);
      function PassThroughHandlerContext(promise, type, handler) {
        this.promise = promise;
        this.type = type;
        this.handler = handler;
        this.called = false;
        this.cancelPromise = null;
      }
      PassThroughHandlerContext.prototype.isFinallyHandler = function() {
        return this.type === 0;
      };
      function FinallyHandlerCancelReaction(finallyHandler2) {
        this.finallyHandler = finallyHandler2;
      }
      FinallyHandlerCancelReaction.prototype._resultCancelled = function() {
        checkCancel(this.finallyHandler);
      };
      function checkCancel(ctx, reason) {
        if (ctx.cancelPromise != null) {
          if (arguments.length > 1) {
            ctx.cancelPromise._reject(reason);
          } else {
            ctx.cancelPromise._cancel();
          }
          ctx.cancelPromise = null;
          return true;
        }
        return false;
      }
      function succeed() {
        return finallyHandler.call(this, this.promise._target()._settledValue());
      }
      function fail(reason) {
        if (checkCancel(this, reason)) return;
        errorObj2.e = reason;
        return errorObj2;
      }
      function finallyHandler(reasonOrValue) {
        var promise = this.promise;
        var handler = this.handler;
        if (!this.called) {
          this.called = true;
          var ret2 = this.isFinallyHandler() ? handler.call(promise._boundValue()) : handler.call(promise._boundValue(), reasonOrValue);
          if (ret2 === NEXT_FILTER) {
            return ret2;
          } else if (ret2 !== void 0) {
            promise._setReturnedNonUndefined();
            var maybePromise = tryConvertToPromise(ret2, promise);
            if (maybePromise instanceof Promise2) {
              if (this.cancelPromise != null) {
                if (maybePromise._isCancelled()) {
                  var reason = new CancellationError("late cancellation observer");
                  promise._attachExtraTrace(reason);
                  errorObj2.e = reason;
                  return errorObj2;
                } else if (maybePromise.isPending()) {
                  maybePromise._attachCancellationCallback(
                    new FinallyHandlerCancelReaction(this)
                  );
                }
              }
              return maybePromise._then(
                succeed,
                fail,
                void 0,
                this,
                void 0
              );
            }
          }
        }
        if (promise.isRejected()) {
          checkCancel(this);
          errorObj2.e = reasonOrValue;
          return errorObj2;
        } else {
          checkCancel(this);
          return reasonOrValue;
        }
      }
      Promise2.prototype._passThrough = function(handler, type, success, fail2) {
        if (typeof handler !== "function") return this.then();
        return this._then(
          success,
          fail2,
          void 0,
          new PassThroughHandlerContext(this, type, handler),
          void 0
        );
      };
      Promise2.prototype.lastly = Promise2.prototype["finally"] = function(handler) {
        return this._passThrough(
          handler,
          0,
          finallyHandler,
          finallyHandler
        );
      };
      Promise2.prototype.tap = function(handler) {
        return this._passThrough(handler, 1, finallyHandler);
      };
      Promise2.prototype.tapCatch = function(handlerOrPredicate) {
        var len = arguments.length;
        if (len === 1) {
          return this._passThrough(
            handlerOrPredicate,
            1,
            void 0,
            finallyHandler
          );
        } else {
          var catchInstances = new Array(len - 1), j = 0, i;
          for (i = 0; i < len - 1; ++i) {
            var item = arguments[i];
            if (util.isObject(item)) {
              catchInstances[j++] = item;
            } else {
              return Promise2.reject(new TypeError(
                "tapCatch statement predicate: expecting an object but got " + util.classString(item)
              ));
            }
          }
          catchInstances.length = j;
          var handler = arguments[i];
          return this._passThrough(
            catchFilter(catchInstances, handler, this),
            1,
            void 0,
            finallyHandler
          );
        }
      };
      return PassThroughHandlerContext;
    };
  }
});

// node_modules/bluebird/js/release/nodeback.js
var require_nodeback = __commonJS({
  "node_modules/bluebird/js/release/nodeback.js"(exports2, module2) {
    "use strict";
    var util = require_util();
    var maybeWrapAsError2 = util.maybeWrapAsError;
    var errors = require_errors();
    var OperationalError = errors.OperationalError;
    var es52 = require_es5();
    function isUntypedError(obj2) {
      return obj2 instanceof Error && es52.getPrototypeOf(obj2) === Error.prototype;
    }
    var rErrorKey = /^(?:name|message|stack|cause)$/;
    function wrapAsOperationalError(obj2) {
      var ret2;
      if (isUntypedError(obj2)) {
        ret2 = new OperationalError(obj2);
        ret2.name = obj2.name;
        ret2.message = obj2.message;
        ret2.stack = obj2.stack;
        var keys = es52.keys(obj2);
        for (var i = 0; i < keys.length; ++i) {
          var key = keys[i];
          if (!rErrorKey.test(key)) {
            ret2[key] = obj2[key];
          }
        }
        return ret2;
      }
      util.markAsOriginatingFromRejection(obj2);
      return obj2;
    }
    function nodebackForPromise(promise, multiArgs) {
      return function(err, value) {
        if (promise === null) return;
        if (err) {
          var wrapped = wrapAsOperationalError(maybeWrapAsError2(err));
          promise._attachExtraTrace(wrapped);
          promise._reject(wrapped);
        } else if (!multiArgs) {
          promise._fulfill(value);
        } else {
          var $_len = arguments.length;
          var args = new Array(Math.max($_len - 1, 0));
          for (var $_i = 1; $_i < $_len; ++$_i) {
            args[$_i - 1] = arguments[$_i];
          }
          ;
          promise._fulfill(args);
        }
        promise = null;
      };
    }
    module2.exports = nodebackForPromise;
  }
});

// node_modules/bluebird/js/release/method.js
var require_method = __commonJS({
  "node_modules/bluebird/js/release/method.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL, tryConvertToPromise, apiRejection, debug) {
      var util = require_util();
      var tryCatch2 = util.tryCatch;
      Promise2.method = function(fn) {
        if (typeof fn !== "function") {
          throw new Promise2.TypeError("expecting a function but got " + util.classString(fn));
        }
        return function() {
          var ret2 = new Promise2(INTERNAL);
          ret2._captureStackTrace();
          ret2._pushContext();
          var value = tryCatch2(fn).apply(this, arguments);
          var promiseCreated = ret2._popContext();
          debug.checkForgottenReturns(
            value,
            promiseCreated,
            "Promise.method",
            ret2
          );
          ret2._resolveFromSyncValue(value);
          return ret2;
        };
      };
      Promise2.attempt = Promise2["try"] = function(fn) {
        if (typeof fn !== "function") {
          return apiRejection("expecting a function but got " + util.classString(fn));
        }
        var ret2 = new Promise2(INTERNAL);
        ret2._captureStackTrace();
        ret2._pushContext();
        var value;
        if (arguments.length > 1) {
          debug.deprecated("calling Promise.try with more than 1 argument");
          var arg = arguments[1];
          var ctx = arguments[2];
          value = util.isArray(arg) ? tryCatch2(fn).apply(ctx, arg) : tryCatch2(fn).call(ctx, arg);
        } else {
          value = tryCatch2(fn)();
        }
        var promiseCreated = ret2._popContext();
        debug.checkForgottenReturns(
          value,
          promiseCreated,
          "Promise.try",
          ret2
        );
        ret2._resolveFromSyncValue(value);
        return ret2;
      };
      Promise2.prototype._resolveFromSyncValue = function(value) {
        if (value === util.errorObj) {
          this._rejectCallback(value.e, false);
        } else {
          this._resolveCallback(value, true);
        }
      };
    };
  }
});

// node_modules/bluebird/js/release/bind.js
var require_bind = __commonJS({
  "node_modules/bluebird/js/release/bind.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL, tryConvertToPromise, debug) {
      var calledBind = false;
      var rejectThis = function(_, e) {
        this._reject(e);
      };
      var targetRejected = function(e, context) {
        context.promiseRejectionQueued = true;
        context.bindingPromise._then(rejectThis, rejectThis, null, this, e);
      };
      var bindingResolved = function(thisArg, context) {
        if ((this._bitField & 50397184) === 0) {
          this._resolveCallback(context.target);
        }
      };
      var bindingRejected = function(e, context) {
        if (!context.promiseRejectionQueued) this._reject(e);
      };
      Promise2.prototype.bind = function(thisArg) {
        if (!calledBind) {
          calledBind = true;
          Promise2.prototype._propagateFrom = debug.propagateFromFunction();
          Promise2.prototype._boundValue = debug.boundValueFunction();
        }
        var maybePromise = tryConvertToPromise(thisArg);
        var ret2 = new Promise2(INTERNAL);
        ret2._propagateFrom(this, 1);
        var target = this._target();
        ret2._setBoundTo(maybePromise);
        if (maybePromise instanceof Promise2) {
          var context = {
            promiseRejectionQueued: false,
            promise: ret2,
            target,
            bindingPromise: maybePromise
          };
          target._then(INTERNAL, targetRejected, void 0, ret2, context);
          maybePromise._then(
            bindingResolved,
            bindingRejected,
            void 0,
            ret2,
            context
          );
          ret2._setOnCancel(maybePromise);
        } else {
          ret2._resolveCallback(target);
        }
        return ret2;
      };
      Promise2.prototype._setBoundTo = function(obj2) {
        if (obj2 !== void 0) {
          this._bitField = this._bitField | 2097152;
          this._boundTo = obj2;
        } else {
          this._bitField = this._bitField & ~2097152;
        }
      };
      Promise2.prototype._isBound = function() {
        return (this._bitField & 2097152) === 2097152;
      };
      Promise2.bind = function(thisArg, value) {
        return Promise2.resolve(value).bind(thisArg);
      };
    };
  }
});

// node_modules/bluebird/js/release/cancel.js
var require_cancel = __commonJS({
  "node_modules/bluebird/js/release/cancel.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, PromiseArray, apiRejection, debug) {
      var util = require_util();
      var tryCatch2 = util.tryCatch;
      var errorObj2 = util.errorObj;
      var async = Promise2._async;
      Promise2.prototype["break"] = Promise2.prototype.cancel = function() {
        if (!debug.cancellation()) return this._warn("cancellation is disabled");
        var promise = this;
        var child = promise;
        while (promise._isCancellable()) {
          if (!promise._cancelBy(child)) {
            if (child._isFollowing()) {
              child._followee().cancel();
            } else {
              child._cancelBranched();
            }
            break;
          }
          var parent = promise._cancellationParent;
          if (parent == null || !parent._isCancellable()) {
            if (promise._isFollowing()) {
              promise._followee().cancel();
            } else {
              promise._cancelBranched();
            }
            break;
          } else {
            if (promise._isFollowing()) promise._followee().cancel();
            promise._setWillBeCancelled();
            child = promise;
            promise = parent;
          }
        }
      };
      Promise2.prototype._branchHasCancelled = function() {
        this._branchesRemainingToCancel--;
      };
      Promise2.prototype._enoughBranchesHaveCancelled = function() {
        return this._branchesRemainingToCancel === void 0 || this._branchesRemainingToCancel <= 0;
      };
      Promise2.prototype._cancelBy = function(canceller) {
        if (canceller === this) {
          this._branchesRemainingToCancel = 0;
          this._invokeOnCancel();
          return true;
        } else {
          this._branchHasCancelled();
          if (this._enoughBranchesHaveCancelled()) {
            this._invokeOnCancel();
            return true;
          }
        }
        return false;
      };
      Promise2.prototype._cancelBranched = function() {
        if (this._enoughBranchesHaveCancelled()) {
          this._cancel();
        }
      };
      Promise2.prototype._cancel = function() {
        if (!this._isCancellable()) return;
        this._setCancelled();
        async.invoke(this._cancelPromises, this, void 0);
      };
      Promise2.prototype._cancelPromises = function() {
        if (this._length() > 0) this._settlePromises();
      };
      Promise2.prototype._unsetOnCancel = function() {
        this._onCancelField = void 0;
      };
      Promise2.prototype._isCancellable = function() {
        return this.isPending() && !this._isCancelled();
      };
      Promise2.prototype.isCancellable = function() {
        return this.isPending() && !this.isCancelled();
      };
      Promise2.prototype._doInvokeOnCancel = function(onCancelCallback, internalOnly) {
        if (util.isArray(onCancelCallback)) {
          for (var i = 0; i < onCancelCallback.length; ++i) {
            this._doInvokeOnCancel(onCancelCallback[i], internalOnly);
          }
        } else if (onCancelCallback !== void 0) {
          if (typeof onCancelCallback === "function") {
            if (!internalOnly) {
              var e = tryCatch2(onCancelCallback).call(this._boundValue());
              if (e === errorObj2) {
                this._attachExtraTrace(e.e);
                async.throwLater(e.e);
              }
            }
          } else {
            onCancelCallback._resultCancelled(this);
          }
        }
      };
      Promise2.prototype._invokeOnCancel = function() {
        var onCancelCallback = this._onCancel();
        this._unsetOnCancel();
        async.invoke(this._doInvokeOnCancel, this, onCancelCallback);
      };
      Promise2.prototype._invokeInternalOnCancel = function() {
        if (this._isCancellable()) {
          this._doInvokeOnCancel(this._onCancel(), true);
          this._unsetOnCancel();
        }
      };
      Promise2.prototype._resultCancelled = function() {
        this.cancel();
      };
    };
  }
});

// node_modules/bluebird/js/release/direct_resolve.js
var require_direct_resolve = __commonJS({
  "node_modules/bluebird/js/release/direct_resolve.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2) {
      function returner() {
        return this.value;
      }
      function thrower2() {
        throw this.reason;
      }
      Promise2.prototype["return"] = Promise2.prototype.thenReturn = function(value) {
        if (value instanceof Promise2) value.suppressUnhandledRejections();
        return this._then(
          returner,
          void 0,
          void 0,
          { value },
          void 0
        );
      };
      Promise2.prototype["throw"] = Promise2.prototype.thenThrow = function(reason) {
        return this._then(
          thrower2,
          void 0,
          void 0,
          { reason },
          void 0
        );
      };
      Promise2.prototype.catchThrow = function(reason) {
        if (arguments.length <= 1) {
          return this._then(
            void 0,
            thrower2,
            void 0,
            { reason },
            void 0
          );
        } else {
          var _reason = arguments[1];
          var handler = function() {
            throw _reason;
          };
          return this.caught(reason, handler);
        }
      };
      Promise2.prototype.catchReturn = function(value) {
        if (arguments.length <= 1) {
          if (value instanceof Promise2) value.suppressUnhandledRejections();
          return this._then(
            void 0,
            returner,
            void 0,
            { value },
            void 0
          );
        } else {
          var _value = arguments[1];
          if (_value instanceof Promise2) _value.suppressUnhandledRejections();
          var handler = function() {
            return _value;
          };
          return this.caught(value, handler);
        }
      };
    };
  }
});

// node_modules/bluebird/js/release/synchronous_inspection.js
var require_synchronous_inspection = __commonJS({
  "node_modules/bluebird/js/release/synchronous_inspection.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2) {
      function PromiseInspection(promise) {
        if (promise !== void 0) {
          promise = promise._target();
          this._bitField = promise._bitField;
          this._settledValueField = promise._isFateSealed() ? promise._settledValue() : void 0;
        } else {
          this._bitField = 0;
          this._settledValueField = void 0;
        }
      }
      PromiseInspection.prototype._settledValue = function() {
        return this._settledValueField;
      };
      var value = PromiseInspection.prototype.value = function() {
        if (!this.isFulfilled()) {
          throw new TypeError("cannot get fulfillment value of a non-fulfilled promise\n\n    See http://goo.gl/MqrFmX\n");
        }
        return this._settledValue();
      };
      var reason = PromiseInspection.prototype.error = PromiseInspection.prototype.reason = function() {
        if (!this.isRejected()) {
          throw new TypeError("cannot get rejection reason of a non-rejected promise\n\n    See http://goo.gl/MqrFmX\n");
        }
        return this._settledValue();
      };
      var isFulfilled = PromiseInspection.prototype.isFulfilled = function() {
        return (this._bitField & 33554432) !== 0;
      };
      var isRejected = PromiseInspection.prototype.isRejected = function() {
        return (this._bitField & 16777216) !== 0;
      };
      var isPending = PromiseInspection.prototype.isPending = function() {
        return (this._bitField & 50397184) === 0;
      };
      var isResolved = PromiseInspection.prototype.isResolved = function() {
        return (this._bitField & 50331648) !== 0;
      };
      PromiseInspection.prototype.isCancelled = function() {
        return (this._bitField & 8454144) !== 0;
      };
      Promise2.prototype.__isCancelled = function() {
        return (this._bitField & 65536) === 65536;
      };
      Promise2.prototype._isCancelled = function() {
        return this._target().__isCancelled();
      };
      Promise2.prototype.isCancelled = function() {
        return (this._target()._bitField & 8454144) !== 0;
      };
      Promise2.prototype.isPending = function() {
        return isPending.call(this._target());
      };
      Promise2.prototype.isRejected = function() {
        return isRejected.call(this._target());
      };
      Promise2.prototype.isFulfilled = function() {
        return isFulfilled.call(this._target());
      };
      Promise2.prototype.isResolved = function() {
        return isResolved.call(this._target());
      };
      Promise2.prototype.value = function() {
        return value.call(this._target());
      };
      Promise2.prototype.reason = function() {
        var target = this._target();
        target._unsetRejectionIsUnhandled();
        return reason.call(target);
      };
      Promise2.prototype._value = function() {
        return this._settledValue();
      };
      Promise2.prototype._reason = function() {
        this._unsetRejectionIsUnhandled();
        return this._settledValue();
      };
      Promise2.PromiseInspection = PromiseInspection;
    };
  }
});

// node_modules/bluebird/js/release/join.js
var require_join = __commonJS({
  "node_modules/bluebird/js/release/join.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, PromiseArray, tryConvertToPromise, INTERNAL, async) {
      var util = require_util();
      var canEvaluate2 = util.canEvaluate;
      var tryCatch2 = util.tryCatch;
      var errorObj2 = util.errorObj;
      var reject;
      if (true) {
        if (canEvaluate2) {
          var thenCallback = function(i2) {
            return new Function("value", "holder", "                             \n            'use strict';                                                    \n            holder.pIndex = value;                                           \n            holder.checkFulfillment(this);                                   \n            ".replace(/Index/g, i2));
          };
          var promiseSetter = function(i2) {
            return new Function("promise", "holder", "                           \n            'use strict';                                                    \n            holder.pIndex = promise;                                         \n            ".replace(/Index/g, i2));
          };
          var generateHolderClass = function(total) {
            var props = new Array(total);
            for (var i2 = 0; i2 < props.length; ++i2) {
              props[i2] = "this.p" + (i2 + 1);
            }
            var assignment = props.join(" = ") + " = null;";
            var cancellationCode = "var promise;\n" + props.map(function(prop) {
              return "                                                         \n                promise = " + prop + ";                                      \n                if (promise instanceof Promise) {                            \n                    promise.cancel();                                        \n                }                                                            \n            ";
            }).join("\n");
            var passedArguments = props.join(", ");
            var name = "Holder$" + total;
            var code = "return function(tryCatch, errorObj, Promise, async) {    \n            'use strict';                                                    \n            function [TheName](fn) {                                         \n                [TheProperties]                                              \n                this.fn = fn;                                                \n                this.asyncNeeded = true;                                     \n                this.now = 0;                                                \n            }                                                                \n                                                                             \n            [TheName].prototype._callFunction = function(promise) {          \n                promise._pushContext();                                      \n                var ret = tryCatch(this.fn)([ThePassedArguments]);           \n                promise._popContext();                                       \n                if (ret === errorObj) {                                      \n                    promise._rejectCallback(ret.e, false);                   \n                } else {                                                     \n                    promise._resolveCallback(ret);                           \n                }                                                            \n            };                                                               \n                                                                             \n            [TheName].prototype.checkFulfillment = function(promise) {       \n                var now = ++this.now;                                        \n                if (now === [TheTotal]) {                                    \n                    if (this.asyncNeeded) {                                  \n                        async.invoke(this._callFunction, this, promise);     \n                    } else {                                                 \n                        this._callFunction(promise);                         \n                    }                                                        \n                                                                             \n                }                                                            \n            };                                                               \n                                                                             \n            [TheName].prototype._resultCancelled = function() {              \n                [CancellationCode]                                           \n            };                                                               \n                                                                             \n            return [TheName];                                                \n        }(tryCatch, errorObj, Promise, async);                               \n        ";
            code = code.replace(/\[TheName\]/g, name).replace(/\[TheTotal\]/g, total).replace(/\[ThePassedArguments\]/g, passedArguments).replace(/\[TheProperties\]/g, assignment).replace(/\[CancellationCode\]/g, cancellationCode);
            return new Function("tryCatch", "errorObj", "Promise", "async", code)(tryCatch2, errorObj2, Promise2, async);
          };
          var holderClasses = [];
          var thenCallbacks = [];
          var promiseSetters = [];
          for (var i = 0; i < 8; ++i) {
            holderClasses.push(generateHolderClass(i + 1));
            thenCallbacks.push(thenCallback(i + 1));
            promiseSetters.push(promiseSetter(i + 1));
          }
          reject = function(reason) {
            this._reject(reason);
          };
        }
      }
      Promise2.join = function() {
        var last = arguments.length - 1;
        var fn;
        if (last > 0 && typeof arguments[last] === "function") {
          fn = arguments[last];
          if (true) {
            if (last <= 8 && canEvaluate2) {
              var ret2 = new Promise2(INTERNAL);
              ret2._captureStackTrace();
              var HolderClass = holderClasses[last - 1];
              var holder = new HolderClass(fn);
              var callbacks = thenCallbacks;
              for (var i2 = 0; i2 < last; ++i2) {
                var maybePromise = tryConvertToPromise(arguments[i2], ret2);
                if (maybePromise instanceof Promise2) {
                  maybePromise = maybePromise._target();
                  var bitField = maybePromise._bitField;
                  ;
                  if ((bitField & 50397184) === 0) {
                    maybePromise._then(
                      callbacks[i2],
                      reject,
                      void 0,
                      ret2,
                      holder
                    );
                    promiseSetters[i2](maybePromise, holder);
                    holder.asyncNeeded = false;
                  } else if ((bitField & 33554432) !== 0) {
                    callbacks[i2].call(
                      ret2,
                      maybePromise._value(),
                      holder
                    );
                  } else if ((bitField & 16777216) !== 0) {
                    ret2._reject(maybePromise._reason());
                  } else {
                    ret2._cancel();
                  }
                } else {
                  callbacks[i2].call(ret2, maybePromise, holder);
                }
              }
              if (!ret2._isFateSealed()) {
                if (holder.asyncNeeded) {
                  var context = Promise2._getContext();
                  holder.fn = util.contextBind(context, holder.fn);
                }
                ret2._setAsyncGuaranteed();
                ret2._setOnCancel(holder);
              }
              return ret2;
            }
          }
        }
        var $_len = arguments.length;
        var args = new Array($_len);
        for (var $_i = 0; $_i < $_len; ++$_i) {
          args[$_i] = arguments[$_i];
        }
        ;
        if (fn) args.pop();
        var ret2 = new PromiseArray(args).promise();
        return fn !== void 0 ? ret2.spread(fn) : ret2;
      };
    };
  }
});

// node_modules/bluebird/js/release/call_get.js
var require_call_get = __commonJS({
  "node_modules/bluebird/js/release/call_get.js"(exports2, module2) {
    "use strict";
    var cr = Object.create;
    if (cr) {
      callerCache = cr(null);
      getterCache = cr(null);
      callerCache[" size"] = getterCache[" size"] = 0;
    }
    var callerCache;
    var getterCache;
    module2.exports = function(Promise2) {
      var util = require_util();
      var canEvaluate2 = util.canEvaluate;
      var isIdentifier2 = util.isIdentifier;
      var getMethodCaller;
      var getGetter;
      if (true) {
        var makeMethodCaller = function(methodName) {
          return new Function("ensureMethod", "                                    \n        return function(obj) {                                               \n            'use strict'                                                     \n            var len = this.length;                                           \n            ensureMethod(obj, 'methodName');                                 \n            switch(len) {                                                    \n                case 1: return obj.methodName(this[0]);                      \n                case 2: return obj.methodName(this[0], this[1]);             \n                case 3: return obj.methodName(this[0], this[1], this[2]);    \n                case 0: return obj.methodName();                             \n                default:                                                     \n                    return obj.methodName.apply(obj, this);                  \n            }                                                                \n        };                                                                   \n        ".replace(/methodName/g, methodName))(ensureMethod);
        };
        var makeGetter = function(propertyName) {
          return new Function("obj", "                                             \n        'use strict';                                                        \n        return obj.propertyName;                                             \n        ".replace("propertyName", propertyName));
        };
        var getCompiled = function(name, compiler, cache) {
          var ret2 = cache[name];
          if (typeof ret2 !== "function") {
            if (!isIdentifier2(name)) {
              return null;
            }
            ret2 = compiler(name);
            cache[name] = ret2;
            cache[" size"]++;
            if (cache[" size"] > 512) {
              var keys = Object.keys(cache);
              for (var i = 0; i < 256; ++i) delete cache[keys[i]];
              cache[" size"] = keys.length - 256;
            }
          }
          return ret2;
        };
        getMethodCaller = function(name) {
          return getCompiled(name, makeMethodCaller, callerCache);
        };
        getGetter = function(name) {
          return getCompiled(name, makeGetter, getterCache);
        };
      }
      function ensureMethod(obj2, methodName) {
        var fn;
        if (obj2 != null) fn = obj2[methodName];
        if (typeof fn !== "function") {
          var message = "Object " + util.classString(obj2) + " has no method '" + util.toString(methodName) + "'";
          throw new Promise2.TypeError(message);
        }
        return fn;
      }
      function caller(obj2) {
        var methodName = this.pop();
        var fn = ensureMethod(obj2, methodName);
        return fn.apply(obj2, this);
      }
      Promise2.prototype.call = function(methodName) {
        var $_len = arguments.length;
        var args = new Array(Math.max($_len - 1, 0));
        for (var $_i = 1; $_i < $_len; ++$_i) {
          args[$_i - 1] = arguments[$_i];
        }
        ;
        if (true) {
          if (canEvaluate2) {
            var maybeCaller = getMethodCaller(methodName);
            if (maybeCaller !== null) {
              return this._then(
                maybeCaller,
                void 0,
                void 0,
                args,
                void 0
              );
            }
          }
        }
        args.push(methodName);
        return this._then(caller, void 0, void 0, args, void 0);
      };
      function namedGetter(obj2) {
        return obj2[this];
      }
      function indexedGetter(obj2) {
        var index = +this;
        if (index < 0) index = Math.max(0, index + obj2.length);
        return obj2[index];
      }
      Promise2.prototype.get = function(propertyName) {
        var isIndex = typeof propertyName === "number";
        var getter;
        if (!isIndex) {
          if (canEvaluate2) {
            var maybeGetter = getGetter(propertyName);
            getter = maybeGetter !== null ? maybeGetter : namedGetter;
          } else {
            getter = namedGetter;
          }
        } else {
          getter = indexedGetter;
        }
        return this._then(getter, void 0, void 0, propertyName, void 0);
      };
    };
  }
});

// node_modules/bluebird/js/release/generators.js
var require_generators = __commonJS({
  "node_modules/bluebird/js/release/generators.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, apiRejection, INTERNAL, tryConvertToPromise, Proxyable, debug) {
      var errors = require_errors();
      var TypeError2 = errors.TypeError;
      var util = require_util();
      var errorObj2 = util.errorObj;
      var tryCatch2 = util.tryCatch;
      var yieldHandlers = [];
      function promiseFromYieldHandler(value, yieldHandlers2, traceParent) {
        for (var i = 0; i < yieldHandlers2.length; ++i) {
          traceParent._pushContext();
          var result = tryCatch2(yieldHandlers2[i])(value);
          traceParent._popContext();
          if (result === errorObj2) {
            traceParent._pushContext();
            var ret2 = Promise2.reject(errorObj2.e);
            traceParent._popContext();
            return ret2;
          }
          var maybePromise = tryConvertToPromise(result, traceParent);
          if (maybePromise instanceof Promise2) return maybePromise;
        }
        return null;
      }
      function PromiseSpawn(generatorFunction, receiver2, yieldHandler, stack) {
        if (debug.cancellation()) {
          var internal = new Promise2(INTERNAL);
          var _finallyPromise = this._finallyPromise = new Promise2(INTERNAL);
          this._promise = internal.lastly(function() {
            return _finallyPromise;
          });
          internal._captureStackTrace();
          internal._setOnCancel(this);
        } else {
          var promise = this._promise = new Promise2(INTERNAL);
          promise._captureStackTrace();
        }
        this._stack = stack;
        this._generatorFunction = generatorFunction;
        this._receiver = receiver2;
        this._generator = void 0;
        this._yieldHandlers = typeof yieldHandler === "function" ? [yieldHandler].concat(yieldHandlers) : yieldHandlers;
        this._yieldedPromise = null;
        this._cancellationPhase = false;
      }
      util.inherits(PromiseSpawn, Proxyable);
      PromiseSpawn.prototype._isResolved = function() {
        return this._promise === null;
      };
      PromiseSpawn.prototype._cleanup = function() {
        this._promise = this._generator = null;
        if (debug.cancellation() && this._finallyPromise !== null) {
          this._finallyPromise._fulfill();
          this._finallyPromise = null;
        }
      };
      PromiseSpawn.prototype._promiseCancelled = function() {
        if (this._isResolved()) return;
        var implementsReturn = typeof this._generator["return"] !== "undefined";
        var result;
        if (!implementsReturn) {
          var reason = new Promise2.CancellationError(
            "generator .return() sentinel"
          );
          Promise2.coroutine.returnSentinel = reason;
          this._promise._attachExtraTrace(reason);
          this._promise._pushContext();
          result = tryCatch2(this._generator["throw"]).call(
            this._generator,
            reason
          );
          this._promise._popContext();
        } else {
          this._promise._pushContext();
          result = tryCatch2(this._generator["return"]).call(
            this._generator,
            void 0
          );
          this._promise._popContext();
        }
        this._cancellationPhase = true;
        this._yieldedPromise = null;
        this._continue(result);
      };
      PromiseSpawn.prototype._promiseFulfilled = function(value) {
        this._yieldedPromise = null;
        this._promise._pushContext();
        var result = tryCatch2(this._generator.next).call(this._generator, value);
        this._promise._popContext();
        this._continue(result);
      };
      PromiseSpawn.prototype._promiseRejected = function(reason) {
        this._yieldedPromise = null;
        this._promise._attachExtraTrace(reason);
        this._promise._pushContext();
        var result = tryCatch2(this._generator["throw"]).call(this._generator, reason);
        this._promise._popContext();
        this._continue(result);
      };
      PromiseSpawn.prototype._resultCancelled = function() {
        if (this._yieldedPromise instanceof Promise2) {
          var promise = this._yieldedPromise;
          this._yieldedPromise = null;
          promise.cancel();
        }
      };
      PromiseSpawn.prototype.promise = function() {
        return this._promise;
      };
      PromiseSpawn.prototype._run = function() {
        this._generator = this._generatorFunction.call(this._receiver);
        this._receiver = this._generatorFunction = void 0;
        this._promiseFulfilled(void 0);
      };
      PromiseSpawn.prototype._continue = function(result) {
        var promise = this._promise;
        if (result === errorObj2) {
          this._cleanup();
          if (this._cancellationPhase) {
            return promise.cancel();
          } else {
            return promise._rejectCallback(result.e, false);
          }
        }
        var value = result.value;
        if (result.done === true) {
          this._cleanup();
          if (this._cancellationPhase) {
            return promise.cancel();
          } else {
            return promise._resolveCallback(value);
          }
        } else {
          var maybePromise = tryConvertToPromise(value, this._promise);
          if (!(maybePromise instanceof Promise2)) {
            maybePromise = promiseFromYieldHandler(
              maybePromise,
              this._yieldHandlers,
              this._promise
            );
            if (maybePromise === null) {
              this._promiseRejected(
                new TypeError2(
                  "A value %s was yielded that could not be treated as a promise\n\n    See http://goo.gl/MqrFmX\n\n".replace("%s", String(value)) + "From coroutine:\n" + this._stack.split("\n").slice(1, -7).join("\n")
                )
              );
              return;
            }
          }
          maybePromise = maybePromise._target();
          var bitField = maybePromise._bitField;
          ;
          if ((bitField & 50397184) === 0) {
            this._yieldedPromise = maybePromise;
            maybePromise._proxy(this, null);
          } else if ((bitField & 33554432) !== 0) {
            Promise2._async.invoke(
              this._promiseFulfilled,
              this,
              maybePromise._value()
            );
          } else if ((bitField & 16777216) !== 0) {
            Promise2._async.invoke(
              this._promiseRejected,
              this,
              maybePromise._reason()
            );
          } else {
            this._promiseCancelled();
          }
        }
      };
      Promise2.coroutine = function(generatorFunction, options) {
        if (typeof generatorFunction !== "function") {
          throw new TypeError2("generatorFunction must be a function\n\n    See http://goo.gl/MqrFmX\n");
        }
        var yieldHandler = Object(options).yieldHandler;
        var PromiseSpawn$ = PromiseSpawn;
        var stack = new Error().stack;
        return function() {
          var generator = generatorFunction.apply(this, arguments);
          var spawn = new PromiseSpawn$(
            void 0,
            void 0,
            yieldHandler,
            stack
          );
          var ret2 = spawn.promise();
          spawn._generator = generator;
          spawn._promiseFulfilled(void 0);
          return ret2;
        };
      };
      Promise2.coroutine.addYieldHandler = function(fn) {
        if (typeof fn !== "function") {
          throw new TypeError2("expecting a function but got " + util.classString(fn));
        }
        yieldHandlers.push(fn);
      };
      Promise2.spawn = function(generatorFunction) {
        debug.deprecated("Promise.spawn()", "Promise.coroutine()");
        if (typeof generatorFunction !== "function") {
          return apiRejection("generatorFunction must be a function\n\n    See http://goo.gl/MqrFmX\n");
        }
        var spawn = new PromiseSpawn(generatorFunction, this);
        var ret2 = spawn.promise();
        spawn._run(Promise2.spawn);
        return ret2;
      };
    };
  }
});

// node_modules/bluebird/js/release/map.js
var require_map = __commonJS({
  "node_modules/bluebird/js/release/map.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, PromiseArray, apiRejection, tryConvertToPromise, INTERNAL, debug) {
      var util = require_util();
      var tryCatch2 = util.tryCatch;
      var errorObj2 = util.errorObj;
      var async = Promise2._async;
      function MappingPromiseArray(promises, fn, limit, _filter) {
        this.constructor$(promises);
        this._promise._captureStackTrace();
        var context = Promise2._getContext();
        this._callback = util.contextBind(context, fn);
        this._preservedValues = _filter === INTERNAL ? new Array(this.length()) : null;
        this._limit = limit;
        this._inFlight = 0;
        this._queue = [];
        async.invoke(this._asyncInit, this, void 0);
        if (util.isArray(promises)) {
          for (var i = 0; i < promises.length; ++i) {
            var maybePromise = promises[i];
            if (maybePromise instanceof Promise2) {
              maybePromise.suppressUnhandledRejections();
            }
          }
        }
      }
      util.inherits(MappingPromiseArray, PromiseArray);
      MappingPromiseArray.prototype._asyncInit = function() {
        this._init$(void 0, -2);
      };
      MappingPromiseArray.prototype._init = function() {
      };
      MappingPromiseArray.prototype._promiseFulfilled = function(value, index) {
        var values = this._values;
        var length = this.length();
        var preservedValues = this._preservedValues;
        var limit = this._limit;
        if (index < 0) {
          index = index * -1 - 1;
          values[index] = value;
          if (limit >= 1) {
            this._inFlight--;
            this._drainQueue();
            if (this._isResolved()) return true;
          }
        } else {
          if (limit >= 1 && this._inFlight >= limit) {
            values[index] = value;
            this._queue.push(index);
            return false;
          }
          if (preservedValues !== null) preservedValues[index] = value;
          var promise = this._promise;
          var callback = this._callback;
          var receiver2 = promise._boundValue();
          promise._pushContext();
          var ret2 = tryCatch2(callback).call(receiver2, value, index, length);
          var promiseCreated = promise._popContext();
          debug.checkForgottenReturns(
            ret2,
            promiseCreated,
            preservedValues !== null ? "Promise.filter" : "Promise.map",
            promise
          );
          if (ret2 === errorObj2) {
            this._reject(ret2.e);
            return true;
          }
          var maybePromise = tryConvertToPromise(ret2, this._promise);
          if (maybePromise instanceof Promise2) {
            maybePromise = maybePromise._target();
            var bitField = maybePromise._bitField;
            ;
            if ((bitField & 50397184) === 0) {
              if (limit >= 1) this._inFlight++;
              values[index] = maybePromise;
              maybePromise._proxy(this, (index + 1) * -1);
              return false;
            } else if ((bitField & 33554432) !== 0) {
              ret2 = maybePromise._value();
            } else if ((bitField & 16777216) !== 0) {
              this._reject(maybePromise._reason());
              return true;
            } else {
              this._cancel();
              return true;
            }
          }
          values[index] = ret2;
        }
        var totalResolved = ++this._totalResolved;
        if (totalResolved >= length) {
          if (preservedValues !== null) {
            this._filter(values, preservedValues);
          } else {
            this._resolve(values);
          }
          return true;
        }
        return false;
      };
      MappingPromiseArray.prototype._drainQueue = function() {
        var queue = this._queue;
        var limit = this._limit;
        var values = this._values;
        while (queue.length > 0 && this._inFlight < limit) {
          if (this._isResolved()) return;
          var index = queue.pop();
          this._promiseFulfilled(values[index], index);
        }
      };
      MappingPromiseArray.prototype._filter = function(booleans, values) {
        var len = values.length;
        var ret2 = new Array(len);
        var j = 0;
        for (var i = 0; i < len; ++i) {
          if (booleans[i]) ret2[j++] = values[i];
        }
        ret2.length = j;
        this._resolve(ret2);
      };
      MappingPromiseArray.prototype.preservedValues = function() {
        return this._preservedValues;
      };
      function map(promises, fn, options, _filter) {
        if (typeof fn !== "function") {
          return apiRejection("expecting a function but got " + util.classString(fn));
        }
        var limit = 0;
        if (options !== void 0) {
          if (typeof options === "object" && options !== null) {
            if (typeof options.concurrency !== "number") {
              return Promise2.reject(
                new TypeError("'concurrency' must be a number but it is " + util.classString(options.concurrency))
              );
            }
            limit = options.concurrency;
          } else {
            return Promise2.reject(new TypeError(
              "options argument must be an object but it is " + util.classString(options)
            ));
          }
        }
        limit = typeof limit === "number" && isFinite(limit) && limit >= 1 ? limit : 0;
        return new MappingPromiseArray(promises, fn, limit, _filter).promise();
      }
      Promise2.prototype.map = function(fn, options) {
        return map(this, fn, options, null);
      };
      Promise2.map = function(promises, fn, options, _filter) {
        return map(promises, fn, options, _filter);
      };
    };
  }
});

// node_modules/bluebird/js/release/nodeify.js
var require_nodeify = __commonJS({
  "node_modules/bluebird/js/release/nodeify.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2) {
      var util = require_util();
      var async = Promise2._async;
      var tryCatch2 = util.tryCatch;
      var errorObj2 = util.errorObj;
      function spreadAdapter(val, nodeback) {
        var promise = this;
        if (!util.isArray(val)) return successAdapter.call(promise, val, nodeback);
        var ret2 = tryCatch2(nodeback).apply(promise._boundValue(), [null].concat(val));
        if (ret2 === errorObj2) {
          async.throwLater(ret2.e);
        }
      }
      function successAdapter(val, nodeback) {
        var promise = this;
        var receiver2 = promise._boundValue();
        var ret2 = val === void 0 ? tryCatch2(nodeback).call(receiver2, null) : tryCatch2(nodeback).call(receiver2, null, val);
        if (ret2 === errorObj2) {
          async.throwLater(ret2.e);
        }
      }
      function errorAdapter(reason, nodeback) {
        var promise = this;
        if (!reason) {
          var newReason = new Error(reason + "");
          newReason.cause = reason;
          reason = newReason;
        }
        var ret2 = tryCatch2(nodeback).call(promise._boundValue(), reason);
        if (ret2 === errorObj2) {
          async.throwLater(ret2.e);
        }
      }
      Promise2.prototype.asCallback = Promise2.prototype.nodeify = function(nodeback, options) {
        if (typeof nodeback == "function") {
          var adapter = successAdapter;
          if (options !== void 0 && Object(options).spread) {
            adapter = spreadAdapter;
          }
          this._then(
            adapter,
            errorAdapter,
            void 0,
            this,
            nodeback
          );
        }
        return this;
      };
    };
  }
});

// node_modules/bluebird/js/release/promisify.js
var require_promisify = __commonJS({
  "node_modules/bluebird/js/release/promisify.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL) {
      var THIS = {};
      var util = require_util();
      var nodebackForPromise = require_nodeback();
      var withAppended2 = util.withAppended;
      var maybeWrapAsError2 = util.maybeWrapAsError;
      var canEvaluate2 = util.canEvaluate;
      var TypeError2 = require_errors().TypeError;
      var defaultSuffix = "Async";
      var defaultPromisified = { __isPromisified__: true };
      var noCopyProps = [
        "arity",
        "length",
        "name",
        "arguments",
        "caller",
        "callee",
        "prototype",
        "__isPromisified__"
      ];
      var noCopyPropsPattern = new RegExp("^(?:" + noCopyProps.join("|") + ")$");
      var defaultFilter = function(name) {
        return util.isIdentifier(name) && name.charAt(0) !== "_" && name !== "constructor";
      };
      function propsFilter(key) {
        return !noCopyPropsPattern.test(key);
      }
      function isPromisified(fn) {
        try {
          return fn.__isPromisified__ === true;
        } catch (e) {
          return false;
        }
      }
      function hasPromisified(obj2, key, suffix) {
        var val = util.getDataPropertyOrDefault(
          obj2,
          key + suffix,
          defaultPromisified
        );
        return val ? isPromisified(val) : false;
      }
      function checkValid(ret2, suffix, suffixRegexp) {
        for (var i = 0; i < ret2.length; i += 2) {
          var key = ret2[i];
          if (suffixRegexp.test(key)) {
            var keyWithoutAsyncSuffix = key.replace(suffixRegexp, "");
            for (var j = 0; j < ret2.length; j += 2) {
              if (ret2[j] === keyWithoutAsyncSuffix) {
                throw new TypeError2("Cannot promisify an API that has normal methods with '%s'-suffix\n\n    See http://goo.gl/MqrFmX\n".replace("%s", suffix));
              }
            }
          }
        }
      }
      function promisifiableMethods(obj2, suffix, suffixRegexp, filter) {
        var keys = util.inheritedDataKeys(obj2);
        var ret2 = [];
        for (var i = 0; i < keys.length; ++i) {
          var key = keys[i];
          var value = obj2[key];
          var passesDefaultFilter = filter === defaultFilter ? true : defaultFilter(key, value, obj2);
          if (typeof value === "function" && !isPromisified(value) && !hasPromisified(obj2, key, suffix) && filter(key, value, obj2, passesDefaultFilter)) {
            ret2.push(key, value);
          }
        }
        checkValid(ret2, suffix, suffixRegexp);
        return ret2;
      }
      var escapeIdentRegex = function(str) {
        return str.replace(/([$])/, "\\$");
      };
      var makeNodePromisifiedEval;
      if (true) {
        var switchCaseArgumentOrder = function(likelyArgumentCount) {
          var ret2 = [likelyArgumentCount];
          var min = Math.max(0, likelyArgumentCount - 1 - 3);
          for (var i = likelyArgumentCount - 1; i >= min; --i) {
            ret2.push(i);
          }
          for (var i = likelyArgumentCount + 1; i <= 3; ++i) {
            ret2.push(i);
          }
          return ret2;
        };
        var argumentSequence = function(argumentCount) {
          return util.filledRange(argumentCount, "_arg", "");
        };
        var parameterDeclaration = function(parameterCount2) {
          return util.filledRange(
            Math.max(parameterCount2, 3),
            "_arg",
            ""
          );
        };
        var parameterCount = function(fn) {
          if (typeof fn.length === "number") {
            return Math.max(Math.min(fn.length, 1023 + 1), 0);
          }
          return 0;
        };
        makeNodePromisifiedEval = function(callback, receiver2, originalName, fn, _, multiArgs) {
          var newParameterCount = Math.max(0, parameterCount(fn) - 1);
          var argumentOrder = switchCaseArgumentOrder(newParameterCount);
          var shouldProxyThis = typeof callback === "string" || receiver2 === THIS;
          function generateCallForArgumentCount(count) {
            var args = argumentSequence(count).join(", ");
            var comma = count > 0 ? ", " : "";
            var ret2;
            if (shouldProxyThis) {
              ret2 = "ret = callback.call(this, {{args}}, nodeback); break;\n";
            } else {
              ret2 = receiver2 === void 0 ? "ret = callback({{args}}, nodeback); break;\n" : "ret = callback.call(receiver, {{args}}, nodeback); break;\n";
            }
            return ret2.replace("{{args}}", args).replace(", ", comma);
          }
          function generateArgumentSwitchCase() {
            var ret2 = "";
            for (var i = 0; i < argumentOrder.length; ++i) {
              ret2 += "case " + argumentOrder[i] + ":" + generateCallForArgumentCount(argumentOrder[i]);
            }
            ret2 += "                                                             \n        default:                                                             \n            var args = new Array(len + 1);                                   \n            var i = 0;                                                       \n            for (var i = 0; i < len; ++i) {                                  \n               args[i] = arguments[i];                                       \n            }                                                                \n            args[i] = nodeback;                                              \n            [CodeForCall]                                                    \n            break;                                                           \n        ".replace("[CodeForCall]", shouldProxyThis ? "ret = callback.apply(this, args);\n" : "ret = callback.apply(receiver, args);\n");
            return ret2;
          }
          var getFunctionCode = typeof callback === "string" ? "this != null ? this['" + callback + "'] : fn" : "fn";
          var body = "'use strict';                                                \n        var ret = function (Parameters) {                                    \n            'use strict';                                                    \n            var len = arguments.length;                                      \n            var promise = new Promise(INTERNAL);                             \n            promise._captureStackTrace();                                    \n            var nodeback = nodebackForPromise(promise, " + multiArgs + ");   \n            var ret;                                                         \n            var callback = tryCatch([GetFunctionCode]);                      \n            switch(len) {                                                    \n                [CodeForSwitchCase]                                          \n            }                                                                \n            if (ret === errorObj) {                                          \n                promise._rejectCallback(maybeWrapAsError(ret.e), true, true);\n            }                                                                \n            if (!promise._isFateSealed()) promise._setAsyncGuaranteed();     \n            return promise;                                                  \n        };                                                                   \n        notEnumerableProp(ret, '__isPromisified__', true);                   \n        return ret;                                                          \n    ".replace("[CodeForSwitchCase]", generateArgumentSwitchCase()).replace("[GetFunctionCode]", getFunctionCode);
          body = body.replace("Parameters", parameterDeclaration(newParameterCount));
          return new Function(
            "Promise",
            "fn",
            "receiver",
            "withAppended",
            "maybeWrapAsError",
            "nodebackForPromise",
            "tryCatch",
            "errorObj",
            "notEnumerableProp",
            "INTERNAL",
            body
          )(
            Promise2,
            fn,
            receiver2,
            withAppended2,
            maybeWrapAsError2,
            nodebackForPromise,
            util.tryCatch,
            util.errorObj,
            util.notEnumerableProp,
            INTERNAL
          );
        };
      }
      function makeNodePromisifiedClosure(callback, receiver2, _, fn, __, multiArgs) {
        var defaultThis = /* @__PURE__ */ (function() {
          return this;
        })();
        var method = callback;
        if (typeof method === "string") {
          callback = fn;
        }
        function promisified() {
          var _receiver = receiver2;
          if (receiver2 === THIS) _receiver = this;
          var promise = new Promise2(INTERNAL);
          promise._captureStackTrace();
          var cb = typeof method === "string" && this !== defaultThis ? this[method] : callback;
          var fn2 = nodebackForPromise(promise, multiArgs);
          try {
            cb.apply(_receiver, withAppended2(arguments, fn2));
          } catch (e) {
            promise._rejectCallback(maybeWrapAsError2(e), true, true);
          }
          if (!promise._isFateSealed()) promise._setAsyncGuaranteed();
          return promise;
        }
        util.notEnumerableProp(promisified, "__isPromisified__", true);
        return promisified;
      }
      var makeNodePromisified = canEvaluate2 ? makeNodePromisifiedEval : makeNodePromisifiedClosure;
      function promisifyAll(obj2, suffix, filter, promisifier, multiArgs) {
        var suffixRegexp = new RegExp(escapeIdentRegex(suffix) + "$");
        var methods = promisifiableMethods(obj2, suffix, suffixRegexp, filter);
        for (var i = 0, len = methods.length; i < len; i += 2) {
          var key = methods[i];
          var fn = methods[i + 1];
          var promisifiedKey = key + suffix;
          if (promisifier === makeNodePromisified) {
            obj2[promisifiedKey] = makeNodePromisified(key, THIS, key, fn, suffix, multiArgs);
          } else {
            var promisified = promisifier(fn, function() {
              return makeNodePromisified(
                key,
                THIS,
                key,
                fn,
                suffix,
                multiArgs
              );
            });
            util.notEnumerableProp(promisified, "__isPromisified__", true);
            obj2[promisifiedKey] = promisified;
          }
        }
        util.toFastProperties(obj2);
        return obj2;
      }
      function promisify(callback, receiver2, multiArgs) {
        return makeNodePromisified(
          callback,
          receiver2,
          void 0,
          callback,
          null,
          multiArgs
        );
      }
      Promise2.promisify = function(fn, options) {
        if (typeof fn !== "function") {
          throw new TypeError2("expecting a function but got " + util.classString(fn));
        }
        if (isPromisified(fn)) {
          return fn;
        }
        options = Object(options);
        var receiver2 = options.context === void 0 ? THIS : options.context;
        var multiArgs = !!options.multiArgs;
        var ret2 = promisify(fn, receiver2, multiArgs);
        util.copyDescriptors(fn, ret2, propsFilter);
        return ret2;
      };
      Promise2.promisifyAll = function(target, options) {
        if (typeof target !== "function" && typeof target !== "object") {
          throw new TypeError2("the target of promisifyAll must be an object or a function\n\n    See http://goo.gl/MqrFmX\n");
        }
        options = Object(options);
        var multiArgs = !!options.multiArgs;
        var suffix = options.suffix;
        if (typeof suffix !== "string") suffix = defaultSuffix;
        var filter = options.filter;
        if (typeof filter !== "function") filter = defaultFilter;
        var promisifier = options.promisifier;
        if (typeof promisifier !== "function") promisifier = makeNodePromisified;
        if (!util.isIdentifier(suffix)) {
          throw new RangeError("suffix must be a valid identifier\n\n    See http://goo.gl/MqrFmX\n");
        }
        var keys = util.inheritedDataKeys(target);
        for (var i = 0; i < keys.length; ++i) {
          var value = target[keys[i]];
          if (keys[i] !== "constructor" && util.isClass(value)) {
            promisifyAll(
              value.prototype,
              suffix,
              filter,
              promisifier,
              multiArgs
            );
            promisifyAll(value, suffix, filter, promisifier, multiArgs);
          }
        }
        return promisifyAll(target, suffix, filter, promisifier, multiArgs);
      };
    };
  }
});

// node_modules/bluebird/js/release/props.js
var require_props = __commonJS({
  "node_modules/bluebird/js/release/props.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, PromiseArray, tryConvertToPromise, apiRejection) {
      var util = require_util();
      var isObject2 = util.isObject;
      var es52 = require_es5();
      var Es6Map;
      if (typeof Map === "function") Es6Map = Map;
      var mapToEntries = /* @__PURE__ */ (function() {
        var index = 0;
        var size = 0;
        function extractEntry(value, key) {
          this[index] = value;
          this[index + size] = key;
          index++;
        }
        return function mapToEntries2(map) {
          size = map.size;
          index = 0;
          var ret2 = new Array(map.size * 2);
          map.forEach(extractEntry, ret2);
          return ret2;
        };
      })();
      var entriesToMap = function(entries) {
        var ret2 = new Es6Map();
        var length = entries.length / 2 | 0;
        for (var i = 0; i < length; ++i) {
          var key = entries[length + i];
          var value = entries[i];
          ret2.set(key, value);
        }
        return ret2;
      };
      function PropertiesPromiseArray(obj2) {
        var isMap = false;
        var entries;
        if (Es6Map !== void 0 && obj2 instanceof Es6Map) {
          entries = mapToEntries(obj2);
          isMap = true;
        } else {
          var keys = es52.keys(obj2);
          var len = keys.length;
          entries = new Array(len * 2);
          for (var i = 0; i < len; ++i) {
            var key = keys[i];
            entries[i] = obj2[key];
            entries[i + len] = key;
          }
        }
        this.constructor$(entries);
        this._isMap = isMap;
        this._init$(void 0, isMap ? -6 : -3);
      }
      util.inherits(PropertiesPromiseArray, PromiseArray);
      PropertiesPromiseArray.prototype._init = function() {
      };
      PropertiesPromiseArray.prototype._promiseFulfilled = function(value, index) {
        this._values[index] = value;
        var totalResolved = ++this._totalResolved;
        if (totalResolved >= this._length) {
          var val;
          if (this._isMap) {
            val = entriesToMap(this._values);
          } else {
            val = {};
            var keyOffset = this.length();
            for (var i = 0, len = this.length(); i < len; ++i) {
              val[this._values[i + keyOffset]] = this._values[i];
            }
          }
          this._resolve(val);
          return true;
        }
        return false;
      };
      PropertiesPromiseArray.prototype.shouldCopyValues = function() {
        return false;
      };
      PropertiesPromiseArray.prototype.getActualLength = function(len) {
        return len >> 1;
      };
      function props(promises) {
        var ret2;
        var castValue = tryConvertToPromise(promises);
        if (!isObject2(castValue)) {
          return apiRejection("cannot await properties of a non-object\n\n    See http://goo.gl/MqrFmX\n");
        } else if (castValue instanceof Promise2) {
          ret2 = castValue._then(
            Promise2.props,
            void 0,
            void 0,
            void 0,
            void 0
          );
        } else {
          ret2 = new PropertiesPromiseArray(castValue).promise();
        }
        if (castValue instanceof Promise2) {
          ret2._propagateFrom(castValue, 2);
        }
        return ret2;
      }
      Promise2.prototype.props = function() {
        return props(this);
      };
      Promise2.props = function(promises) {
        return props(promises);
      };
    };
  }
});

// node_modules/bluebird/js/release/race.js
var require_race = __commonJS({
  "node_modules/bluebird/js/release/race.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL, tryConvertToPromise, apiRejection) {
      var util = require_util();
      var raceLater = function(promise) {
        return promise.then(function(array) {
          return race(array, promise);
        });
      };
      function race(promises, parent) {
        var maybePromise = tryConvertToPromise(promises);
        if (maybePromise instanceof Promise2) {
          return raceLater(maybePromise);
        } else {
          promises = util.asArray(promises);
          if (promises === null)
            return apiRejection("expecting an array or an iterable object but got " + util.classString(promises));
        }
        var ret2 = new Promise2(INTERNAL);
        if (parent !== void 0) {
          ret2._propagateFrom(parent, 3);
        }
        var fulfill = ret2._fulfill;
        var reject = ret2._reject;
        for (var i = 0, len = promises.length; i < len; ++i) {
          var val = promises[i];
          if (val === void 0 && !(i in promises)) {
            continue;
          }
          Promise2.cast(val)._then(fulfill, reject, void 0, ret2, null);
        }
        return ret2;
      }
      Promise2.race = function(promises) {
        return race(promises, void 0);
      };
      Promise2.prototype.race = function() {
        return race(this, void 0);
      };
    };
  }
});

// node_modules/bluebird/js/release/reduce.js
var require_reduce = __commonJS({
  "node_modules/bluebird/js/release/reduce.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, PromiseArray, apiRejection, tryConvertToPromise, INTERNAL, debug) {
      var util = require_util();
      var tryCatch2 = util.tryCatch;
      function ReductionPromiseArray(promises, fn, initialValue, _each) {
        this.constructor$(promises);
        var context = Promise2._getContext();
        this._fn = util.contextBind(context, fn);
        if (initialValue !== void 0) {
          initialValue = Promise2.resolve(initialValue);
          initialValue._attachCancellationCallback(this);
        }
        this._initialValue = initialValue;
        this._currentCancellable = null;
        if (_each === INTERNAL) {
          this._eachValues = Array(this._length);
        } else if (_each === 0) {
          this._eachValues = null;
        } else {
          this._eachValues = void 0;
        }
        this._promise._captureStackTrace();
        this._init$(void 0, -5);
      }
      util.inherits(ReductionPromiseArray, PromiseArray);
      ReductionPromiseArray.prototype._gotAccum = function(accum) {
        if (this._eachValues !== void 0 && this._eachValues !== null && accum !== INTERNAL) {
          this._eachValues.push(accum);
        }
      };
      ReductionPromiseArray.prototype._eachComplete = function(value) {
        if (this._eachValues !== null) {
          this._eachValues.push(value);
        }
        return this._eachValues;
      };
      ReductionPromiseArray.prototype._init = function() {
      };
      ReductionPromiseArray.prototype._resolveEmptyArray = function() {
        this._resolve(this._eachValues !== void 0 ? this._eachValues : this._initialValue);
      };
      ReductionPromiseArray.prototype.shouldCopyValues = function() {
        return false;
      };
      ReductionPromiseArray.prototype._resolve = function(value) {
        this._promise._resolveCallback(value);
        this._values = null;
      };
      ReductionPromiseArray.prototype._resultCancelled = function(sender) {
        if (sender === this._initialValue) return this._cancel();
        if (this._isResolved()) return;
        this._resultCancelled$();
        if (this._currentCancellable instanceof Promise2) {
          this._currentCancellable.cancel();
        }
        if (this._initialValue instanceof Promise2) {
          this._initialValue.cancel();
        }
      };
      ReductionPromiseArray.prototype._iterate = function(values) {
        this._values = values;
        var value;
        var i;
        var length = values.length;
        if (this._initialValue !== void 0) {
          value = this._initialValue;
          i = 0;
        } else {
          value = Promise2.resolve(values[0]);
          i = 1;
        }
        this._currentCancellable = value;
        for (var j = i; j < length; ++j) {
          var maybePromise = values[j];
          if (maybePromise instanceof Promise2) {
            maybePromise.suppressUnhandledRejections();
          }
        }
        if (!value.isRejected()) {
          for (; i < length; ++i) {
            var ctx = {
              accum: null,
              value: values[i],
              index: i,
              length,
              array: this
            };
            value = value._then(gotAccum, void 0, void 0, ctx, void 0);
            if ((i & 127) === 0) {
              value._setNoAsyncGuarantee();
            }
          }
        }
        if (this._eachValues !== void 0) {
          value = value._then(this._eachComplete, void 0, void 0, this, void 0);
        }
        value._then(completed, completed, void 0, value, this);
      };
      Promise2.prototype.reduce = function(fn, initialValue) {
        return reduce(this, fn, initialValue, null);
      };
      Promise2.reduce = function(promises, fn, initialValue, _each) {
        return reduce(promises, fn, initialValue, _each);
      };
      function completed(valueOrReason, array) {
        if (this.isFulfilled()) {
          array._resolve(valueOrReason);
        } else {
          array._reject(valueOrReason);
        }
      }
      function reduce(promises, fn, initialValue, _each) {
        if (typeof fn !== "function") {
          return apiRejection("expecting a function but got " + util.classString(fn));
        }
        var array = new ReductionPromiseArray(promises, fn, initialValue, _each);
        return array.promise();
      }
      function gotAccum(accum) {
        this.accum = accum;
        this.array._gotAccum(accum);
        var value = tryConvertToPromise(this.value, this.array._promise);
        if (value instanceof Promise2) {
          this.array._currentCancellable = value;
          return value._then(gotValue, void 0, void 0, this, void 0);
        } else {
          return gotValue.call(this, value);
        }
      }
      function gotValue(value) {
        var array = this.array;
        var promise = array._promise;
        var fn = tryCatch2(array._fn);
        promise._pushContext();
        var ret2;
        if (array._eachValues !== void 0) {
          ret2 = fn.call(promise._boundValue(), value, this.index, this.length);
        } else {
          ret2 = fn.call(
            promise._boundValue(),
            this.accum,
            value,
            this.index,
            this.length
          );
        }
        if (ret2 instanceof Promise2) {
          array._currentCancellable = ret2;
        }
        var promiseCreated = promise._popContext();
        debug.checkForgottenReturns(
          ret2,
          promiseCreated,
          array._eachValues !== void 0 ? "Promise.each" : "Promise.reduce",
          promise
        );
        return ret2;
      }
    };
  }
});

// node_modules/bluebird/js/release/settle.js
var require_settle = __commonJS({
  "node_modules/bluebird/js/release/settle.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, PromiseArray, debug) {
      var PromiseInspection = Promise2.PromiseInspection;
      var util = require_util();
      function SettledPromiseArray(values) {
        this.constructor$(values);
      }
      util.inherits(SettledPromiseArray, PromiseArray);
      SettledPromiseArray.prototype._promiseResolved = function(index, inspection) {
        this._values[index] = inspection;
        var totalResolved = ++this._totalResolved;
        if (totalResolved >= this._length) {
          this._resolve(this._values);
          return true;
        }
        return false;
      };
      SettledPromiseArray.prototype._promiseFulfilled = function(value, index) {
        var ret2 = new PromiseInspection();
        ret2._bitField = 33554432;
        ret2._settledValueField = value;
        return this._promiseResolved(index, ret2);
      };
      SettledPromiseArray.prototype._promiseRejected = function(reason, index) {
        var ret2 = new PromiseInspection();
        ret2._bitField = 16777216;
        ret2._settledValueField = reason;
        return this._promiseResolved(index, ret2);
      };
      Promise2.settle = function(promises) {
        debug.deprecated(".settle()", ".reflect()");
        return new SettledPromiseArray(promises).promise();
      };
      Promise2.allSettled = function(promises) {
        return new SettledPromiseArray(promises).promise();
      };
      Promise2.prototype.settle = function() {
        return Promise2.settle(this);
      };
    };
  }
});

// node_modules/bluebird/js/release/some.js
var require_some = __commonJS({
  "node_modules/bluebird/js/release/some.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, PromiseArray, apiRejection) {
      var util = require_util();
      var RangeError2 = require_errors().RangeError;
      var AggregateError = require_errors().AggregateError;
      var isArray = util.isArray;
      var CANCELLATION = {};
      function SomePromiseArray(values) {
        this.constructor$(values);
        this._howMany = 0;
        this._unwrap = false;
        this._initialized = false;
      }
      util.inherits(SomePromiseArray, PromiseArray);
      SomePromiseArray.prototype._init = function() {
        if (!this._initialized) {
          return;
        }
        if (this._howMany === 0) {
          this._resolve([]);
          return;
        }
        this._init$(void 0, -5);
        var isArrayResolved = isArray(this._values);
        if (!this._isResolved() && isArrayResolved && this._howMany > this._canPossiblyFulfill()) {
          this._reject(this._getRangeError(this.length()));
        }
      };
      SomePromiseArray.prototype.init = function() {
        this._initialized = true;
        this._init();
      };
      SomePromiseArray.prototype.setUnwrap = function() {
        this._unwrap = true;
      };
      SomePromiseArray.prototype.howMany = function() {
        return this._howMany;
      };
      SomePromiseArray.prototype.setHowMany = function(count) {
        this._howMany = count;
      };
      SomePromiseArray.prototype._promiseFulfilled = function(value) {
        this._addFulfilled(value);
        if (this._fulfilled() === this.howMany()) {
          this._values.length = this.howMany();
          if (this.howMany() === 1 && this._unwrap) {
            this._resolve(this._values[0]);
          } else {
            this._resolve(this._values);
          }
          return true;
        }
        return false;
      };
      SomePromiseArray.prototype._promiseRejected = function(reason) {
        this._addRejected(reason);
        return this._checkOutcome();
      };
      SomePromiseArray.prototype._promiseCancelled = function() {
        if (this._values instanceof Promise2 || this._values == null) {
          return this._cancel();
        }
        this._addRejected(CANCELLATION);
        return this._checkOutcome();
      };
      SomePromiseArray.prototype._checkOutcome = function() {
        if (this.howMany() > this._canPossiblyFulfill()) {
          var e = new AggregateError();
          for (var i = this.length(); i < this._values.length; ++i) {
            if (this._values[i] !== CANCELLATION) {
              e.push(this._values[i]);
            }
          }
          if (e.length > 0) {
            this._reject(e);
          } else {
            this._cancel();
          }
          return true;
        }
        return false;
      };
      SomePromiseArray.prototype._fulfilled = function() {
        return this._totalResolved;
      };
      SomePromiseArray.prototype._rejected = function() {
        return this._values.length - this.length();
      };
      SomePromiseArray.prototype._addRejected = function(reason) {
        this._values.push(reason);
      };
      SomePromiseArray.prototype._addFulfilled = function(value) {
        this._values[this._totalResolved++] = value;
      };
      SomePromiseArray.prototype._canPossiblyFulfill = function() {
        return this.length() - this._rejected();
      };
      SomePromiseArray.prototype._getRangeError = function(count) {
        var message = "Input array must contain at least " + this._howMany + " items but contains only " + count + " items";
        return new RangeError2(message);
      };
      SomePromiseArray.prototype._resolveEmptyArray = function() {
        this._reject(this._getRangeError(0));
      };
      function some(promises, howMany) {
        if ((howMany | 0) !== howMany || howMany < 0) {
          return apiRejection("expecting a positive integer\n\n    See http://goo.gl/MqrFmX\n");
        }
        var ret2 = new SomePromiseArray(promises);
        var promise = ret2.promise();
        ret2.setHowMany(howMany);
        ret2.init();
        return promise;
      }
      Promise2.some = function(promises, howMany) {
        return some(promises, howMany);
      };
      Promise2.prototype.some = function(howMany) {
        return some(this, howMany);
      };
      Promise2._SomePromiseArray = SomePromiseArray;
    };
  }
});

// node_modules/bluebird/js/release/timers.js
var require_timers = __commonJS({
  "node_modules/bluebird/js/release/timers.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL, debug) {
      var util = require_util();
      var TimeoutError = Promise2.TimeoutError;
      function HandleWrapper(handle) {
        this.handle = handle;
      }
      HandleWrapper.prototype._resultCancelled = function() {
        clearTimeout(this.handle);
      };
      var afterValue = function(value) {
        return delay2(+this).thenReturn(value);
      };
      var delay2 = Promise2.delay = function(ms, value) {
        var ret2;
        var handle;
        if (value !== void 0) {
          ret2 = Promise2.resolve(value)._then(afterValue, null, null, ms, void 0);
          if (debug.cancellation() && value instanceof Promise2) {
            ret2._setOnCancel(value);
          }
        } else {
          ret2 = new Promise2(INTERNAL);
          handle = setTimeout(function() {
            ret2._fulfill();
          }, +ms);
          if (debug.cancellation()) {
            ret2._setOnCancel(new HandleWrapper(handle));
          }
          ret2._captureStackTrace();
        }
        ret2._setAsyncGuaranteed();
        return ret2;
      };
      Promise2.prototype.delay = function(ms) {
        return delay2(ms, this);
      };
      var afterTimeout = function(promise, message, parent) {
        var err;
        if (typeof message !== "string") {
          if (message instanceof Error) {
            err = message;
          } else {
            err = new TimeoutError("operation timed out");
          }
        } else {
          err = new TimeoutError(message);
        }
        util.markAsOriginatingFromRejection(err);
        promise._attachExtraTrace(err);
        promise._reject(err);
        if (parent != null) {
          parent.cancel();
        }
      };
      function successClear(value) {
        clearTimeout(this.handle);
        return value;
      }
      function failureClear(reason) {
        clearTimeout(this.handle);
        throw reason;
      }
      Promise2.prototype.timeout = function(ms, message) {
        ms = +ms;
        var ret2, parent;
        var handleWrapper = new HandleWrapper(setTimeout(function timeoutTimeout() {
          if (ret2.isPending()) {
            afterTimeout(ret2, message, parent);
          }
        }, ms));
        if (debug.cancellation()) {
          parent = this.then();
          ret2 = parent._then(
            successClear,
            failureClear,
            void 0,
            handleWrapper,
            void 0
          );
          ret2._setOnCancel(handleWrapper);
        } else {
          ret2 = this._then(
            successClear,
            failureClear,
            void 0,
            handleWrapper,
            void 0
          );
        }
        return ret2;
      };
    };
  }
});

// node_modules/bluebird/js/release/using.js
var require_using = __commonJS({
  "node_modules/bluebird/js/release/using.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, apiRejection, tryConvertToPromise, createContext, INTERNAL, debug) {
      var util = require_util();
      var TypeError2 = require_errors().TypeError;
      var inherits2 = require_util().inherits;
      var errorObj2 = util.errorObj;
      var tryCatch2 = util.tryCatch;
      var NULL = {};
      function thrower2(e) {
        setTimeout(function() {
          throw e;
        }, 0);
      }
      function castPreservingDisposable(thenable) {
        var maybePromise = tryConvertToPromise(thenable);
        if (maybePromise !== thenable && typeof thenable._isDisposable === "function" && typeof thenable._getDisposer === "function" && thenable._isDisposable()) {
          maybePromise._setDisposable(thenable._getDisposer());
        }
        return maybePromise;
      }
      function dispose(resources, inspection) {
        var i = 0;
        var len = resources.length;
        var ret2 = new Promise2(INTERNAL);
        function iterator() {
          if (i >= len) return ret2._fulfill();
          var maybePromise = castPreservingDisposable(resources[i++]);
          if (maybePromise instanceof Promise2 && maybePromise._isDisposable()) {
            try {
              maybePromise = tryConvertToPromise(
                maybePromise._getDisposer().tryDispose(inspection),
                resources.promise
              );
            } catch (e) {
              return thrower2(e);
            }
            if (maybePromise instanceof Promise2) {
              return maybePromise._then(
                iterator,
                thrower2,
                null,
                null,
                null
              );
            }
          }
          iterator();
        }
        iterator();
        return ret2;
      }
      function Disposer(data, promise, context) {
        this._data = data;
        this._promise = promise;
        this._context = context;
      }
      Disposer.prototype.data = function() {
        return this._data;
      };
      Disposer.prototype.promise = function() {
        return this._promise;
      };
      Disposer.prototype.resource = function() {
        if (this.promise().isFulfilled()) {
          return this.promise().value();
        }
        return NULL;
      };
      Disposer.prototype.tryDispose = function(inspection) {
        var resource = this.resource();
        var context = this._context;
        if (context !== void 0) context._pushContext();
        var ret2 = resource !== NULL ? this.doDispose(resource, inspection) : null;
        if (context !== void 0) context._popContext();
        this._promise._unsetDisposable();
        this._data = null;
        return ret2;
      };
      Disposer.isDisposer = function(d) {
        return d != null && typeof d.resource === "function" && typeof d.tryDispose === "function";
      };
      function FunctionDisposer(fn, promise, context) {
        this.constructor$(fn, promise, context);
      }
      inherits2(FunctionDisposer, Disposer);
      FunctionDisposer.prototype.doDispose = function(resource, inspection) {
        var fn = this.data();
        return fn.call(resource, resource, inspection);
      };
      function maybeUnwrapDisposer(value) {
        if (Disposer.isDisposer(value)) {
          this.resources[this.index]._setDisposable(value);
          return value.promise();
        }
        return value;
      }
      function ResourceList(length) {
        this.length = length;
        this.promise = null;
        this[length - 1] = null;
      }
      ResourceList.prototype._resultCancelled = function() {
        var len = this.length;
        for (var i = 0; i < len; ++i) {
          var item = this[i];
          if (item instanceof Promise2) {
            item.cancel();
          }
        }
      };
      Promise2.using = function() {
        var len = arguments.length;
        if (len < 2) return apiRejection(
          "you must pass at least 2 arguments to Promise.using"
        );
        var fn = arguments[len - 1];
        if (typeof fn !== "function") {
          return apiRejection("expecting a function but got " + util.classString(fn));
        }
        var input;
        var spreadArgs = true;
        if (len === 2 && Array.isArray(arguments[0])) {
          input = arguments[0];
          len = input.length;
          spreadArgs = false;
        } else {
          input = arguments;
          len--;
        }
        var resources = new ResourceList(len);
        for (var i = 0; i < len; ++i) {
          var resource = input[i];
          if (Disposer.isDisposer(resource)) {
            var disposer = resource;
            resource = resource.promise();
            resource._setDisposable(disposer);
          } else {
            var maybePromise = tryConvertToPromise(resource);
            if (maybePromise instanceof Promise2) {
              resource = maybePromise._then(maybeUnwrapDisposer, null, null, {
                resources,
                index: i
              }, void 0);
            }
          }
          resources[i] = resource;
        }
        var reflectedResources = new Array(resources.length);
        for (var i = 0; i < reflectedResources.length; ++i) {
          reflectedResources[i] = Promise2.resolve(resources[i]).reflect();
        }
        var resultPromise = Promise2.all(reflectedResources).then(function(inspections) {
          for (var i2 = 0; i2 < inspections.length; ++i2) {
            var inspection = inspections[i2];
            if (inspection.isRejected()) {
              errorObj2.e = inspection.error();
              return errorObj2;
            } else if (!inspection.isFulfilled()) {
              resultPromise.cancel();
              return;
            }
            inspections[i2] = inspection.value();
          }
          promise._pushContext();
          fn = tryCatch2(fn);
          var ret2 = spreadArgs ? fn.apply(void 0, inspections) : fn(inspections);
          var promiseCreated = promise._popContext();
          debug.checkForgottenReturns(
            ret2,
            promiseCreated,
            "Promise.using",
            promise
          );
          return ret2;
        });
        var promise = resultPromise.lastly(function() {
          var inspection = new Promise2.PromiseInspection(resultPromise);
          return dispose(resources, inspection);
        });
        resources.promise = promise;
        promise._setOnCancel(resources);
        return promise;
      };
      Promise2.prototype._setDisposable = function(disposer) {
        this._bitField = this._bitField | 131072;
        this._disposer = disposer;
      };
      Promise2.prototype._isDisposable = function() {
        return (this._bitField & 131072) > 0;
      };
      Promise2.prototype._getDisposer = function() {
        return this._disposer;
      };
      Promise2.prototype._unsetDisposable = function() {
        this._bitField = this._bitField & ~131072;
        this._disposer = void 0;
      };
      Promise2.prototype.disposer = function(fn) {
        if (typeof fn === "function") {
          return new FunctionDisposer(fn, this, createContext());
        }
        throw new TypeError2();
      };
    };
  }
});

// node_modules/bluebird/js/release/any.js
var require_any = __commonJS({
  "node_modules/bluebird/js/release/any.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2) {
      var SomePromiseArray = Promise2._SomePromiseArray;
      function any(promises) {
        var ret2 = new SomePromiseArray(promises);
        var promise = ret2.promise();
        ret2.setHowMany(1);
        ret2.setUnwrap();
        ret2.init();
        return promise;
      }
      Promise2.any = function(promises) {
        return any(promises);
      };
      Promise2.prototype.any = function() {
        return any(this);
      };
    };
  }
});

// node_modules/bluebird/js/release/each.js
var require_each = __commonJS({
  "node_modules/bluebird/js/release/each.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL) {
      var PromiseReduce = Promise2.reduce;
      var PromiseAll = Promise2.all;
      function promiseAllThis() {
        return PromiseAll(this);
      }
      function PromiseMapSeries(promises, fn) {
        return PromiseReduce(promises, fn, INTERNAL, INTERNAL);
      }
      Promise2.prototype.each = function(fn) {
        return PromiseReduce(this, fn, INTERNAL, 0)._then(promiseAllThis, void 0, void 0, this, void 0);
      };
      Promise2.prototype.mapSeries = function(fn) {
        return PromiseReduce(this, fn, INTERNAL, INTERNAL);
      };
      Promise2.each = function(promises, fn) {
        return PromiseReduce(promises, fn, INTERNAL, 0)._then(promiseAllThis, void 0, void 0, promises, void 0);
      };
      Promise2.mapSeries = PromiseMapSeries;
    };
  }
});

// node_modules/bluebird/js/release/filter.js
var require_filter = __commonJS({
  "node_modules/bluebird/js/release/filter.js"(exports2, module2) {
    "use strict";
    module2.exports = function(Promise2, INTERNAL) {
      var PromiseMap = Promise2.map;
      Promise2.prototype.filter = function(fn, options) {
        return PromiseMap(this, fn, options, INTERNAL);
      };
      Promise2.filter = function(promises, fn, options) {
        return PromiseMap(promises, fn, options, INTERNAL);
      };
    };
  }
});

// node_modules/bluebird/js/release/promise.js
var require_promise = __commonJS({
  "node_modules/bluebird/js/release/promise.js"(exports2, module2) {
    "use strict";
    module2.exports = function() {
      var makeSelfResolutionError = function() {
        return new TypeError2("circular promise resolution chain\n\n    See http://goo.gl/MqrFmX\n");
      };
      var reflectHandler2 = function() {
        return new Promise2.PromiseInspection(this._target());
      };
      var apiRejection = function(msg) {
        return Promise2.reject(new TypeError2(msg));
      };
      function Proxyable() {
      }
      var UNDEFINED_BINDING = {};
      var util = require_util();
      util.setReflectHandler(reflectHandler2);
      var getDomain = function() {
        var domain = process.domain;
        if (domain === void 0) {
          return null;
        }
        return domain;
      };
      var getContextDefault = function() {
        return null;
      };
      var getContextDomain = function() {
        return {
          domain: getDomain(),
          async: null
        };
      };
      var AsyncResource = util.isNode && util.nodeSupportsAsyncResource ? require("async_hooks").AsyncResource : null;
      var getContextAsyncHooks = function() {
        return {
          domain: getDomain(),
          async: new AsyncResource("Bluebird::Promise")
        };
      };
      var getContext = util.isNode ? getContextDomain : getContextDefault;
      util.notEnumerableProp(Promise2, "_getContext", getContext);
      var enableAsyncHooks = function() {
        getContext = getContextAsyncHooks;
        util.notEnumerableProp(Promise2, "_getContext", getContextAsyncHooks);
      };
      var disableAsyncHooks = function() {
        getContext = getContextDomain;
        util.notEnumerableProp(Promise2, "_getContext", getContextDomain);
      };
      var es52 = require_es5();
      var Async = require_async2();
      var async = new Async();
      es52.defineProperty(Promise2, "_async", { value: async });
      var errors = require_errors();
      var TypeError2 = Promise2.TypeError = errors.TypeError;
      Promise2.RangeError = errors.RangeError;
      var CancellationError = Promise2.CancellationError = errors.CancellationError;
      Promise2.TimeoutError = errors.TimeoutError;
      Promise2.OperationalError = errors.OperationalError;
      Promise2.RejectionError = errors.OperationalError;
      Promise2.AggregateError = errors.AggregateError;
      var INTERNAL = function() {
      };
      var APPLY = {};
      var NEXT_FILTER = {};
      var tryConvertToPromise = require_thenables()(Promise2, INTERNAL);
      var PromiseArray = require_promise_array()(
        Promise2,
        INTERNAL,
        tryConvertToPromise,
        apiRejection,
        Proxyable
      );
      var Context = require_context()(Promise2);
      var createContext = Context.create;
      var debug = require_debuggability()(
        Promise2,
        Context,
        enableAsyncHooks,
        disableAsyncHooks
      );
      var CapturedTrace = debug.CapturedTrace;
      var PassThroughHandlerContext = require_finally()(Promise2, tryConvertToPromise, NEXT_FILTER);
      var catchFilter = require_catch_filter()(NEXT_FILTER);
      var nodebackForPromise = require_nodeback();
      var errorObj2 = util.errorObj;
      var tryCatch2 = util.tryCatch;
      function check(self2, executor) {
        if (self2 == null || self2.constructor !== Promise2) {
          throw new TypeError2("the promise constructor cannot be invoked directly\n\n    See http://goo.gl/MqrFmX\n");
        }
        if (typeof executor !== "function") {
          throw new TypeError2("expecting a function but got " + util.classString(executor));
        }
      }
      function Promise2(executor) {
        if (executor !== INTERNAL) {
          check(this, executor);
        }
        this._bitField = 0;
        this._fulfillmentHandler0 = void 0;
        this._rejectionHandler0 = void 0;
        this._promise0 = void 0;
        this._receiver0 = void 0;
        this._resolveFromExecutor(executor);
        this._promiseCreated();
        this._fireEvent("promiseCreated", this);
      }
      Promise2.prototype.toString = function() {
        return "[object Promise]";
      };
      Promise2.prototype.caught = Promise2.prototype["catch"] = function(fn) {
        var len = arguments.length;
        if (len > 1) {
          var catchInstances = new Array(len - 1), j = 0, i;
          for (i = 0; i < len - 1; ++i) {
            var item = arguments[i];
            if (util.isObject(item)) {
              catchInstances[j++] = item;
            } else {
              return apiRejection("Catch statement predicate: expecting an object but got " + util.classString(item));
            }
          }
          catchInstances.length = j;
          fn = arguments[i];
          if (typeof fn !== "function") {
            throw new TypeError2("The last argument to .catch() must be a function, got " + util.toString(fn));
          }
          return this.then(void 0, catchFilter(catchInstances, fn, this));
        }
        return this.then(void 0, fn);
      };
      Promise2.prototype.reflect = function() {
        return this._then(
          reflectHandler2,
          reflectHandler2,
          void 0,
          this,
          void 0
        );
      };
      Promise2.prototype.then = function(didFulfill, didReject) {
        if (debug.warnings() && arguments.length > 0 && typeof didFulfill !== "function" && typeof didReject !== "function") {
          var msg = ".then() only accepts functions but was passed: " + util.classString(didFulfill);
          if (arguments.length > 1) {
            msg += ", " + util.classString(didReject);
          }
          this._warn(msg);
        }
        return this._then(didFulfill, didReject, void 0, void 0, void 0);
      };
      Promise2.prototype.done = function(didFulfill, didReject) {
        var promise = this._then(didFulfill, didReject, void 0, void 0, void 0);
        promise._setIsFinal();
      };
      Promise2.prototype.spread = function(fn) {
        if (typeof fn !== "function") {
          return apiRejection("expecting a function but got " + util.classString(fn));
        }
        return this.all()._then(fn, void 0, void 0, APPLY, void 0);
      };
      Promise2.prototype.toJSON = function() {
        var ret2 = {
          isFulfilled: false,
          isRejected: false,
          fulfillmentValue: void 0,
          rejectionReason: void 0
        };
        if (this.isFulfilled()) {
          ret2.fulfillmentValue = this.value();
          ret2.isFulfilled = true;
        } else if (this.isRejected()) {
          ret2.rejectionReason = this.reason();
          ret2.isRejected = true;
        }
        return ret2;
      };
      Promise2.prototype.all = function() {
        if (arguments.length > 0) {
          this._warn(".all() was passed arguments but it does not take any");
        }
        return new PromiseArray(this).promise();
      };
      Promise2.prototype.error = function(fn) {
        return this.caught(util.originatesFromRejection, fn);
      };
      Promise2.getNewLibraryCopy = module2.exports;
      Promise2.is = function(val) {
        return val instanceof Promise2;
      };
      Promise2.fromNode = Promise2.fromCallback = function(fn) {
        var ret2 = new Promise2(INTERNAL);
        ret2._captureStackTrace();
        var multiArgs = arguments.length > 1 ? !!Object(arguments[1]).multiArgs : false;
        var result = tryCatch2(fn)(nodebackForPromise(ret2, multiArgs));
        if (result === errorObj2) {
          ret2._rejectCallback(result.e, true);
        }
        if (!ret2._isFateSealed()) ret2._setAsyncGuaranteed();
        return ret2;
      };
      Promise2.all = function(promises) {
        return new PromiseArray(promises).promise();
      };
      Promise2.cast = function(obj2) {
        var ret2 = tryConvertToPromise(obj2);
        if (!(ret2 instanceof Promise2)) {
          ret2 = new Promise2(INTERNAL);
          ret2._captureStackTrace();
          ret2._setFulfilled();
          ret2._rejectionHandler0 = obj2;
        }
        return ret2;
      };
      Promise2.resolve = Promise2.fulfilled = Promise2.cast;
      Promise2.reject = Promise2.rejected = function(reason) {
        var ret2 = new Promise2(INTERNAL);
        ret2._captureStackTrace();
        ret2._rejectCallback(reason, true);
        return ret2;
      };
      Promise2.setScheduler = function(fn) {
        if (typeof fn !== "function") {
          throw new TypeError2("expecting a function but got " + util.classString(fn));
        }
        return async.setScheduler(fn);
      };
      Promise2.prototype._then = function(didFulfill, didReject, _, receiver2, internalData) {
        var haveInternalData = internalData !== void 0;
        var promise = haveInternalData ? internalData : new Promise2(INTERNAL);
        var target = this._target();
        var bitField = target._bitField;
        if (!haveInternalData) {
          promise._propagateFrom(this, 3);
          promise._captureStackTrace();
          if (receiver2 === void 0 && (this._bitField & 2097152) !== 0) {
            if (!((bitField & 50397184) === 0)) {
              receiver2 = this._boundValue();
            } else {
              receiver2 = target === this ? void 0 : this._boundTo;
            }
          }
          this._fireEvent("promiseChained", this, promise);
        }
        var context = getContext();
        if (!((bitField & 50397184) === 0)) {
          var handler, value, settler = target._settlePromiseCtx;
          if ((bitField & 33554432) !== 0) {
            value = target._rejectionHandler0;
            handler = didFulfill;
          } else if ((bitField & 16777216) !== 0) {
            value = target._fulfillmentHandler0;
            handler = didReject;
            target._unsetRejectionIsUnhandled();
          } else {
            settler = target._settlePromiseLateCancellationObserver;
            value = new CancellationError("late cancellation observer");
            target._attachExtraTrace(value);
            handler = didReject;
          }
          async.invoke(settler, target, {
            handler: util.contextBind(context, handler),
            promise,
            receiver: receiver2,
            value
          });
        } else {
          target._addCallbacks(
            didFulfill,
            didReject,
            promise,
            receiver2,
            context
          );
        }
        return promise;
      };
      Promise2.prototype._length = function() {
        return this._bitField & 65535;
      };
      Promise2.prototype._isFateSealed = function() {
        return (this._bitField & 117506048) !== 0;
      };
      Promise2.prototype._isFollowing = function() {
        return (this._bitField & 67108864) === 67108864;
      };
      Promise2.prototype._setLength = function(len) {
        this._bitField = this._bitField & -65536 | len & 65535;
      };
      Promise2.prototype._setFulfilled = function() {
        this._bitField = this._bitField | 33554432;
        this._fireEvent("promiseFulfilled", this);
      };
      Promise2.prototype._setRejected = function() {
        this._bitField = this._bitField | 16777216;
        this._fireEvent("promiseRejected", this);
      };
      Promise2.prototype._setFollowing = function() {
        this._bitField = this._bitField | 67108864;
        this._fireEvent("promiseResolved", this);
      };
      Promise2.prototype._setIsFinal = function() {
        this._bitField = this._bitField | 4194304;
      };
      Promise2.prototype._isFinal = function() {
        return (this._bitField & 4194304) > 0;
      };
      Promise2.prototype._unsetCancelled = function() {
        this._bitField = this._bitField & ~65536;
      };
      Promise2.prototype._setCancelled = function() {
        this._bitField = this._bitField | 65536;
        this._fireEvent("promiseCancelled", this);
      };
      Promise2.prototype._setWillBeCancelled = function() {
        this._bitField = this._bitField | 8388608;
      };
      Promise2.prototype._setAsyncGuaranteed = function() {
        if (async.hasCustomScheduler()) return;
        var bitField = this._bitField;
        this._bitField = bitField | (bitField & 536870912) >> 2 ^ 134217728;
      };
      Promise2.prototype._setNoAsyncGuarantee = function() {
        this._bitField = (this._bitField | 536870912) & ~134217728;
      };
      Promise2.prototype._receiverAt = function(index) {
        var ret2 = index === 0 ? this._receiver0 : this[index * 4 - 4 + 3];
        if (ret2 === UNDEFINED_BINDING) {
          return void 0;
        } else if (ret2 === void 0 && this._isBound()) {
          return this._boundValue();
        }
        return ret2;
      };
      Promise2.prototype._promiseAt = function(index) {
        return this[index * 4 - 4 + 2];
      };
      Promise2.prototype._fulfillmentHandlerAt = function(index) {
        return this[index * 4 - 4 + 0];
      };
      Promise2.prototype._rejectionHandlerAt = function(index) {
        return this[index * 4 - 4 + 1];
      };
      Promise2.prototype._boundValue = function() {
      };
      Promise2.prototype._migrateCallback0 = function(follower) {
        var bitField = follower._bitField;
        var fulfill = follower._fulfillmentHandler0;
        var reject = follower._rejectionHandler0;
        var promise = follower._promise0;
        var receiver2 = follower._receiverAt(0);
        if (receiver2 === void 0) receiver2 = UNDEFINED_BINDING;
        this._addCallbacks(fulfill, reject, promise, receiver2, null);
      };
      Promise2.prototype._migrateCallbackAt = function(follower, index) {
        var fulfill = follower._fulfillmentHandlerAt(index);
        var reject = follower._rejectionHandlerAt(index);
        var promise = follower._promiseAt(index);
        var receiver2 = follower._receiverAt(index);
        if (receiver2 === void 0) receiver2 = UNDEFINED_BINDING;
        this._addCallbacks(fulfill, reject, promise, receiver2, null);
      };
      Promise2.prototype._addCallbacks = function(fulfill, reject, promise, receiver2, context) {
        var index = this._length();
        if (index >= 65535 - 4) {
          index = 0;
          this._setLength(0);
        }
        if (index === 0) {
          this._promise0 = promise;
          this._receiver0 = receiver2;
          if (typeof fulfill === "function") {
            this._fulfillmentHandler0 = util.contextBind(context, fulfill);
          }
          if (typeof reject === "function") {
            this._rejectionHandler0 = util.contextBind(context, reject);
          }
        } else {
          var base = index * 4 - 4;
          this[base + 2] = promise;
          this[base + 3] = receiver2;
          if (typeof fulfill === "function") {
            this[base + 0] = util.contextBind(context, fulfill);
          }
          if (typeof reject === "function") {
            this[base + 1] = util.contextBind(context, reject);
          }
        }
        this._setLength(index + 1);
        return index;
      };
      Promise2.prototype._proxy = function(proxyable, arg) {
        this._addCallbacks(void 0, void 0, arg, proxyable, null);
      };
      Promise2.prototype._resolveCallback = function(value, shouldBind) {
        if ((this._bitField & 117506048) !== 0) return;
        if (value === this)
          return this._rejectCallback(makeSelfResolutionError(), false);
        var maybePromise = tryConvertToPromise(value, this);
        if (!(maybePromise instanceof Promise2)) return this._fulfill(value);
        if (shouldBind) this._propagateFrom(maybePromise, 2);
        var promise = maybePromise._target();
        if (promise === this) {
          this._reject(makeSelfResolutionError());
          return;
        }
        var bitField = promise._bitField;
        if ((bitField & 50397184) === 0) {
          var len = this._length();
          if (len > 0) promise._migrateCallback0(this);
          for (var i = 1; i < len; ++i) {
            promise._migrateCallbackAt(this, i);
          }
          this._setFollowing();
          this._setLength(0);
          this._setFollowee(maybePromise);
        } else if ((bitField & 33554432) !== 0) {
          this._fulfill(promise._value());
        } else if ((bitField & 16777216) !== 0) {
          this._reject(promise._reason());
        } else {
          var reason = new CancellationError("late cancellation observer");
          promise._attachExtraTrace(reason);
          this._reject(reason);
        }
      };
      Promise2.prototype._rejectCallback = function(reason, synchronous, ignoreNonErrorWarnings) {
        var trace = util.ensureErrorObject(reason);
        var hasStack = trace === reason;
        if (!hasStack && !ignoreNonErrorWarnings && debug.warnings()) {
          var message = "a promise was rejected with a non-error: " + util.classString(reason);
          this._warn(message, true);
        }
        this._attachExtraTrace(trace, synchronous ? hasStack : false);
        this._reject(reason);
      };
      Promise2.prototype._resolveFromExecutor = function(executor) {
        if (executor === INTERNAL) return;
        var promise = this;
        this._captureStackTrace();
        this._pushContext();
        var synchronous = true;
        var r = this._execute(executor, function(value) {
          promise._resolveCallback(value);
        }, function(reason) {
          promise._rejectCallback(reason, synchronous);
        });
        synchronous = false;
        this._popContext();
        if (r !== void 0) {
          promise._rejectCallback(r, true);
        }
      };
      Promise2.prototype._settlePromiseFromHandler = function(handler, receiver2, value, promise) {
        var bitField = promise._bitField;
        if ((bitField & 65536) !== 0) return;
        promise._pushContext();
        var x;
        if (receiver2 === APPLY) {
          if (!value || typeof value.length !== "number") {
            x = errorObj2;
            x.e = new TypeError2("cannot .spread() a non-array: " + util.classString(value));
          } else {
            x = tryCatch2(handler).apply(this._boundValue(), value);
          }
        } else {
          x = tryCatch2(handler).call(receiver2, value);
        }
        var promiseCreated = promise._popContext();
        bitField = promise._bitField;
        if ((bitField & 65536) !== 0) return;
        if (x === NEXT_FILTER) {
          promise._reject(value);
        } else if (x === errorObj2) {
          promise._rejectCallback(x.e, false);
        } else {
          debug.checkForgottenReturns(x, promiseCreated, "", promise, this);
          promise._resolveCallback(x);
        }
      };
      Promise2.prototype._target = function() {
        var ret2 = this;
        while (ret2._isFollowing()) ret2 = ret2._followee();
        return ret2;
      };
      Promise2.prototype._followee = function() {
        return this._rejectionHandler0;
      };
      Promise2.prototype._setFollowee = function(promise) {
        this._rejectionHandler0 = promise;
      };
      Promise2.prototype._settlePromise = function(promise, handler, receiver2, value) {
        var isPromise = promise instanceof Promise2;
        var bitField = this._bitField;
        var asyncGuaranteed = (bitField & 134217728) !== 0;
        if ((bitField & 65536) !== 0) {
          if (isPromise) promise._invokeInternalOnCancel();
          if (receiver2 instanceof PassThroughHandlerContext && receiver2.isFinallyHandler()) {
            receiver2.cancelPromise = promise;
            if (tryCatch2(handler).call(receiver2, value) === errorObj2) {
              promise._reject(errorObj2.e);
            }
          } else if (handler === reflectHandler2) {
            promise._fulfill(reflectHandler2.call(receiver2));
          } else if (receiver2 instanceof Proxyable) {
            receiver2._promiseCancelled(promise);
          } else if (isPromise || promise instanceof PromiseArray) {
            promise._cancel();
          } else {
            receiver2.cancel();
          }
        } else if (typeof handler === "function") {
          if (!isPromise) {
            handler.call(receiver2, value, promise);
          } else {
            if (asyncGuaranteed) promise._setAsyncGuaranteed();
            this._settlePromiseFromHandler(handler, receiver2, value, promise);
          }
        } else if (receiver2 instanceof Proxyable) {
          if (!receiver2._isResolved()) {
            if ((bitField & 33554432) !== 0) {
              receiver2._promiseFulfilled(value, promise);
            } else {
              receiver2._promiseRejected(value, promise);
            }
          }
        } else if (isPromise) {
          if (asyncGuaranteed) promise._setAsyncGuaranteed();
          if ((bitField & 33554432) !== 0) {
            promise._fulfill(value);
          } else {
            promise._reject(value);
          }
        }
      };
      Promise2.prototype._settlePromiseLateCancellationObserver = function(ctx) {
        var handler = ctx.handler;
        var promise = ctx.promise;
        var receiver2 = ctx.receiver;
        var value = ctx.value;
        if (typeof handler === "function") {
          if (!(promise instanceof Promise2)) {
            handler.call(receiver2, value, promise);
          } else {
            this._settlePromiseFromHandler(handler, receiver2, value, promise);
          }
        } else if (promise instanceof Promise2) {
          promise._reject(value);
        }
      };
      Promise2.prototype._settlePromiseCtx = function(ctx) {
        this._settlePromise(ctx.promise, ctx.handler, ctx.receiver, ctx.value);
      };
      Promise2.prototype._settlePromise0 = function(handler, value, bitField) {
        var promise = this._promise0;
        var receiver2 = this._receiverAt(0);
        this._promise0 = void 0;
        this._receiver0 = void 0;
        this._settlePromise(promise, handler, receiver2, value);
      };
      Promise2.prototype._clearCallbackDataAtIndex = function(index) {
        var base = index * 4 - 4;
        this[base + 2] = this[base + 3] = this[base + 0] = this[base + 1] = void 0;
      };
      Promise2.prototype._fulfill = function(value) {
        var bitField = this._bitField;
        if ((bitField & 117506048) >>> 16) return;
        if (value === this) {
          var err = makeSelfResolutionError();
          this._attachExtraTrace(err);
          return this._reject(err);
        }
        this._setFulfilled();
        this._rejectionHandler0 = value;
        if ((bitField & 65535) > 0) {
          if ((bitField & 134217728) !== 0) {
            this._settlePromises();
          } else {
            async.settlePromises(this);
          }
          this._dereferenceTrace();
        }
      };
      Promise2.prototype._reject = function(reason) {
        var bitField = this._bitField;
        if ((bitField & 117506048) >>> 16) return;
        this._setRejected();
        this._fulfillmentHandler0 = reason;
        if (this._isFinal()) {
          return async.fatalError(reason, util.isNode);
        }
        if ((bitField & 65535) > 0) {
          async.settlePromises(this);
        } else {
          this._ensurePossibleRejectionHandled();
        }
      };
      Promise2.prototype._fulfillPromises = function(len, value) {
        for (var i = 1; i < len; i++) {
          var handler = this._fulfillmentHandlerAt(i);
          var promise = this._promiseAt(i);
          var receiver2 = this._receiverAt(i);
          this._clearCallbackDataAtIndex(i);
          this._settlePromise(promise, handler, receiver2, value);
        }
      };
      Promise2.prototype._rejectPromises = function(len, reason) {
        for (var i = 1; i < len; i++) {
          var handler = this._rejectionHandlerAt(i);
          var promise = this._promiseAt(i);
          var receiver2 = this._receiverAt(i);
          this._clearCallbackDataAtIndex(i);
          this._settlePromise(promise, handler, receiver2, reason);
        }
      };
      Promise2.prototype._settlePromises = function() {
        var bitField = this._bitField;
        var len = bitField & 65535;
        if (len > 0) {
          if ((bitField & 16842752) !== 0) {
            var reason = this._fulfillmentHandler0;
            this._settlePromise0(this._rejectionHandler0, reason, bitField);
            this._rejectPromises(len, reason);
          } else {
            var value = this._rejectionHandler0;
            this._settlePromise0(this._fulfillmentHandler0, value, bitField);
            this._fulfillPromises(len, value);
          }
          this._setLength(0);
        }
        this._clearCancellationData();
      };
      Promise2.prototype._settledValue = function() {
        var bitField = this._bitField;
        if ((bitField & 33554432) !== 0) {
          return this._rejectionHandler0;
        } else if ((bitField & 16777216) !== 0) {
          return this._fulfillmentHandler0;
        }
      };
      if (typeof Symbol !== "undefined" && Symbol.toStringTag) {
        es52.defineProperty(Promise2.prototype, Symbol.toStringTag, {
          get: function() {
            return "Object";
          }
        });
      }
      function deferResolve(v) {
        this.promise._resolveCallback(v);
      }
      function deferReject(v) {
        this.promise._rejectCallback(v, false);
      }
      Promise2.defer = Promise2.pending = function() {
        debug.deprecated("Promise.defer", "new Promise");
        var promise = new Promise2(INTERNAL);
        return {
          promise,
          resolve: deferResolve,
          reject: deferReject
        };
      };
      util.notEnumerableProp(
        Promise2,
        "_makeSelfResolutionError",
        makeSelfResolutionError
      );
      require_method()(
        Promise2,
        INTERNAL,
        tryConvertToPromise,
        apiRejection,
        debug
      );
      require_bind()(Promise2, INTERNAL, tryConvertToPromise, debug);
      require_cancel()(Promise2, PromiseArray, apiRejection, debug);
      require_direct_resolve()(Promise2);
      require_synchronous_inspection()(Promise2);
      require_join()(
        Promise2,
        PromiseArray,
        tryConvertToPromise,
        INTERNAL,
        async
      );
      Promise2.Promise = Promise2;
      Promise2.version = "3.7.2";
      require_call_get()(Promise2);
      require_generators()(Promise2, apiRejection, INTERNAL, tryConvertToPromise, Proxyable, debug);
      require_map()(Promise2, PromiseArray, apiRejection, tryConvertToPromise, INTERNAL, debug);
      require_nodeify()(Promise2);
      require_promisify()(Promise2, INTERNAL);
      require_props()(Promise2, PromiseArray, tryConvertToPromise, apiRejection);
      require_race()(Promise2, INTERNAL, tryConvertToPromise, apiRejection);
      require_reduce()(Promise2, PromiseArray, apiRejection, tryConvertToPromise, INTERNAL, debug);
      require_settle()(Promise2, PromiseArray, debug);
      require_some()(Promise2, PromiseArray, apiRejection);
      require_timers()(Promise2, INTERNAL, debug);
      require_using()(Promise2, apiRejection, tryConvertToPromise, createContext, INTERNAL, debug);
      require_any()(Promise2);
      require_each()(Promise2, INTERNAL);
      require_filter()(Promise2, INTERNAL);
      util.toFastProperties(Promise2);
      util.toFastProperties(Promise2.prototype);
      function fillTypes(value) {
        var p = new Promise2(INTERNAL);
        p._fulfillmentHandler0 = value;
        p._rejectionHandler0 = value;
        p._promise0 = value;
        p._receiver0 = value;
      }
      fillTypes({ a: 1 });
      fillTypes({ b: 2 });
      fillTypes({ c: 3 });
      fillTypes(1);
      fillTypes(function() {
      });
      fillTypes(void 0);
      fillTypes(false);
      fillTypes(new Promise2(INTERNAL));
      debug.setBounds(Async.firstLineError, util.lastLineError);
      return Promise2;
    };
  }
});

// node_modules/bluebird/js/release/bluebird.js
var require_bluebird = __commonJS({
  "node_modules/bluebird/js/release/bluebird.js"(exports2, module2) {
    "use strict";
    var old;
    if (typeof Promise !== "undefined") old = Promise;
    function noConflict() {
      try {
        if (Promise === bluebird) Promise = old;
      } catch (e) {
      }
      return bluebird;
    }
    var bluebird = require_promise()();
    bluebird.noConflict = noConflict;
    module2.exports = bluebird;
  }
});

// node_modules/node-ssdp/lib/server.js
var require_server = __commonJS({
  "node_modules/node-ssdp/lib/server.js"(exports2, module2) {
    "use strict";
    var SSDP = require_lib();
    var util = require("util");
    var assert = require("assert");
    var c = require_const();
    var Promise2 = require_bluebird();
    var SsdpHeader = require_ssdpHeader();
    function SsdpServer(opts) {
      this._subclass = "node-ssdp:server";
      SSDP.call(this, opts);
      this._adLoopInterval = null;
      if (opts && opts.sourcePort && opts.sourcePort != c.SSDP_DEFAULT_PORT) {
        this._logger("WARNING: SSDP server `sourcePort` option is set to non-SSDP port. Server will likely receive no messages. It is highly recommended to not pass this option to SSDP server constructor.");
      }
      if (!this._sourcePort) this._sourcePort = c.SSDP_DEFAULT_PORT;
    }
    util.inherits(SsdpServer, SSDP);
    SsdpServer.prototype.start = function(cb) {
      var self2 = this;
      if (self2._socketBound) {
        self2._logger("Server already running.");
        return;
      }
      self2._socketBound = true;
      if (!self2._suppressRootDeviceAdvertisements) {
        this._usns[this._udn] = this._udn;
      }
      return new Promise2(function(success, failure) {
        function onBind(err) {
          self2._initAdLoop.apply(self2, arguments);
          if (cb) cb.apply(self2, arguments);
          if (err) return failure(err);
          success();
        }
        self2._start(onBind);
      });
    };
    SsdpServer.prototype._initAdLoop = function() {
      var self2 = this;
      setTimeout(self2.advertise.bind(self2), 3e3);
      self2._startAdLoop();
    };
    SsdpServer.prototype.stop = function() {
      if (!this.sockets) {
        this._logger("Already stopped.");
        return;
      }
      this.advertise(false);
      this._stopAdLoop();
      this._stop();
    };
    SsdpServer.prototype._startAdLoop = function() {
      assert.strictEqual(this._adLoopInterval, null, "Attempting to start a parallel ad loop");
      this._adLoopInterval = setInterval(this.advertise.bind(this), this._adInterval);
    };
    SsdpServer.prototype._stopAdLoop = function() {
      assert.notStrictEqual(this._adLoopInterval, null, "Attempting to clear a non-existing interval");
      clearInterval(this._adLoopInterval);
      this._adLoopInterval = null;
    };
    SsdpServer.prototype.advertise = function(alive) {
      var self2 = this;
      if (!this.sockets) return;
      if (alive === void 0) alive = true;
      Object.keys(self2._usns).forEach(function(usn) {
        var udn = self2._usns[usn], nts = alive ? c.SSDP_ALIVE : c.SSDP_BYE;
        var header = new SsdpHeader(c.NOTIFY, {
          "HOST": self2._ssdpServerHost,
          "NT": usn,
          // notification type, in this case same as ST
          "NTS": nts,
          "USN": udn
        });
        if (alive) {
          if (self2._location) {
            header.setHeader("LOCATION", self2._location);
          } else {
            header.overrideLocationOnSend();
          }
          header.setHeader("CACHE-CONTROL", "max-age=1800");
          header.setHeader("SERVER", self2._ssdpSig);
        }
        header.setHeaders(self2._extraHeaders);
        self2._logger("Sending an advertisement event");
        self2._send(header, function(err, bytes) {
          self2._logger("Outgoing server message: %o", { "message": header.toString(), id: header.id() });
        });
      });
    };
    module2.exports = SsdpServer;
  }
});

// node_modules/node-ssdp/lib/client.js
var require_client = __commonJS({
  "node_modules/node-ssdp/lib/client.js"(exports2, module2) {
    "use strict";
    var SSDP = require_lib();
    var util = require("util");
    var c = require_const();
    var Promise2 = require_bluebird();
    var SsdpHeader = require_ssdpHeader();
    function SsdpClient(opts) {
      this._subclass = "node-ssdp:client";
      SSDP.call(this, opts);
    }
    util.inherits(SsdpClient, SSDP);
    SsdpClient.prototype.start = function(cb) {
      var self2 = this;
      return new Promise2(function(success, failure) {
        function onBind(err) {
          if (cb) cb.apply(self2, arguments);
          if (err) return failure(err);
          success();
        }
        self2._start(onBind);
      });
    };
    SsdpClient.prototype.stop = function() {
      this._stop();
    };
    SsdpClient.prototype.search = function search(serviceType) {
      var self2 = this;
      if (!this._started) {
        return this.start(function() {
          self2.search(serviceType);
        });
      }
      var header = new SsdpHeader(c.M_SEARCH, {
        "HOST": self2._ssdpServerHost,
        "ST": serviceType,
        "MAN": '"ssdp:discover"',
        "MX": 3
      });
      self2._logger("Attempting to send an M-SEARCH request");
      self2._send(header, function(err, bytes) {
        if (err) {
          self2._logger("Error: unable to send M-SEARCH request ID %s: %o", header.id(), err);
        } else {
          self2._logger("Sent M-SEARCH request: %o", { "message": header.toString(), id: header.id() });
        }
      });
    };
    module2.exports = SsdpClient;
  }
});

// node_modules/node-ssdp/index.js
var require_node_ssdp = __commonJS({
  "node_modules/node-ssdp/index.js"(exports2, module2) {
    module2.exports = {
      Server: require_server(),
      Client: require_client(),
      Base: require_lib()
    };
  }
});

// tv-service.js
var os = require("os");
var http = require("http");
var fs = require("fs");
var path = require("path");
var cp = require("child_process");
var WebSocketServer = require_ws().Server;
var genaLib = require_gena();
var soapLib = require_soap();
var parseLib = require_parse();
var inputLib = require_input();
var corrLib = require_correlator();
var startListener = genaLib.startListener;
var subscribe = genaLib.subscribe;
var renew = genaLib.renew;
var unsubscribe = genaLib.unsubscribe;
var getVolume = soapLib.getVolume;
var setSonosVolume = soapLib.setVolume;
var getTransportState = soapLib.getTransportState;
var parseLastChange = parseLib.parseLastChange;
var openInputDevicesMulti = inputLib.openInputDevicesMulti;
var detectInputDevice = inputLib.detectInputDevice;
var Correlator = corrLib.Correlator;
var EVENT_PATH = "/MediaRenderer/RenderingControl/Event";
var REQUESTED_TIMEOUT = 1800;
var RENEW_FACTOR = 0.8;
var DEFAULT_INPUT_DEV = "/dev/input/event1";
var DEFAULT_LISTEN_PORT = 7474;
var DEFAULT_WS_PORT = 7475;
var API_PORT = 7476;
var CONFIG_DIR = "/var/lib/com.brineandbuild.sonosoverlay";
var CONFIG_FILE = CONFIG_DIR + "/config.json";
var RETRY_BASE_MS = 3e3;
var RETRY_MAX_MS = 6e4;
var SYNC_INTERVAL_MS = 1e4;
var TV_TO_SONOS_RATIO = 2;
var DEFAULT_MAX_VOLUME = 70;
var MAX_RAISE_PER_CORRECTION = 20;
var SETTLE_MS = 400;
var SETTLE_POLL_MS = 700;
var APP_DIR = path.dirname(process.argv[1] || __filename);
var state = {
  device: null,
  config: null,
  sid: null,
  seqExpected: 0,
  renewTimer: null,
  correlator: null,
  inputHandle: null,
  retryTimer: null,
  server: null,
  wss: null,
  apiServer: null,
  lastKeyAt: 0,
  genaReceived: false,
  connecting: false,
  tvVol: null,
  // last value read from the TV — the source of truth
  sonosVol: null,
  // last value the Arc reported, via GENA or SOAP
  pendingSonosWrite: null,
  // a SetVolume we issued, so its echo is not "external"
  optimisticMuted: false,
  maxVolume: DEFAULT_MAX_VOLUME,
  settleTimer: null,
  transportState: null,
  holding: false
};
function readConfig() {
  try {
    return JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"));
  } catch (e) {
    return null;
  }
}
function saveConfig(cfg) {
  try {
    if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true });
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2), "utf8");
  } catch (e) {
    console.error("[config] write failed:", e.message);
  }
}
function openPorts() {
  [DEFAULT_LISTEN_PORT, DEFAULT_WS_PORT, API_PORT].forEach(function(p) {
    cp.exec("iptables -I INPUT -p tcp --dport " + p + " -j ACCEPT 2>/dev/null");
  });
}
function scanSonos(timeoutMs) {
  return new Promise(function(resolve) {
    var Client = require_node_ssdp().Client;
    var client = new Client();
    var found = {};
    client.on("response", function(headers) {
      var loc = headers.LOCATION || headers.location || "";
      var m = loc.match(/http:\/\/([\d.]+):(\d+)/);
      if (m) {
        var ip = m[1], port = parseInt(m[2], 10);
        if (!found[ip]) found[ip] = { ip, port, name: ip, model: "", uuid: "" };
      }
    });
    client.search("urn:schemas-upnp-org:device:ZonePlayer:1");
    setTimeout(function() {
      client.stop();
      var devices = Object.values(found);
      if (!devices.length) {
        resolve([]);
        return;
      }
      var pending = devices.length;
      devices.forEach(function(dev) {
        var req = http.get("http://" + dev.ip + ":" + dev.port + "/xml/device_description.xml", function(res) {
          var data = "";
          res.on("data", function(c) {
            data += c;
          });
          res.on("end", function() {
            var rm = data.match(/<roomName>([^<]+)<\/roomName>/);
            var mm = data.match(/<modelName>([^<]+)<\/modelName>/);
            var um = data.match(/<UDN>([^<]+)<\/UDN>/);
            if (rm) dev.name = rm[1];
            if (mm) dev.model = mm[1];
            if (um) dev.uuid = um[1];
            if (--pending === 0) resolve(devices);
          });
        });
        req.on("error", function() {
          if (--pending === 0) resolve(devices);
        });
        req.setTimeout(3e3, function() {
          req.abort();
        });
      });
    }, timeoutMs || 5e3);
  });
}
function startApiServer() {
  state.apiServer = http.createServer(function(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    res.setHeader("Content-Type", "application/json");
    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }
    var url = req.url.split("?")[0];
    if (url === "/api/status" && req.method === "GET") {
      res.end(JSON.stringify({
        ok: true,
        configured: !!state.config,
        connecting: state.connecting,
        connected: !!state.sid,
        sonosIp: state.config ? state.config.sonosIp : null,
        sonosName: state.config ? state.config.sonosName : null,
        lastVol: state.correlator ? state.correlator.lastVol : null,
        lastMuted: state.correlator ? state.correlator.lastMuted : null,
        wsClients: state.wss ? state.wss.clients.size : 0,
        lastKeyAt: state.lastKeyAt,
        genaReceived: state.genaReceived
      }));
      return;
    }
    if (url === "/api/scan" && req.method === "GET") {
      console.log("[api] SSDP scan starting...");
      scanSonos(5e3).then(function(devices) {
        console.log("[api] scan found", devices.length, "device(s)");
        res.end(JSON.stringify({ ok: true, devices }));
      });
      return;
    }
    if (url === "/api/test" && req.method === "POST") {
      readBody(req, function(body) {
        var ip = body.sonosIp;
        var port = body.sonosPort || 1400;
        if (!ip) {
          res.end(JSON.stringify({ ok: false, error: "Missing sonosIp" }));
          return;
        }
        getVolume({ ip, port }).then(function(vol) {
          res.end(JSON.stringify({ ok: true, volume: vol }));
        }).catch(function(e) {
          res.end(JSON.stringify({ ok: false, error: e.message }));
        });
      });
      return;
    }
    if (url === "/api/config" && req.method === "POST") {
      readBody(req, function(body) {
        if (!body.sonosIp) {
          res.end(JSON.stringify({ ok: false, error: "Missing sonosIp" }));
          return;
        }
        saveConfig(body);
        state.config = body;
        console.log("[api] config saved, connecting to Sonos at", body.sonosIp);
        connectToSonos(body).catch(function(e) {
          console.error("[api] connect error after config save:", e.message);
        });
        res.end(JSON.stringify({ ok: true }));
      });
      return;
    }
    if (url === "/api/setup-mode" && req.method === "POST") {
      if (state.wss) {
        state.wss.clients.forEach(function(client) {
          if (client.readyState === 1) client.send(JSON.stringify({ type: "setup" }));
        });
      }
      res.end(JSON.stringify({ ok: true }));
      return;
    }
    if (url === "/api/startup-hook" && req.method === "POST") {
      var hookDir = "/var/lib/webosbrew/init.d";
      var hookPath = hookDir + "/sonos-overlay";
      var script = '#!/bin/sh\niptables -I INPUT -p tcp --dport 7474 -j ACCEPT 2>/dev/null\niptables -I INPUT -p tcp --dport 7475 -j ACCEPT 2>/dev/null\niptables -I INPUT -p tcp --dport 7476 -j ACCEPT 2>/dev/null\nQML=/usr/lib/qml/WebOSCompositor/views/volume/StarfishVolume.qml\nPATCHED=/var/lib/com.brineandbuild.sonosoverlay/StarfishVolume.qml\n# Generate patched QML on first run or after a firmware update\nif [ -f "$QML" ] && grep -q external_arc "$QML" && [ ! -f "$PATCHED" ]; then\n  mkdir -p /var/lib/com.brineandbuild.sonosoverlay\n  sed \'/external_arc/d\' "$QML" > "$PATCHED"\nfi\n# Apply bind mount and restart compositor only if not already mounted\nif [ -f "$PATCHED" ] && ! grep -q StarfishVolume.qml /proc/mounts; then\n  mount --bind "$PATCHED" "$QML" 2>/dev/null\n  systemctl restart surface-manager-daemon.service 2>/dev/null\nfi\npkill -f \'tv-service.bundle.js\' 2>/dev/null\nsleep 1\nAPP=/media/developer/apps/usr/palm/applications/com.brineandbuild.sonosoverlay\nnohup /usr/bin/node "$APP/tv-service.bundle.js" >> /var/log/sonos-overlay.log 2>&1 &\n';
      try {
        if (!fs.existsSync(hookDir)) fs.mkdirSync(hookDir, { recursive: true });
        fs.writeFileSync(hookPath, script, "utf8");
        fs.chmodSync(hookPath, 493);
        res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
      return;
    }
    res.writeHead(404);
    res.end(JSON.stringify({ ok: false, error: "Not found" }));
  });
  state.apiServer.listen(API_PORT, function() {
    console.log("[api]  setup server on :" + API_PORT);
  });
}
function readBody(req, cb) {
  var raw = "";
  req.on("data", function(c) {
    raw += c;
  });
  req.on("end", function() {
    try {
      cb(JSON.parse(raw || "{}"));
    } catch (e) {
      cb({});
    }
  });
}
async function resolveDevice(config) {
  var port = config.sonosPort || 1400;
  if (config.sonosIp) {
    try {
      await getVolume({ ip: config.sonosIp, port });
      return { ip: config.sonosIp, port };
    } catch (e) {
      console.warn("[resolve] stored IP", config.sonosIp, "unreachable:", e.message);
    }
  }
  console.log("[resolve] scanning for the configured player...");
  var devices = await scanSonos(5e3);
  if (!devices.length) throw new Error("no Sonos devices found on the network");
  var match = null;
  if (config.sonosUuid) {
    match = devices.filter(function(d) {
      return d.uuid === config.sonosUuid;
    })[0] || null;
    if (match) console.log("[resolve] matched stored UUID at", match.ip);
  }
  if (!match && config.sonosName) {
    var byName = devices.filter(function(d) {
      return d.name === config.sonosName && (!config.sonosModel || d.model === config.sonosModel);
    });
    if (byName.length === 1) {
      match = byName[0];
      console.log("[resolve] matched name+model at", match.ip);
    } else if (byName.length > 1) {
      console.warn("[resolve]", byName.length, 'players share room "' + config.sonosName + '" \u2014 cannot disambiguate without a stored UUID');
    }
  }
  if (!match) throw new Error("could not identify the configured Sonos among " + devices.length + " device(s)");
  config.sonosIp = match.ip;
  if (match.uuid) config.sonosUuid = match.uuid;
  if (match.model) config.sonosModel = match.model;
  saveConfig(config);
  state.config = config;
  return { ip: match.ip, port };
}
async function connectToSonos(config, attempt) {
  if (state.connecting) return;
  state.connecting = true;
  attempt = attempt || 0;
  if (state.retryTimer) {
    clearTimeout(state.retryTimer);
    state.retryTimer = null;
  }
  if (state.sid) {
    try {
      await unsubscribe(state.device, state.sid, EVENT_PATH);
    } catch (e) {
    }
    state.sid = null;
    state.seqExpected = 0;
    state.genaReceived = false;
  }
  if (state.renewTimer) {
    clearTimeout(state.renewTimer);
    state.renewTimer = null;
  }
  var args = parseArgs();
  try {
    var device = await resolveDevice(config);
    state.device = device;
    var callbackIp = args.callbackIp || detectLanIp();
    var callbackUrl = "http://" + callbackIp + ":" + DEFAULT_LISTEN_PORT + "/notify";
    console.log("[main] Sonos:    ", device.ip + ":" + device.port);
    console.log("[main] callback: ", callbackUrl);
    state.maxVolume = config && typeof config.maxVolume === "number" ? Math.max(1, Math.min(100, config.maxVolume)) : DEFAULT_MAX_VOLUME;
    console.log(
      "[main] volume ceiling:",
      state.maxVolume,
      "Sonos =",
      Math.floor(state.maxVolume / TV_TO_SONOS_RATIO),
      "TV"
    );
    state.correlator = new Correlator({ windowMs: 3e3 });
    var initVol = await getVolume(device);
    state.correlator.lastVol = initVol;
    state.correlator.lastMuted = false;
    console.log("[main] current volume:", initVol);
    var seed = Math.min(sonosToTv(initVol), maxTvVol());
    state.sonosVol = initVol;
    state.tvVol = seed;
    state.optimisticMuted = false;
    console.log("[main] seeding TV to", seed, "from Arc", initVol, "(2:1)");
    pushTvVolume(seed);
    refreshTransportState();
    if (!config.sonosUuid) {
      scanSonos(5e3).then(function(devices) {
        var self2 = devices.filter(function(d) {
          return d.ip === device.ip;
        })[0];
        if (self2 && self2.uuid) {
          config.sonosUuid = self2.uuid;
          config.sonosModel = self2.model;
          saveConfig(config);
          console.log("[resolve] learned UUID", self2.uuid);
        }
      }).catch(function() {
      });
    }
    var sub = await subscribe(device, callbackUrl, EVENT_PATH, REQUESTED_TIMEOUT);
    state.sid = sub.sid;
    console.log("[gena] subscribed SID:", sub.sid, "| negotiated:", sub.negotiatedSeconds + "s");
    scheduleRenew(device, callbackUrl, sub.negotiatedSeconds);
    if (!state.inputHandle) {
      var detected = await detectInputDevice();
      var devPaths = detected.candidates.length > 0 ? detected.candidates.map(function(c) {
        return c.path;
      }) : [DEFAULT_INPUT_DEV];
      console.log("[input] listening on", devPaths.length, "device(s):", devPaths.join(", "));
      state.inputHandle = openInputDevicesMulti(devPaths, onInputEvent, function(err) {
        console.error("[input] fatal:", err.message);
      });
    }
    console.log("\n[main] ready \u2014 press Vol+/Vol\u2212/Mute on the remote.\n");
  } catch (e) {
    console.error("[main] connect failed (attempt " + (attempt + 1) + "):", e.message);
    var delayMs = Math.min(RETRY_BASE_MS * Math.pow(2, attempt), RETRY_MAX_MS);
    console.log("[main] retrying in", Math.round(delayMs / 1e3) + "s");
    state.retryTimer = setTimeout(function() {
      connectToSonos(config, attempt + 1);
    }, delayMs);
  } finally {
    state.connecting = false;
  }
}
function onInputEvent(event) {
  state.lastKeyAt = Date.now();
  var label = event.value === 1 ? "down  " : "repeat";
  console.log(
    "[input]",
    label,
    "dir=" + event.direction,
    "| kernel=" + event.kernelSec + "." + pad6(event.kernelUsec),
    "| src=" + (event.sourceDev || "?")
  );
  if (state.correlator) state.correlator.recordKeypress(event.direction, event.recvAt);
  state.holding = event.value === 2;
  onVolumeKey(event.direction);
}
function onVolumeKey(direction) {
  if (direction === "mute") {
    state.optimisticMuted = !state.optimisticMuted;
    return;
  }
  scheduleSettle();
}
function settled() {
  return Date.now() - state.lastKeyAt > SETTLE_MS;
}
function clampVol(v) {
  return Math.max(0, Math.min(state.maxVolume, v));
}
function tvToSonos(tvVol) {
  return Math.max(0, Math.min(100, tvVol * TV_TO_SONOS_RATIO));
}
function sonosToTv(sonosVol) {
  return Math.round(sonosVol / TV_TO_SONOS_RATIO);
}
function maxTvVol() {
  return Math.floor(state.maxVolume / TV_TO_SONOS_RATIO);
}
function refreshTransportState() {
  if (!state.device) return;
  getTransportState(state.device).then(function(ts) {
    if (ts && ts !== state.transportState) {
      console.log("[arc] transport state:", state.transportState, "->", ts);
    }
    if (ts) state.transportState = ts;
  }).catch(function() {
  });
}
function scheduleSettle() {
  if (state.settleTimer) clearTimeout(state.settleTimer);
  state.settleTimer = setTimeout(function() {
    state.settleTimer = null;
    if (!settled()) {
      scheduleSettle();
      return;
    }
    reconcileTvAndSonos();
  }, SETTLE_POLL_MS);
}
function reconcileTvAndSonos() {
  if (!state.device) return;
  readTvVolume(function(tvVol) {
    if (tvVol === null) return;
    state.tvVol = tvVol;
    var tvCap = maxTvVol();
    if (tvVol > tvCap) {
      console.warn(
        "[cap] TV at",
        tvVol,
        "\u2014 holding at TV ceiling",
        tvCap,
        "(Sonos " + state.maxVolume + ")"
      );
      pushTvVolume(tvCap);
      state.tvVol = tvCap;
    }
    getVolume(state.device).then(function(sonosVol) {
      state.sonosVol = sonosVol;
      var target = clampVol(tvToSonos(state.tvVol));
      if (target === sonosVol) return;
      if (target > sonosVol + MAX_RAISE_PER_CORRECTION) {
        target = sonosVol + MAX_RAISE_PER_CORRECTION;
        console.warn("[cap] limiting raise to", target, "- TV asked for", state.tvVol);
      }
      console.log(
        "[settle] TV",
        state.tvVol,
        "(wants Arc " + tvToSonos(state.tvVol) + ")",
        "| Arc",
        sonosVol,
        "-> setting Arc to",
        target
      );
      state.pendingSonosWrite = target;
      setSonosVolume(state.device, target).catch(function(e) {
        state.pendingSonosWrite = null;
        console.error("[settle] SetVolume failed:", e.message);
      });
    }).catch(function() {
    });
  });
}
function reconcile(genaVol, genaMuted) {
  if (genaMuted !== null) state.optimisticMuted = genaMuted;
  if (genaVol === null) return;
  var prev = state.sonosVol;
  state.sonosVol = genaVol;
  if (state.pendingSonosWrite !== null && genaVol === state.pendingSonosWrite) {
    state.pendingSonosWrite = null;
    return;
  }
  if (!settled()) return;
  if (state.tvVol !== null && genaVol !== tvToSonos(state.tvVol)) {
    var adopted = Math.min(sonosToTv(genaVol), maxTvVol());
    console.log(
      "[extern] Arc moved",
      prev,
      "->",
      genaVol,
      "with no keypress \u2014 adopting into TV as",
      adopted
    );
    state.tvVol = adopted;
    pushTvVolume(adopted);
  }
}
function onGenaNotify(headers, rawBody, recvAt) {
  var sid = headers["sid"] || "";
  var seq = parseInt(headers["seq"] || "0", 10);
  if (sid === state.sid) {
    if (seq !== state.seqExpected && !(seq === 0 && state.seqExpected > 0))
      console.warn("[gena] SEQ gap: expected", state.seqExpected, "got", seq);
    state.seqExpected = seq + 1;
  }
  var entries;
  try {
    entries = parseLastChange(rawBody);
  } catch (e) {
    console.error("[parse] error:", e.message);
    return;
  }
  var elapsed = process.hrtime(recvAt);
  var dispatchUs = Math.round((elapsed[0] * 1e9 + elapsed[1]) / 1e3);
  var masterVol = null, muted = null;
  entries.forEach(function(e) {
    if (e.name === "Volume" && e.channel === "Master") masterVol = Number(e.val);
    if (e.name === "Mute" && e.channel === "Master") muted = e.val !== 0 && e.val !== "0";
  });
  var summary = entries.map(function(e) {
    return e.name + "[" + e.channel + "]=" + e.val;
  }).join(" ");
  console.log("[gena]", "SEQ=" + seq, "|", summary, "| recv\u2192parsed=" + dispatchUs + "\xB5s");
  if (masterVol !== null || muted !== null) {
    state.genaReceived = true;
    if (state.correlator) state.correlator.recordGena(masterVol, muted, recvAt);
    broadcastVolume(masterVol, muted);
  }
}
function pushTvVolume(vol) {
  if (vol === null || vol === void 0 || isNaN(vol)) return;
  cp.exec(
    '/usr/bin/luna-send -n 1 luna://com.webos.service.audio/master/setVolume \'{"volume":' + vol + "}' 2>/dev/null",
    function() {
    }
  );
}
function readTvVolume(cb) {
  cp.exec(
    "/usr/bin/luna-send -n 1 -f luna://com.webos.service.audio/master/getVolume '{}' 2>/dev/null",
    { timeout: 5e3 },
    function(err, stdout) {
      if (err) {
        cb(null);
        return;
      }
      try {
        var parsed = JSON.parse(stdout);
        var v = parsed && parsed.volumeStatus ? parsed.volumeStatus.volume : null;
        cb(typeof v === "number" ? v : null);
      } catch (e) {
        cb(null);
      }
    }
  );
}
function broadcastVolume(vol, muted) {
  reconcile(vol, muted);
  var sendVol = state.tvVol !== null ? state.tvVol : state.sonosVol !== null ? state.sonosVol : 0;
  var sendMuted = state.optimisticMuted;
  if (!state.wss || state.wss.clients.size === 0) return;
  var msg = JSON.stringify({ vol: sendVol, muted: sendMuted });
  state.wss.clients.forEach(function(client) {
    if (client.readyState === 1) client.send(msg);
  });
}
function scheduleRenew(device, callbackUrl, negotiatedSeconds) {
  if (state.renewTimer) clearTimeout(state.renewTimer);
  var delayMs = Math.floor(negotiatedSeconds * RENEW_FACTOR * 1e3);
  console.log("[gena] renewal in", Math.round(delayMs / 1e3) + "s");
  state.renewTimer = setTimeout(async function() {
    try {
      var result = await renew(device, state.sid, EVENT_PATH, REQUESTED_TIMEOUT);
      console.log("[gena] renewed | negotiated:", result.negotiatedSeconds + "s");
      scheduleRenew(device, callbackUrl, result.negotiatedSeconds);
    } catch (e) {
      console.error("[gena] renew failed:", e.message, "\u2014 re-subscribing...");
      try {
        var sub = await subscribe(device, callbackUrl, EVENT_PATH, REQUESTED_TIMEOUT);
        state.sid = sub.sid;
        state.seqExpected = 0;
        console.log("[gena] re-subscribed SID:", sub.sid);
        scheduleRenew(device, callbackUrl, sub.negotiatedSeconds);
      } catch (e2) {
        console.error("[gena] re-subscribe failed:", e2.message);
      }
    }
  }, delayMs);
}
function startPeriodicSync() {
  setInterval(function() {
    if (!state.device || !state.correlator || !state.sid) return;
    refreshTransportState();
    if (!settled()) return;
    reconcileTvAndSonos();
  }, SYNC_INTERVAL_MS);
}
var shuttingDown = false;
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log("\n[main] shutting down...");
  if (state.renewTimer) clearTimeout(state.renewTimer);
  if (state.retryTimer) clearTimeout(state.retryTimer);
  if (state.settleTimer) clearTimeout(state.settleTimer);
  if (state.inputHandle) state.inputHandle.close();
  if (state.sid) await unsubscribe(state.device, state.sid, EVENT_PATH).catch(function() {
  });
  if (state.wss) state.wss.close();
  if (state.server) state.server.close();
  if (state.apiServer) state.apiServer.close();
  process.exit(0);
}
function parseArgs() {
  var argv = process.argv.slice(2), out = {};
  for (var i = 0; i < argv.length; i++) {
    switch (argv[i]) {
      case "--callback-ip":
        out.callbackIp = argv[++i];
        break;
      case "--input-device":
        out.inputDevice = argv[++i];
        break;
      default:
        break;
    }
  }
  return out;
}
function detectLanIp() {
  var ifaces = os.networkInterfaces(), names = Object.keys(ifaces);
  for (var i = 0; i < names.length; i++) {
    var list = ifaces[names[i]];
    for (var j = 0; j < list.length; j++) {
      var iface = list[j];
      if (iface.family === "IPv4" && !iface.internal) return iface.address;
    }
  }
  throw new Error("Cannot detect LAN IP");
}
function delay(ms) {
  return new Promise(function(r) {
    setTimeout(r, ms);
  });
}
function pad6(n) {
  var s = String(n);
  while (s.length < 6) s = "0" + s;
  return s;
}
async function main() {
  openPorts();
  state.server = startListener(DEFAULT_LISTEN_PORT, onGenaNotify);
  state.wss = new WebSocketServer({ port: DEFAULT_WS_PORT });
  state.wss.on("listening", function() {
    console.log("[ws]   overlay server on :" + DEFAULT_WS_PORT);
  });
  state.wss.on("error", function(e) {
    console.error("[ws]   server error:", e.message);
  });
  startApiServer();
  await delay(150);
  var config = readConfig();
  if (config) {
    state.config = config;
    console.log("[main] config found \u2014 connecting to Sonos at", config.sonosIp);
    await connectToSonos(config);
  } else {
    console.log("[main] no config \u2014 waiting for setup via the Setup app.");
  }
  startPeriodicSync();
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
main().catch(function(err) {
  console.error("[fatal]", err.message);
  process.exit(1);
});
