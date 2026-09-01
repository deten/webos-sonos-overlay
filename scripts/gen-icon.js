#!/usr/bin/env node
'use strict';
var fs   = require('fs');
var path = require('path');
var zlib = require('zlib');

var W = 256, H = 256;
// RGBA buffer
var pixels = Buffer.alloc(W * H * 4, 0);

// ── Pixel helpers ─────────────────────────────────────────────────────────
function getPixel(x, y) {
  var i = (y * W + x) * 4;
  return [pixels[i], pixels[i+1], pixels[i+2], pixels[i+3]];
}
function blend(x, y, r, g, b, a) {
  if (x < 0 || x >= W || y < 0 || y >= H) return;
  var i   = (y * W + x) * 4;
  var sa  = a / 255;
  var da  = pixels[i+3] / 255;
  var oa  = sa + da * (1 - sa);
  if (oa < 0.001) { pixels[i+3] = 0; return; }
  pixels[i]   = Math.round((r * sa + pixels[i]   * da * (1 - sa)) / oa);
  pixels[i+1] = Math.round((g * sa + pixels[i+1] * da * (1 - sa)) / oa);
  pixels[i+2] = Math.round((b * sa + pixels[i+2] * da * (1 - sa)) / oa);
  pixels[i+3] = Math.round(oa * 255);
}
function setOpaque(x, y, r, g, b) {
  if (x < 0 || x >= W || y < 0 || y >= H) return;
  var i = (y * W + x) * 4;
  pixels[i] = r; pixels[i+1] = g; pixels[i+2] = b; pixels[i+3] = 255;
}

// ── Background: dark radial gradient ─────────────────────────────────────
for (var py = 0; py < H; py++) {
  for (var px = 0; px < W; px++) {
    var dx = (px - W/2) / (W/2), dy = (py - H/2) / (H/2);
    var d  = Math.min(1, Math.sqrt(dx*dx + dy*dy));
    // Centre: #141628  Edge: #080810
    var r  = Math.round(20  * (1-d) + 8  * d);
    var g  = Math.round(22  * (1-d) + 8  * d);
    var b  = Math.round(40  * (1-d) + 12 * d);
    setOpaque(px, py, r, g, b);
  }
}

// ── Anti-aliased line/arc primitives ──────────────────────────────────────
function drawArc(cx, cy, radius, thick, startDeg, endDeg, r, g, b, globalAlpha) {
  globalAlpha = (globalAlpha === undefined) ? 1 : globalAlpha;
  var startRad = startDeg * Math.PI / 180;
  var endRad   = endDeg   * Math.PI / 180;
  var innerR   = radius - thick / 2;
  var outerR   = radius + thick / 2;
  var pad      = Math.ceil(thick / 2) + 2;
  for (var iy = Math.floor(cy - outerR - pad); iy <= Math.ceil(cy + outerR + pad); iy++) {
    for (var ix = Math.floor(cx - outerR - pad); ix <= Math.ceil(cx + outerR + pad); ix++) {
      var ddx = ix - cx, ddy = iy - cy;
      var dist = Math.sqrt(ddx*ddx + ddy*ddy);
      if (dist < innerR - 1.5 || dist > outerR + 1.5) continue;
      var angle = Math.atan2(ddy, ddx);
      // Wrap angle so it falls in [startRad, endRad]
      while (angle < startRad - 0.001) angle += 2 * Math.PI;
      if (angle > endRad + 0.001) continue;
      var radAlpha = Math.max(0, Math.min(1, Math.min(dist - innerR, outerR - dist) + 0.5));
      if (radAlpha > 0) blend(ix, iy, r, g, b, Math.round(255 * radAlpha * globalAlpha));
    }
  }
}

// ── Speaker shape ─────────────────────────────────────────────────────────
// Speaker = rectangle body + triangular horn pointing right
// Layout centred at (105, 128):
//   Body rect:   x[68..98]  y[108..148]   width=30 height=40
//   Horn:        from x=98, y∈[108,148] → tip at x=120, y=128 ± varies
//     Triangle vertices: (98,108),(98,148),(120,108),(120,148) → actually
//     horn flares outward: left edge y∈[108,148], right edge y∈[100,156]
//   We'll use per-scanline fill

var WR = 238, WG = 238, WB = 255;  // near-white speaker color

// Body rectangle (68..98, 108..148) with AA edges
for (var iy = 107; iy <= 149; iy++) {
  for (var ix = 67; ix <= 99; ix++) {
    // AA on all edges
    var edge = Math.min(ix-67, 99-ix, iy-107, 149-iy);
    var aa = Math.max(0, Math.min(1, edge + 0.5));
    if (aa > 0) blend(ix, iy, WR, WG, WB, Math.round(255 * aa));
  }
}

// Horn / cone: trapezoid from (98,108..148) widening to (120, 96..160)
// Left edge of horn at x=98 (y: 108..148, span=40)
// Right edge at x=120, y-span flares to 64 (96..160), centred on 128
// For each x from 98..120, compute y range by linear interpolation
for (var ix = 98; ix <= 121; ix++) {
  var t    = (ix - 98) / (120 - 98);          // 0..1
  var span = 40 + (64 - 40) * t;              // 40..64
  var yTop = 128 - span / 2;
  var yBot = 128 + span / 2;
  for (var iy = Math.floor(yTop - 1); iy <= Math.ceil(yBot + 1); iy++) {
    var aa = Math.max(0, Math.min(1, Math.min(iy - yTop, yBot - iy) + 0.5));
    if (aa > 0) blend(ix, iy, WR, WG, WB, Math.round(255 * aa));
  }
}

// ── Sound waves (orange arcs, right of horn tip at ~122,128) ─────────────
var OR = 232, OG = 164, OB = 48;   // #E8A430
var wCX = 118, wCY = 128;

// Three arcs: small → large, fanning out ±55°→±65°
drawArc(wCX, wCY, 24, 7,  -52,  52, OR, OG, OB);
drawArc(wCX, wCY, 40, 7,  -58,  58, OR, OG, OB);
drawArc(wCX, wCY, 58, 7,  -64,  64, OR, OG, OB);

// ── Rounded-rect border (subtle orange glow) ─────────────────────────────
var MARGIN = 10, CORNER = 22, BTHICK = 3;
function drawRRBorder(x0, y0, x1, y1, rr, thick, r, g, b, alpha) {
  // edges
  for (var bx = x0+rr; bx <= x1-rr; bx++) {
    for (var t = 0; t < thick+2; t++) {
      var aa = Math.max(0, Math.min(1, thick - t + 0.5));
      blend(bx, y0+t, r, g, b, Math.round(alpha * aa));
      blend(bx, y1-t, r, g, b, Math.round(alpha * aa));
    }
  }
  for (var by = y0+rr; by <= y1-rr; by++) {
    for (var t = 0; t < thick+2; t++) {
      var aa = Math.max(0, Math.min(1, thick - t + 0.5));
      blend(x0+t, by, r, g, b, Math.round(alpha * aa));
      blend(x1-t, by, r, g, b, Math.round(alpha * aa));
    }
  }
  // corners
  var corners = [[x0+rr,y0+rr],[x1-rr,y0+rr],[x0+rr,y1-rr],[x1-rr,y1-rr]];
  corners.forEach(function(c) {
    for (var cy2 = c[1]-rr-3; cy2 <= c[1]+rr+3; cy2++) {
      for (var cx2 = c[0]-rr-3; cx2 <= c[0]+rr+3; cx2++) {
        var dd  = Math.sqrt((cx2-c[0])*(cx2-c[0])+(cy2-c[1])*(cy2-c[1]));
        var aa  = Math.max(0, Math.min(1, thick/2 - Math.abs(dd - rr) + 0.5));
        if (aa > 0) blend(cx2, cy2, r, g, b, Math.round(alpha * aa));
      }
    }
  });
}
drawRRBorder(MARGIN, MARGIN, W-MARGIN-1, H-MARGIN-1, CORNER, BTHICK, OR, OG, OB, 200);

// ── PNG encoder ───────────────────────────────────────────────────────────
function crc32(buf) {
  if (!crc32.t) {
    crc32.t = new Uint32Array(256);
    for (var i = 0; i < 256; i++) {
      var c = i;
      for (var j = 0; j < 8; j++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
      crc32.t[i] = c;
    }
  }
  var c = 0xFFFFFFFF;
  for (var i = 0; i < buf.length; i++) c = crc32.t[(c ^ buf[i]) & 0xFF] ^ (c >>> 8);
  return (c ^ 0xFFFFFFFF) >>> 0;
}
function u32be(n) { var b = Buffer.alloc(4); b.writeUInt32BE(n >>> 0); return b; }
function chunk(type, data) {
  var t = Buffer.from(type, 'ascii');
  return Buffer.concat([u32be(data.length), t, data, u32be(crc32(Buffer.concat([t, data])))]);
}
function pngEncode(w, h, rgba) {
  var raw = [];
  for (var row = 0; row < h; row++) {
    raw.push(0);
    for (var col = 0; col < w; col++) {
      var idx = (row*w+col)*4;
      raw.push(rgba[idx], rgba[idx+1], rgba[idx+2]);
    }
  }
  return Buffer.concat([
    Buffer.from([137,80,78,71,13,10,26,10]),
    chunk('IHDR', Buffer.concat([u32be(w),u32be(h),Buffer.from([8,2,0,0,0])])),
    chunk('IDAT', zlib.deflateSync(Buffer.from(raw))),
    chunk('IEND', Buffer.alloc(0))
  ]);
}

var outPath = path.join(__dirname, '..', 'setup', 'icon.png');
fs.writeFileSync(outPath, pngEncode(W, H, pixels));
console.log('Icon written:', outPath);
