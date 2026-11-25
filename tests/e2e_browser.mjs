#!/usr/bin/env node
/**
 * Independent Real-Browser End-to-End Test Suite for Tacx Flux S (`flux-s.html`).
 *
 * Zero npm dependencies:
 *  - Spins up a real localhost HTTP + RFC 6455 WebSocket v8 bridge server
 *    speaking the exact `flux_bridge.py` v8 protocol (serving `flux-s.html` +
 *    config/workouts/rides/BLE simulation).
 *  - Launches real headless Google Chrome via CDP (Chrome DevTools Protocol).
 *  - Verifies complete feature coverage end-to-end:
 *      1. Page load, zero runtime errors, Bridge v8 detection & Stop bridge UI
 *      2. Live BLE scan, RSSI sort, Trainers & HR filter, + Add
 *      3. FTMS connect, 0x2AD2 telemetry + trainer HR fallback + 📶 connQ pill,
 *         0x05 Control Not Permitted recovery, ERG / Slope / Resistance (uint8
 *         & sint16 ranges), keyboard shortcuts
 *      4. Simultaneous HR strap (0x2A37 8-bit, 16-bit, contact-not-detected,
 *         battery pill, zones Z1-Z5, drop auto-reconnect) + ❤ HR Control Mode
 *      5. Listen-Only Mode (blocks trainer writes + Take control) & Single-Tab
 *         takenOver banner ("Use this tab instead")
 *      6. FE-C over BLE (Auto fallback, pages 0x10/0x19/0x30/0x31/0x37-0x32-0x33
 *         with XOR checksums, Rider weight sync re-sending page 0x37)
 *      7. 42-Workout Training Library (filters, SVG profiles, favorites, load)
 *      8. FTP Tests modal (Ramp, 20-min, 2x8-min, CP) & starting Ramp test
 *      9. Workout Builder, ERG % FTP toggle, live mid-workout edit, Ride View,
 *         Player controls (Start/Pause/Back/Skip/Stop) & Disk Library CRUD
 *     10. Trainer Spin-Down Calibration modal (Demo mode flow + history)
 *     11. Settings Modal (validation, save -> configSave) & Keys Modal
 *     12. Session Recording, End Ride, Rides History Modal, CSV Export &
 *         Binary FIT Export (verifying .FIT header + header CRC + file CRC-16!)
 *     13. Excel (.xlsx) Export + Deflated/SharedStrings/Reordered Import,
 *         Tab CSV Import, JSON Import (valid & invalid), Old .xls Rejection
 *     14. Stop Bridge flow (pause workout, FTMS Stop 0x08, shutdown) & 3 s
 *         restart polling + auto-reconnect
 *     15. Web Bluetooth fallback & No-Bluetooth warning banner
 */
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const HTML_PATH = path.join(ROOT, 'flux-s.html');

const FTMS_SVC = '00001826-0000-1000-8000-00805f9b34fb';
const FTMS_BIKE = '00002ad2-0000-1000-8000-00805f9b34fb';
const FTMS_RES_RNG = '00002ad6-0000-1000-8000-00805f9b34fb';
const FTMS_PWR_RNG = '00002ad8-0000-1000-8000-00805f9b34fb';
const FTMS_CP = '00002ad9-0000-1000-8000-00805f9b34fb';
const FTMS_STATUS = '00002ada-0000-1000-8000-00805f9b34fb';
const FEC_SVC = '6e40fec1-b5a3-f393-e0a9-e50e24dcca9e';
const FEC_RX = '6e40fec2-b5a3-f393-e0a9-e50e24dcca9e';
const FEC_TX = '6e40fec3-b5a3-f393-e0a9-e50e24dcca9e';
const HR_SVC = '0000180d-0000-1000-8000-00805f9b34fb';
const HR_MEAS = '00002a37-0000-1000-8000-00805f9b34fb';
const BATT_SVC = '0000180f-0000-1000-8000-00805f9b34fb';
const BATT_LVL = '00002a19-0000-1000-8000-00805f9b34fb';

const TRAINER_FTMS_ADDR = 'FA:C5:9A:FD:A5:45';
const TRAINER_FEC_ADDR = 'FE:C0:11:22:33:44';
const HR_ADDR = 'CC:B2:7A:17:E6:AF';
const SPEAKER_ADDR = '99:88:77:66:55:44';

function findChromeBinary() {
  const candidates = [
    process.env.CHROME_PATH,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/Applications/Chromium.app/Contents/MacOS/Chromium',
    '/usr/bin/google-chrome',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
  ].filter(Boolean);
  for (const p of candidates) if (fs.existsSync(p)) return p;
  throw new Error('Could not find Chrome/Chromium binary. Set CHROME_PATH env var.');
}

function encodeWsText(str) {
  const payload = Buffer.from(str, 'utf8');
  let header;
  if (payload.length < 126) header = Buffer.from([0x81, payload.length]);
  else if (payload.length < 65536) { header = Buffer.alloc(4); header[0] = 0x81; header[1] = 126; header.writeUInt16BE(payload.length, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(payload.length), 2); }
  return Buffer.concat([header, payload]);
}

function attachWsDecoder(socket, onMessage, onClose) {
  let buf = Buffer.alloc(0);
  socket.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const opcode = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f;
      let off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      const need = off + (masked ? 4 : 0) + len;
      if (buf.length < need) return;
      let payload = buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) {
        const mask = buf.subarray(off, off + 4);
        const un = Buffer.alloc(len);
        for (let i = 0; i < len; i++) un[i] = payload[i] ^ mask[i % 4];
        payload = un;
      }
      buf = buf.subarray(need);
      if (opcode === 0x8) { socket.end(); return; }
      if (opcode === 0x9) { socket.write(Buffer.from([0x8a, 0x00])); continue; }
      if (opcode === 0x1) onMessage(payload.toString('utf8'));
    }
  });
  socket.on('close', onClose);
  socket.on('error', () => {});
}

class MockBridgeV8Server {
  constructor() {
    this.server = null;
    this.port = 0;
    this.wsSockets = new Set();
    this.enableBridge = true;
    this.connectedAddrs = new Set();
    this.writes = [];
    this.ops = [];
    this.config = null;
    this.workouts = new Map();
    this.rides = new Map(); // id -> { header, lines[], meta }
    this.ftmsRejectNextControlNotPermitted = false;
    this.ftmsResRangeHex = '000064000100'; // 0..100 (fits uint8)
    this.ftmsPwrRangeHex = '0000dc050100'; // 0..1500 W
    this.hrBattery = 87;
  }

  async start(preferredPort = 0) {
    this.server = http.createServer((req, res) => {
      const urlPath = (req.url || '/').split('?')[0];
      if (urlPath === '/' || urlPath === '/flux-s.html') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
        res.end(fs.readFileSync(HTML_PATH));
        return;
      }
      if (urlPath === '/ping') {
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ app: 'flux-bridge', page: '/flux-s.html' }));
        return;
      }
      res.writeHead(404);
      res.end('Not found');
    });

    this.server.on('upgrade', (req, socket) => {
      if (!this.enableBridge) {
        socket.write('HTTP/1.1 503 Service Unavailable\r\n\r\n');
        socket.destroy();
        return;
      }
      const key = req.headers['sec-websocket-key'];
      const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
      socket.write(
        'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
      );
      this.wsSockets.add(socket);

      const send = (obj) => { if (!socket.destroyed) socket.write(encodeWsText(JSON.stringify(obj))); };

      attachWsDecoder(socket, (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }
        this.ops.push(msg);
        const { id, op } = msg;

        if (op === 'hello') {
          send({ id, ok: true, version: 8, active: true, busy: false });
        } else if (op === 'claim') {
          send({ id, ok: true, active: true });
        } else if (op === 'configLoad') {
          send({ id, ok: true, exists: this.config !== null, config: this.config || {}, file: '/tmp/flux_config.json' });
        } else if (op === 'configSave') {
          this.config = msg.config;
          send({ id, ok: true, file: '/tmp/flux_config.json' });
        } else if (op === 'workoutList') {
          send({ id, ok: true, folder: '/tmp/Workouts', workouts: [...this.workouts.keys()].sort() });
        } else if (op === 'workoutSave') {
          this.workouts.set(msg.workout.name, msg.workout);
          send({ id, ok: true, file: `/tmp/Workouts/${msg.workout.name}.json` });
        } else if (op === 'workoutLoad') {
          const w = this.workouts.get(msg.name);
          if (w) send({ id, ok: true, workout: w });
          else send({ id, ok: false, error: 'not found' });
        } else if (op === 'workoutDelete') {
          this.workouts.delete(msg.name);
          send({ id, ok: true });
        } else if (op === 'rideAppend') {
          const rid = msg.ride;
          let entry = this.rides.get(rid);
          if (!entry || msg.replace) {
            entry = { header: msg.header || 'time_s,power_w,cadence_rpm,speed_kmh,heart_rate_bpm,distance_m,mode,target,workout_step', lines: [], meta: {} };
            this.rides.set(rid, entry);
          }
          if (Array.isArray(msg.lines)) entry.lines.push(...msg.lines);
          if (msg.meta) Object.assign(entry.meta, msg.meta);
          send({ id, ok: true, file: `/tmp/Rides/${rid}.csv` });
        } else if (op === 'rideList') {
          const rides = [...this.rides.entries()].reverse().map(([rid, r]) => {
            const powers = r.lines.map(l => Number(l.split(',')[1])).filter(n => Number.isFinite(n));
            const avgP = powers.length ? Math.round(powers.reduce((a, b) => a + b, 0) / powers.length) : 0;
            const maxP = powers.length ? Math.max(...powers) : 0;
            return { id: rid, meta: r.meta, seconds: r.lines.length, avgPower: avgP, maxPower: maxP, np: avgP, kj: Math.round(powers.reduce((a, b) => a + b, 0) / 1000), km: 0.5 };
          });
          send({ id, ok: true, folder: '/tmp/Rides', rides });
        } else if (op === 'rideLoad') {
          const r = this.rides.get(msg.ride);
          if (r) send({ id, ok: true, csv: [r.header, ...r.lines].join('\n') + '\n', meta: r.meta, file: `/tmp/Rides/${msg.ride}.csv` });
          else send({ id, ok: false, error: 'ride not found' });
        } else if (op === 'clientLog') {
          send({ id, ok: true });
        } else if (op === 'scanStart') {
          send({ id, ok: true });
          setTimeout(() => {
            send({ ev: 'adv', device: { address: TRAINER_FTMS_ADDR, name: 'Tacx Flux 46956', rssi: -58, ftms: true, fec: true, hr: false } });
            send({ ev: 'adv', device: { address: TRAINER_FEC_ADDR, name: 'Tacx Legacy FEC', rssi: -68, ftms: false, fec: true, hr: false } });
            send({ ev: 'adv', device: { address: HR_ADDR, name: 'Polar H10', rssi: -54, ftms: false, fec: false, hr: true } });
            send({ ev: 'adv', device: { address: SPEAKER_ADDR, name: 'Kitchen Speaker', rssi: -45, ftms: false, fec: false, hr: false } });
          }, 30);
        } else if (op === 'scanStop') {
          send({ id, ok: true });
        } else if (op === 'connect') {
          const addr = msg.address;
          send({ ev: 'progress', address: addr, text: 'connecting… (attempt 1/3)' });
          this.connectedAddrs.add(addr);
          let name = addr, services = [];
          if (addr === TRAINER_FTMS_ADDR) {
            name = 'Tacx Flux 46956';
            services = [
              {
                uuid: FTMS_SVC,
                chars: [
                  { uuid: FTMS_BIKE, props: ['notify'] },
                  { uuid: FTMS_RES_RNG, props: ['read'] },
                  { uuid: FTMS_PWR_RNG, props: ['read'] },
                  { uuid: FTMS_CP, props: ['write', 'indicate'] },
                  { uuid: FTMS_STATUS, props: ['notify'] },
                ],
              },
              { uuid: FEC_SVC, chars: [{ uuid: FEC_RX, props: ['notify'] }, { uuid: FEC_TX, props: ['write-without-response', 'write'] }] },
            ];
          } else if (addr === TRAINER_FEC_ADDR) {
            name = 'Tacx Legacy FEC';
            services = [{ uuid: FEC_SVC, chars: [{ uuid: FEC_RX, props: ['notify'] }, { uuid: FEC_TX, props: ['write-without-response', 'write'] }] }];
          } else if (addr === HR_ADDR) {
            name = 'Polar H10';
            services = [
              { uuid: HR_SVC, chars: [{ uuid: HR_MEAS, props: ['notify'] }] },
              { uuid: BATT_SVC, chars: [{ uuid: BATT_LVL, props: ['read'] }] },
            ];
          }
          send({ id, ok: true, name, services });
        } else if (op === 'disconnect') {
          if (msg.address) this.connectedAddrs.delete(msg.address);
          else this.connectedAddrs.clear();
          send({ id, ok: true });
        } else if (op === 'read') {
          const u = (msg.uuid || '').toLowerCase();
          let data = '00';
          if (u === FTMS_RES_RNG) data = this.ftmsResRangeHex;
          else if (u === FTMS_PWR_RNG) data = this.ftmsPwrRangeHex;
          else if (u === BATT_LVL) data = this.hrBattery.toString(16).padStart(2, '0');
          send({ id, ok: true, data });
        } else if (op === 'subscribe') {
          send({ id, ok: true });
        } else if (op === 'write') {
          const bytes = Buffer.from(msg.data || '', 'hex');
          this.writes.push({ address: msg.address, uuid: (msg.uuid || '').toLowerCase(), hex: msg.data, bytes, response: msg.response });
          send({ id, ok: true });
          if ((msg.uuid || '').toLowerCase() === FTMS_CP && bytes.length >= 1) {
            const reqOp = bytes[0];
            let resCode = 0x01;
            if (this.ftmsRejectNextControlNotPermitted && reqOp !== 0x00) {
              this.ftmsRejectNextControlNotPermitted = false;
              resCode = 0x05;
            }
            setTimeout(() => {
              send({ ev: 'notify', address: msg.address, uuid: FTMS_CP, data: Buffer.from([0x80, reqOp, resCode]).toString('hex') });
            }, 10);
          }
        } else if (op === 'shutdown') {
          send({ id, ok: true });
          this.enableBridge = false;
          setTimeout(() => {
            for (const s of Array.from(this.wsSockets)) { try { s.end(); } catch {} }
            this.wsSockets.clear();
            this.connectedAddrs.clear();
          }, 40);
        }
      }, () => { this.wsSockets.delete(socket); });
    });

    await new Promise((resolve) => this.server.listen(preferredPort, '127.0.0.1', resolve));
    this.port = this.server.address().port;
  }

  broadcast(obj) {
    const frame = encodeWsText(JSON.stringify(obj));
    for (const s of this.wsSockets) if (!s.destroyed) s.write(frame);
  }
  pushNotify(address, uuid, bytes) {
    this.broadcast({ ev: 'notify', address, uuid, data: Buffer.from(bytes).toString('hex') });
  }
  pushDisconnect(address) {
    this.connectedAddrs.delete(address);
    this.broadcast({ ev: 'disconnected', address });
  }
  async stop() {
    for (const s of Array.from(this.wsSockets)) { try { s.destroy(); } catch {} }
    this.wsSockets.clear();
    if (this.server) {
      if (this.server.closeAllConnections) this.server.closeAllConnections();
      await new Promise((resolve) => this.server.close(resolve));
      this.server = null;
    }
  }
}

class CDPBrowser {
  constructor() {
    this.proc = null; this.ws = null; this.userDataDir = null;
    this.nextId = 1; this.pending = new Map();
    this.pageErrors = []; this.dialogs = [];
  }

  async launch(downloadDir) {
    const bin = findChromeBinary();
    this.userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flux-cdp-'));
    this.proc = spawn(bin, [
      '--headless=new', '--disable-gpu', '--no-sandbox', '--disable-dev-shm-usage',
      '--remote-debugging-port=0', `--user-data-dir=${this.userDataDir}`, '--window-size=1440,950', 'about:blank',
    ], { stdio: ['ignore', 'pipe', 'pipe'] });

    const wsUrl = await new Promise((resolve, reject) => {
      let stderr = '';
      const timer = setTimeout(() => reject(new Error('Chrome did not start CDP:\n' + stderr)), 15000);
      this.proc.stderr.on('data', (d) => {
        stderr += d.toString();
        const m = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/);
        if (m) { clearTimeout(timer); resolve(m[1]); }
      });
      this.proc.on('exit', (code) => { clearTimeout(timer); reject(new Error(`Chrome exited (${code}):\n${stderr}`)); });
    });

    const dbgBase = wsUrl.replace(/^ws:/, 'http:').replace(/\/devtools\/.*$/, '');
    const targets = await (await fetch(`${dbgBase}/json/list`)).json();
    const pageTarget = targets.find((t) => t.type === 'page');
    this.ws = new WebSocket(pageTarget.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => { this.ws.addEventListener('open', resolve); this.ws.addEventListener('error', reject); });

    this.ws.addEventListener('message', (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.id && this.pending.has(msg.id)) {
        const p = this.pending.get(msg.id); this.pending.delete(msg.id);
        if (msg.error) p.reject(new Error(msg.error.message)); else p.resolve(msg.result);
        return;
      }
      if (msg.method === 'Runtime.exceptionThrown') {
        this.pageErrors.push(msg.params.exceptionDetails.exception?.description || msg.params.exceptionDetails.text);
      } else if (msg.method === 'Page.javascriptDialogOpening') {
        this.dialogs.push(msg.params.message);
        this.send('Page.handleJavaScriptDialog', { accept: true }).catch(() => {});
      }
    });

    await this.send('Page.enable');
    await this.send('Runtime.enable');
    await this.send('DOM.enable');
    await this.send('Browser.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadDir });
  }

  send(method, params = {}) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`CDP ${method} timed out`)); }, 15000);
      this.pending.set(id, {
        resolve: (r) => { clearTimeout(timer); resolve(r); },
        reject: (e) => { clearTimeout(timer); reject(e); },
      });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async goto(url) {
    await this.send('Page.navigate', { url });
    await this.waitFor(() => document.readyState === 'complete', 10000);
  }

  async eval(fnOrExpr) {
    const expr = typeof fnOrExpr === 'function' ? `(${fnOrExpr.toString()})()` : fnOrExpr;
    const res = await this.send('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true });
    if (res.exceptionDetails) throw new Error('Page eval error: ' + (res.exceptionDetails.exception?.description || res.exceptionDetails.text));
    return res.result.value;
  }

  async waitFor(fn, timeoutMs = 8000, label = 'condition') {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const ok = await this.eval(fn);
      if (ok) return ok;
      await sleep(50);
    }
    throw new Error(`Timed out waiting for: ${label}`);
  }

  async uploadFile(selector, filePath) {
    const doc = await this.send('DOM.getDocument', { depth: -1 });
    const q = await this.send('DOM.querySelector', { nodeId: doc.root.nodeId, selector });
    assert.ok(q.nodeId, `Element ${selector} not found`);
    await this.send('DOM.setFileInputFiles', { nodeId: q.nodeId, files: [filePath] });
  }

  async close() {
    try { this.ws?.close(); } catch {}
    if (this.proc) { this.proc.kill('SIGKILL'); await new Promise((r) => this.proc.on('exit', r)); }
    if (this.userDataDir) fs.rmSync(this.userDataDir, { recursive: true, force: true });
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitUntil(fn, timeoutMs = 5000, label = 'condition') {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return true;
    await sleep(40);
  }
  throw new Error(`Timed out waiting for: ${label}`);
}

function buildFtmsBikeFrame({ speedKmh = 32.5, cadenceRpm = 90, resistance = 40, powerW = 240, hrBpm = null }) {
  let flags = 0x0004 | 0x0020 | 0x0040 | 0x0080 | 0x0100;
  if (hrBpm !== null) flags |= 0x0200;
  const buf = Buffer.alloc(2 + 2 + 2 + 2 + 2 + 2 + 5 + (hrBpm !== null ? 1 : 0));
  let o = 0;
  buf.writeUInt16LE(flags, o); o += 2;
  buf.writeUInt16LE(Math.round(speedKmh * 100), o); o += 2;
  buf.writeUInt16LE(Math.round(cadenceRpm * 2), o); o += 2;
  buf.writeInt16LE(resistance, o); o += 2;
  buf.writeInt16LE(powerW, o); o += 2;
  buf.writeInt16LE(powerW - 10, o); o += 2;
  o += 5;
  if (hrBpm !== null) buf.writeUInt8(hrBpm, o);
  return buf;
}

function wrapAntPage(page8) {
  const pkt = Buffer.alloc(13);
  pkt[0] = 0xa4; pkt[1] = 0x09; pkt[2] = 0x4f; pkt[3] = 0x05;
  for (let i = 0; i < 8; i++) pkt[4 + i] = page8[i] & 0xff;
  let xor = 0;
  for (let i = 0; i < 12; i++) xor ^= pkt[i];
  pkt[12] = xor;
  return pkt;
}

function assertValidAntChecksum(buf) {
  assert.equal(buf.length, 13);
  assert.equal(buf[0], 0xa4);
  let xor = 0;
  for (let i = 0; i < 12; i++) xor ^= buf[i];
  assert.equal(buf[12], xor, 'ANT XOR checksum mismatch');
}

// Independent FIT CRC-16 validator (Garmin FIT SDK polynomial table)
function fitCrc16(buf) {
  const tbl = [0x0000, 0xcc01, 0xd801, 0x1400, 0xf001, 0x3c00, 0x2800, 0xe401,
               0xa001, 0x6c00, 0x7800, 0xb401, 0x5000, 0x9c01, 0x8801, 0x4400];
  let crc = 0;
  for (const b of buf) {
    let tmp = tbl[crc & 0xf]; crc = (crc >> 4) & 0x0fff; crc ^= tmp ^ tbl[b & 0xf];
    tmp = tbl[crc & 0xf]; crc = (crc >> 4) & 0x0fff; crc ^= tmp ^ tbl[(b >> 4) & 0xf];
  }
  return crc;
}

async function run() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flux-e2e-'));
  const bridge = new MockBridgeV8Server();
  const browser = new CDPBrowser();
  const results = [];

  async function test(name, fn) {
    process.stdout.write(`  [RUN ] ${name} ... `);
    const t0 = Date.now();
    await fn();
    const dt = Date.now() - t0;
    results.push({ name, dt });
    console.log(`PASS (${dt} ms)`);
  }

  try {
    await bridge.start(0);
    await browser.launch(tmpDir);
    const baseUrl = `http://127.0.0.1:${bridge.port}/flux-s.html`;

    // 1. Page Load & Bridge v8 Detection
    await test('1. Page load, zero errors & Bridge v8 detection', async () => {
      await browser.goto(baseUrl);
      await browser.waitFor(() => document.getElementById('bleVia').textContent.includes('Bridge'), 5000, 'Bridge pill');
      assert.equal(await browser.eval(() => getComputedStyle(document.getElementById('btnBridgeStop')).display !== 'none'), true);
      assert.equal(browser.pageErrors.length, 0);
    });

    // 2. Live BLE Scan, RSSI Sort, Trainers & HR Filter, + Add
    await test('2. Bridge live scan, RSSI sort, Trainers & HR filter & + Add', async () => {
      await browser.eval(() => document.getElementById('btnScan').click());
      await browser.waitFor(() => document.querySelectorAll('#scanList .dev').length === 3, 4000, '3 filtered scan devices');
      const names = await browser.eval(() => [...document.querySelectorAll('#scanList .nm')].map(el => el.textContent.trim()));
      assert.ok(names[0].includes('Polar H10') && names[1].includes('Tacx Flux 46956') && names[2].includes('Tacx Legacy FEC'));

      await browser.eval(() => { const cb = document.getElementById('trainersOnly'); cb.checked = false; cb.dispatchEvent(new Event('change')); });
      await browser.waitFor(() => document.querySelectorAll('#scanList .dev').length === 4, 2000, '4 unfiltered scan devices');
      await browser.eval(() => { const cb = document.getElementById('trainersOnly'); cb.checked = true; cb.dispatchEvent(new Event('change')); });

      // Click + Add on Tacx Legacy FEC
      await browser.eval(() => {
        const items = [...document.querySelectorAll('#scanList .dev')];
        const fec = items.find(el => el.textContent.includes('Tacx Legacy FEC'));
        [...fec.querySelectorAll('button')].find(b => b.textContent.includes('Add')).click();
      });
      await browser.waitFor(() => document.getElementById('savedList').textContent.includes('Tacx Legacy FEC'), 2000, 'Added FEC trainer');
    });

    // 3. FTMS Connect, Telemetry, Connection Quality Pill, 0x05 Recovery, ERG/Slope/Res (uint8 & sint16) & Keys
    await test('3. FTMS connect, 0x2AD2 telemetry, connQ pill, 0x05 recovery, uint8/sint16 Res & keys', async () => {
      bridge.writes.length = 0;
      await browser.eval(() => {
        const items = [...document.querySelectorAll('#scanList .dev')];
        const ftms = items.find(el => el.textContent.includes('Tacx Flux 46956'));
        [...ftms.querySelectorAll('button')].find(b => b.textContent.trim() === 'Connect').click();
      });
      await browser.waitFor(() => document.getElementById('status').textContent === 'Connected' && document.getElementById('devinfo').textContent.includes('FTMS'), 5000, 'FTMS connected');

      const cpOps = bridge.writes.filter(w => w.uuid === FTMS_CP).map(w => w.bytes[0]);
      assert.ok(cpOps.includes(0x00) && cpOps.includes(0x07), 'Must send FTMS 0x00 and 0x07');

      // Default FTP is 200 W -> 190 W = 95% FTP (Z4 Threshold), 75 kg -> 2.5 W/kg, trainer HR = 144
      bridge.pushNotify(TRAINER_FTMS_ADDR, FTMS_BIKE, buildFtmsBikeFrame({ speedKmh: 33.4, cadenceRpm: 91, resistance: 40, powerW: 190, hrBpm: 144 }));
      await browser.waitFor(() => document.getElementById('vPower').textContent === '190', 3000, 'Power 190 W');
      await browser.waitFor(() => getComputedStyle(document.getElementById('connQ')).display !== 'none', 2500, 'Connection quality 📶 pill visible');
      const snap = await browser.eval(() => ({
        pct: document.getElementById('vFtpPct').textContent,
        wkg: document.getElementById('vWkg').textContent,
        zone: document.getElementById('vZone').textContent,
        cad: document.getElementById('vCad').textContent,
        spd: document.getElementById('vSpd').textContent,
        hr: document.getElementById('vHr').textContent,
        connQText: document.getElementById('connQ').textContent,
      }));
      assert.equal(snap.pct, '95');
      assert.equal(snap.wkg, '2.53');
      assert.ok(snap.zone.includes('Z4'));
      assert.equal(snap.cad, '91');
      assert.equal(snap.spd, '33.4');
      assert.equal(snap.hr, '144');
      assert.ok(snap.connQText.includes('/s'), 'Connection quality 📶 pill must show data rate');

      // Test 0x05 Control Not Permitted recovery on Shift+ArrowUp (+25 W -> 175 W)
      bridge.writes.length = 0;
      bridge.ftmsRejectNextControlNotPermitted = true;
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowUp', shiftKey: true, bubbles: true })));
      await browser.waitFor(() => document.getElementById('ack').classList.contains('ok') && document.getElementById('ack').textContent.includes('175'), 3000, 'ERG 175 W ack after retry');
      const retryOps = bridge.writes.filter(w => w.uuid === FTMS_CP).map(w => w.bytes[0]);
      assert.deepEqual(retryOps, [0x05, 0x00, 0x05], 'Expected 0x05 -> 0x00 -> 0x05 retry');

      // Slope mode (key '2') + Resistance uint8 (key '3')
      bridge.writes.length = 0;
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: '2', bubbles: true })));
      await waitUntil(() => bridge.writes.some(w => w.uuid === FTMS_CP && w.bytes[0] === 0x11), 2500, 'FTMS 0x11 Slope');

      bridge.writes.length = 0;
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: '3', bubbles: true })));
      await waitUntil(() => bridge.writes.some(w => w.uuid === FTMS_CP && w.bytes[0] === 0x04), 2500, 'FTMS 0x04 uint8 Resistance');
      const resU8 = bridge.writes.find(w => w.uuid === FTMS_CP && w.bytes[0] === 0x04);
      assert.equal(resU8.bytes.length, 2, 'Range 0..100 fits in uint8');

      // Reconnect with sint16 resistance range (0..1000) -> 3 bytes [0x04, lo, hi]
      bridge.ftmsResRangeHex = '0000e8030100';
      await browser.eval(() => document.getElementById('btnDisconnect').click());
      await browser.waitFor(() => document.getElementById('status').textContent === 'Disconnected', 3000, 'Disconnected');
      bridge.writes.length = 0;
      await browser.eval(() => {
        const rows = [...document.querySelectorAll('#savedList .dev')];
        const row = rows.find(r => r.textContent.includes('Tacx Flux 46956'));
        [...row.querySelectorAll('button')].find(b => b.textContent.trim() === 'Connect').click();
      });
      await waitUntil(() => bridge.writes.some(w => w.uuid === FTMS_CP && w.bytes[0] === 0x04), 4000, 'FTMS 0x04 sint16 Resistance');
      const resS16 = bridge.writes.find(w => w.uuid === FTMS_CP && w.bytes[0] === 0x04);
      assert.equal(resS16.bytes.length, 3, 'Range 0..1000 must send sint16');
      assert.equal(resS16.bytes.readInt16LE(1), 200, '20% of 0..1000 must be 200');
      bridge.ftmsResRangeHex = '000064000100';

      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: '1', bubbles: true })));
      await sleep(300);
    });

    // 4. Simultaneous HR Strap + Heart-Rate Control Mode (❤ HR)
    await test('4. Simultaneous HR strap (0x2A37 flags, battery, drop reconnect) & ❤ HR Control Mode', async () => {
      await browser.eval(() => document.getElementById('btnHrSearch').click());
      await browser.waitFor(() => document.querySelector('#scanList .dev'), 3000, 'Scan list for HR');
      await browser.eval(() => {
        const items = [...document.querySelectorAll('#scanList .dev')];
        const hr = items.find(el => el.textContent.includes('Polar H10'));
        [...hr.querySelectorAll('button')].find(b => b.textContent.includes('Connect HR')).click();
      });
      await browser.waitFor(() => getComputedStyle(document.getElementById('hrinfo')).display !== 'none', 4000, 'HR header pill');
      assert.ok((await browser.eval(() => document.getElementById('hrinfo').textContent)).includes('87%'));

      // 8-bit 160 bpm, 16-bit 175 bpm, contact-not-detected 199 ignored
      bridge.pushNotify(HR_ADDR, HR_MEAS, [0x06, 160]);
      await browser.waitFor(() => document.getElementById('vHr').textContent === '160', 2500, 'HR 160');
      bridge.pushNotify(HR_ADDR, HR_MEAS, [0x01, 175, 0]);
      await browser.waitFor(() => document.getElementById('vHr').textContent === '175', 2500, 'HR 175 (16-bit)');
      bridge.pushNotify(HR_ADDR, HR_MEAS, [0x04, 199]);
      await sleep(300);
      assert.notEqual(await browser.eval(() => document.getElementById('vHr').textContent), '199');

      // Switch to ❤ HR Control Mode -> verify initial ERG target sent to trainer
      bridge.writes.length = 0;
      bridge.pushNotify(HR_ADDR, HR_MEAS, [0x06, 165]);
      await browser.eval(() => document.querySelector('.tabs button[data-mode="hr"]').click());
      assert.equal(await browser.eval(() => document.getElementById('panel-hr').classList.contains('active')), true);
      await waitUntil(() => bridge.writes.some(w => w.uuid === FTMS_CP && w.bytes[0] === 0x05), 2500, 'HR Control sends initial FTMS 0x05 ERG');
      await browser.eval(() => document.querySelector('.tabs button[data-mode="erg"]').click());

      // Unexpected HR drop -> auto-reconnects
      bridge.pushDisconnect(HR_ADDR);
      await browser.waitFor(() => document.getElementById('hrSub').textContent.toLowerCase().includes('reconnect'), 3000, 'HR reconnecting');
      await browser.waitFor(() => getComputedStyle(document.getElementById('hrinfo')).display !== 'none', 6000, 'HR auto-reconnected');
    });

    // 5. Listen-Only Mode & Single-Tab Takeover Banner
    await test('5. Listen-Only mode (blocks trainer writes) & Single-tab takenOver banner', async () => {
      await browser.eval(() => document.querySelector('#ctlMode button[data-v="listen"]').click());
      assert.equal(await browser.eval(() => getComputedStyle(document.getElementById('listenNote')).display !== 'none'), true);
      bridge.writes.length = 0;
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowUp', bubbles: true })));
      await sleep(400);
      assert.equal(bridge.writes.filter(w => w.uuid === FTMS_CP).length, 0, 'Listen-only mode must not write to trainer');
      await browser.eval(() => document.getElementById('btnTakeControl').click());
      assert.equal(await browser.eval(() => getComputedStyle(document.getElementById('listenNote')).display === 'none'), true);

      // Simulate bridge sending takenOver event -> passive overlay ("Open in another tab") -> clicking "Use this tab instead" reloads & claims!
      bridge.broadcast({ ev: 'takenOver' });
      await browser.waitFor(() => {
        const o = document.getElementById('passiveOverlay');
        return o && o.textContent.toLowerCase().includes('another tab');
      }, 3000, 'takenOver passive overlay');
      bridge.ops.length = 0;
      await browser.eval(() => document.getElementById('passiveUse').click());
      await browser.waitFor(() => !document.getElementById('passiveOverlay') && document.getElementById('bleVia').textContent.includes('Bridge'), 5000, 'Reclaimed after reload');
      assert.ok(bridge.ops.some(o => o.op === 'hello'), 'Reload must re-hello the bridge');
    });

    // 6. FE-C Over BLE (Auto fallback, ANT framing/XOR, Rider weight sync)
    await test('6. FE-C over BLE (Auto fallback, pages 0x10/0x19/0x30/0x31/0x37-0x32-0x33 & weight sync)', async () => {
      // Wait for any post-reload auto-connect to settle, then ensure disconnected
      await browser.waitFor(() => {
        const s = document.getElementById('status').textContent;
        return s === 'Connected' || s === 'Disconnected';
      }, 5000, 'Post-reload settle');
      await browser.eval(() => {
        if (!document.getElementById('btnDisconnect').disabled) document.getElementById('btnDisconnect').click();
        if (!document.getElementById('btnHrDisc').disabled) document.getElementById('btnHrDisc').click();
      });
      await browser.waitFor(() => document.getElementById('status').textContent === 'Disconnected', 3000, 'Disconnected');

      bridge.writes.length = 0;
      await browser.eval(() => {
        const rows = [...document.querySelectorAll('#savedList .dev')];
        const fec = rows.find(r => r.textContent.includes('Tacx Legacy FEC'));
        [...fec.querySelectorAll('button')].find(b => b.textContent.trim() === 'Connect').click();
      });
      await browser.waitFor(() => document.getElementById('status').textContent === 'Connected' && document.getElementById('devinfo').textContent.includes('FE-C'), 5000, 'FE-C connected');

      bridge.pushNotify(TRAINER_FEC_ADDR, FEC_RX, wrapAntPage([0x10, 0x19, 0, 0, 0x10, 0x27, 136, 0]));
      bridge.pushNotify(TRAINER_FEC_ADDR, FEC_RX, wrapAntPage([0x19, 0x01, 88, 0, 0, 0x13, 0x01, 0]));
      await browser.waitFor(() => document.getElementById('vPower').textContent === '275', 3000, 'FE-C power 275 W');
      assert.equal(await browser.eval(() => document.getElementById('vSpd').textContent), '36.0');
      assert.equal(await browser.eval(() => document.getElementById('vCad').textContent), '88');
      assert.equal(await browser.eval(() => document.getElementById('vHr').textContent), '136');

      // ERG -> page 0x31
      bridge.writes.length = 0;
      await browser.eval(() => { const i = document.getElementById('ergInput'); i.value = '200'; i.dispatchEvent(new Event('change')); });
      await waitUntil(() => bridge.writes.some(w => w.uuid === FEC_TX && w.bytes[4] === 0x31), 2500, 'FE-C 0x31');
      assertValidAntChecksum(bridge.writes.find(w => w.uuid === FEC_TX && w.bytes[4] === 0x31).bytes);

      // Resistance -> page 0x30
      bridge.writes.length = 0;
      await browser.eval(() => document.querySelector('.tabs button[data-mode="res"]').click());
      await waitUntil(() => bridge.writes.some(w => w.uuid === FEC_TX && w.bytes[4] === 0x30), 2500, 'FE-C 0x30');
      assertValidAntChecksum(bridge.writes.find(w => w.uuid === FEC_TX && w.bytes[4] === 0x30).bytes);

      // Slope -> 0x37, 0x32, 0x33 + Rider weight sync
      bridge.writes.length = 0;
      await browser.eval(() => document.querySelector('.tabs button[data-mode="sim"]').click());
      await waitUntil(() => bridge.writes.filter(w => w.uuid === FEC_TX).length >= 3, 2500, 'FE-C Slope 3 pages');
      const pages = bridge.writes.filter(w => w.uuid === FEC_TX).slice(0, 3);
      assert.deepEqual(pages.map(p => p.bytes[4]), [0x37, 0x32, 0x33]);
      pages.forEach(p => assertValidAntChecksum(p.bytes));

      bridge.writes.length = 0;
      await browser.eval(() => { const i = document.getElementById('riderKg'); i.value = '70'; i.dispatchEvent(new Event('change')); });
      assert.equal(await browser.eval(() => document.getElementById('simRider').value), '70');
      await waitUntil(() => bridge.writes.some(w => w.uuid === FEC_TX && w.bytes[4] === 0x37), 2500, 'FE-C 0x37 on weight sync');
    });

    // 7. 42-Workout Training Library (filters, SVG profiles, favorites, load)
    await test('7. 42-Workout Training Library (SVG profiles, filters, favorites & load)', async () => {
      await browser.eval(() => document.getElementById('woLibrary').click());
      await browser.waitFor(() => !document.getElementById('libModal').hidden, 2000, 'Library modal open');
      const count = await browser.eval(() => document.querySelectorAll('#libGrid .lib-card').length);
      assert.ok(count >= 40, `Expected ~42 built-in workouts, got ${count}`);
      const svgs = await browser.eval(() => document.querySelectorAll('#libGrid svg').length);
      assert.equal(svgs, count, 'Every workout card must have an SVG profile');

      // Favorite first workout, load first workout into builder
      await browser.eval(() => document.querySelector('#libGrid .lib-star').click());
      await browser.eval(() => {
        const card = document.querySelector('#libGrid .lib-card');
        [...card.querySelectorAll('button')].find(b => b.textContent.includes('Load')).click();
      });
      await browser.waitFor(() => document.getElementById('libModal').hidden, 2000, 'Library modal closed');
    });

    // 8. FTP Tests Modal (Ramp / 20m / 2x8m / CP) & Start Ramp Test in Demo Mode
    await test('8. FTP Tests modal (4 protocols) & starting Ramp test in Demo mode', async () => {
      await browser.eval(() => document.getElementById('btnDisconnect').click());
      await sleep(100);
      await browser.eval(() => document.getElementById('btnDemo').click());
      await browser.waitFor(() => document.getElementById('status').textContent === 'Connected' && document.getElementById('devinfo').textContent.toLowerCase().includes('simulated'), 3000, 'Demo active');

      await browser.eval(() => document.getElementById('woFtpTest').click());
      await browser.waitFor(() => !document.getElementById('ftpModal').hidden, 2000, 'FTP modal open');
      const cards = await browser.eval(() => document.querySelectorAll('#ftpBody .ftp-card').length);
      assert.equal(cards, 4, 'Expected 4 FTP test cards (Ramp, 20-min, 2x8-min, CP)');

      // Start Ramp test (uncheck auto-open ride view first so main UI stays easy to inspect, then test Ride View separately)
      await browser.eval(() => { const c = document.getElementById('rvAuto'); c.checked = false; c.dispatchEvent(new Event('change')); });
      await browser.eval(() => document.querySelector('#ftpBody .ftp-card button').click());
      await browser.waitFor(() => getComputedStyle(document.getElementById('ftpTestLine')).display !== 'none', 3000, 'Live FTP test banner');
      await browser.eval(() => document.getElementById('woStop').click());
      // Stopping an FTP test shows the result modal; close it cleanly
      await browser.waitFor(() => !document.getElementById('ftpModal').hidden, 2000, 'FTP result modal shown');
      await browser.eval(() => document.getElementById('ftpClose').click());
      assert.equal(await browser.eval(() => document.getElementById('ftpModal').hidden), true);
    });

    // 9. Workout Builder, Ride View, Player Controls & Disk Library Save/Load/Delete
    await test('9. Workout builder, Ride View (F/Esc), player controls & bridge workout CRUD', async () => {
      await browser.eval(() => document.getElementById('woNew').click());
      // Verify ERG % FTP toggle converts 150 W <-> 75 % (at 200 W FTP)
      await browser.eval(() => { const p = document.getElementById('woPct'); p.checked = true; p.dispatchEvent(new Event('change')); });
      assert.equal(await browser.eval(() => document.querySelector('#blocks input.starget').value), '75');
      await browser.eval(() => { const p = document.getElementById('woPct'); p.checked = false; p.dispatchEvent(new Event('change')); });
      assert.equal(await browser.eval(() => document.querySelector('#blocks input.starget').value), '150');

      await browser.eval(() => {
        const n = document.getElementById('woName'); n.value = 'E2E Intervals'; n.dispatchEvent(new Event('change'));
        document.getElementById('woSave').click();
      });
      await waitUntil(() => bridge.workouts.has('E2E Intervals'), 2500, 'Workout saved to bridge');

      // Start workout, open & exit Ride View via F / Esc + Numpad0, Pause, and NumpadDecimal double-press Stop
      await browser.eval(() => document.getElementById('woStart').click());
      await browser.waitFor(() => !document.getElementById('woPause').disabled, 2000, 'Workout active');
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'f', bubbles: true })));
      assert.equal(await browser.eval(() => document.getElementById('rideView').hidden), false, 'Ride View must open on F');
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })));
      assert.equal(await browser.eval(() => document.getElementById('rideView').hidden), true, 'Ride View must close on Esc');
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { code: 'Numpad0', key: '0', bubbles: true })));
      assert.equal(await browser.eval(() => document.getElementById('rideView').hidden), false, 'Ride View must open on Numpad0');
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { code: 'Numpad0', key: '0', bubbles: true })));
      assert.equal(await browser.eval(() => document.getElementById('rideView').hidden), true, 'Ride View must close on Numpad0');

      await browser.eval(() => document.getElementById('woPause').click());
      assert.ok((await browser.eval(() => document.getElementById('woPause').textContent)).includes('Resume'));
      await browser.eval(() => document.getElementById('woPause').click());
      // Double-press NumpadDecimal stops workout without confirmation dialog
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { code: 'NumpadDecimal', key: '.', bubbles: true })));
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { code: 'NumpadDecimal', key: '.', bubbles: true })));
      await browser.waitFor(() => document.getElementById('woStop').disabled, 2000, 'Workout stopped via Numpad . .');

      // Delete saved workout from bridge
      await browser.eval(() => { const s = document.getElementById('woLib'); s.value = 'E2E Intervals'; s.dispatchEvent(new Event('change')); });
      await browser.eval(() => document.getElementById('woDelete').click());
      await waitUntil(() => !bridge.workouts.has('E2E Intervals'), 2500, 'Workout deleted from bridge');
    });

    // 10. Trainer Spin-Down Calibration (Demo Mode)
    await test('10. Trainer Spin-Down Calibration modal (Demo mode)', async () => {
      await browser.eval(() => document.getElementById('btnCal').click());
      await browser.waitFor(() => !document.getElementById('calModal').hidden, 2000, 'Calibration modal open');
      await browser.eval(() => document.getElementById('calStart').click());
      await browser.waitFor(() => {
        const t = document.getElementById('calBody').textContent.toLowerCase();
        return t.includes('accelerate') || t.includes('stop pedalling') || t.includes('calibrated');
      }, 3000, 'Calibration active phase');
      await browser.eval(() => document.getElementById('calClose').click());
      assert.equal(await browser.eval(() => document.getElementById('calModal').hidden), true);
    });

    // 11. Settings Modal (Validation + Save -> configSave) & Keys Modal
    await test('11. Settings modal (validation & bridge configSave) + Keys modal', async () => {
      await browser.eval(() => document.getElementById('btnKeys').click());
      assert.equal(await browser.eval(() => document.getElementById('keysModal').hidden), false);
      await browser.eval(() => document.getElementById('btnKeysClose').click());
      assert.equal(await browser.eval(() => document.getElementById('keysModal').hidden), true);

      bridge.ops.length = 0;
      await browser.eval(() => document.getElementById('btnSettings').click());
      await browser.waitFor(() => !document.getElementById('setModal').hidden, 2000, 'Settings modal open');

      // Invalid FTP (999) must be rejected without closing modal or writing config
      await browser.eval(() => {
        const inp = document.querySelector('#setPane input[data-path="ftp"]');
        inp.value = '999'; inp.dispatchEvent(new Event('input', { bubbles: true }));
        document.getElementById('setSave').click();
      });
      assert.equal(await browser.eval(() => document.getElementById('setModal').hidden), false, 'Modal stays open on invalid FTP');
      assert.ok((await browser.eval(() => document.getElementById('setMsg').textContent)).includes('Not saved'), 'Shows validation error');

      // Valid FTP (250) saves to bridge and updates main UI, then restore 200
      await browser.eval(() => {
        const inp = document.querySelector('#setPane input[data-path="ftp"]');
        inp.value = '250'; inp.dispatchEvent(new Event('input', { bubbles: true }));
        document.getElementById('setSave').click();
      });
      await waitUntil(() => bridge.ops.some(o => o.op === 'configSave' && o.config?.ftp === 250), 2500, 'configSave with FTP 250');
      await browser.waitFor(() => document.getElementById('setModal').hidden, 2000, 'Settings modal closed');
      assert.equal(await browser.eval(() => document.getElementById('ftp').value), '250');
      await browser.eval(() => { const f = document.getElementById('ftp'); f.value = '200'; f.dispatchEvent(new Event('change')); });
    });

    // 12. Session Recording, End Ride, Rides History Modal, CSV & Binary FIT Export (CRC-16 Verified!)
    await test('12. Recording, End Ride, Rides modal, CSV & Binary FIT Export (CRC-16 verified)', async () => {
      await browser.waitFor(() => /0:0[3-9]|0:[1-5]|[1-9]:/.test(document.getElementById('recInfo').textContent), 6000, 'At least 3 s recorded');

      // Export Binary FIT and verify FIT 14-byte header, '.FIT' magic, header CRC-16, and trailing file CRC-16!
      const before = new Set(fs.readdirSync(tmpDir));
      await browser.eval(() => document.getElementById('btnFit').click());
      await browser.eval(() => document.getElementById('btnCsv').click());
      await waitUntil(() => {
        const files = fs.readdirSync(tmpDir);
        return files.some(f => f.endsWith('.fit') && !before.has(f)) && files.some(f => f.endsWith('.csv') && !before.has(f));
      }, 3000, 'FIT & CSV downloads');

      const fitName = fs.readdirSync(tmpDir).find(f => f.endsWith('.fit') && !before.has(f));
      const fitBuf = fs.readFileSync(path.join(tmpDir, fitName));
      assert.equal(fitBuf[0], 14, 'FIT header size must be 14');
      assert.equal(fitBuf.subarray(8, 12).toString('ascii'), '.FIT', 'FIT magic must be .FIT');
      const hdrCrc = fitBuf.readUInt16LE(12);
      assert.equal(hdrCrc, fitCrc16(fitBuf.subarray(0, 12)), 'FIT header CRC-16 mismatch');
      const dataSize = fitBuf.readUInt32LE(4);
      assert.equal(fitBuf.length, 14 + dataSize + 2, 'FIT file length must match 14 + dataSize + 2');
      const fileCrc = fitBuf.readUInt16LE(fitBuf.length - 2);
      assert.equal(fileCrc, fitCrc16(fitBuf.subarray(0, fitBuf.length - 2)), 'FIT trailing CRC-16 mismatch');

      const csvName = fs.readdirSync(tmpDir).find(f => f.endsWith('.csv') && !before.has(f));
      const csvHead = fs.readFileSync(path.join(tmpDir, csvName), 'utf8').split(/\r?\n/)[0];
      assert.ok(csvHead.includes('heart_rate_bpm') && csvHead.includes('workout_step'));

      // End ride -> pushes to bridge -> open Rides modal and verify it appears!
      await browser.eval(() => document.getElementById('btnEndRide').click());
      await waitUntil(() => bridge.rides.size >= 1, 3000, 'Ride saved to bridge');
      await browser.eval(() => document.getElementById('btnRides').click());
      await browser.waitFor(() => !document.getElementById('ridesModal').hidden && document.querySelectorAll('#rideList .ride-item').length >= 1, 3000, 'Ride listed in Rides modal');
      await browser.eval(() => document.getElementById('btnRidesClose').click());
    });

    // 13. Excel (.xlsx) Export + Deflated SharedStrings Import + CSV + JSON + Old .xls
    await test('13. Excel .xlsx export/import (deflate-raw + sharedStrings), CSV, JSON & old .xls', async () => {
      const before = new Set(fs.readdirSync(tmpDir));
      await browser.eval(() => document.getElementById('woExportX').click());
      await waitUntil(() => fs.readdirSync(tmpDir).some(f => f.endsWith('.xlsx') && !before.has(f)), 3000, 'XLSX download');
      const xlsxName = fs.readdirSync(tmpDir).find(f => f.endsWith('.xlsx') && !before.has(f));
      const xlsxPath = path.join(tmpDir, xlsxName);

      const excelResavedPath = path.join(tmpDir, 'excel_resaved.xlsx');
      execFileSync('python3', ['-c', `
import zipfile, xml.etree.ElementTree as ET
src, dst = ${JSON.stringify(xlsxPath)}, ${JSON.stringify(excelResavedPath)}
with zipfile.ZipFile(src, 'r') as zf:
    assert zf.testzip() is None
    for n in zf.namelist(): ET.fromstring(zf.read(n))
ss_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="10" uniqueCount="10">
  <si><t>Workout</t></si><si><t>6Sec Deflated</t></si><si><t>Target</t></si><si><t>Mode</t></si>
  <si><t>Duration</t></si><si><t>Block</t></si><si><t>Repeat</t></si><si><t>Warm-up</t></si>
  <si><t>power</t></si><si><t>grade</t></si>
</sst>'''
sheet_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>
  <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
  <row r="4"><c r="A4" t="s"><v>2</v></c><c r="B4" t="s"><v>3</v></c><c r="C4" t="s"><v>4</v></c><c r="D4" t="s"><v>5</v></c><c r="E4" t="s"><v>6</v></c></row>
  <row r="5"><c r="A5"><v>250</v></c><c r="B5" t="s"><v>8</v></c><c r="C5" t="inlineStr"><is><t>2:30</t></is></c><c r="D5" t="s"><v>7</v></c><c r="E5"><v>2</v></c></row>
  <row r="6"><c r="A6"><v>5</v></c><c r="B6" t="s"><v>9</v></c><c r="C6" t="inlineStr"><is><t>0:45</t></is></c></row>
</sheetData></worksheet>'''
wb_xml = '''<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Workout" sheetId="1" r:id="rId1"/></sheets></workbook>'''
rels_xml = '''<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>'''
with zipfile.ZipFile(dst, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
    zout.writestr('xl/workbook.xml', wb_xml)
    zout.writestr('xl/_rels/workbook.xml.rels', rels_xml)
    zout.writestr('xl/sharedStrings.xml', ss_xml)
    zout.writestr('xl/worksheets/sheet1.xml', sheet_xml)
`]);

      await browser.uploadFile('#woFile', excelResavedPath);
      await browser.waitFor(() => document.getElementById('woName').value === '6Sec Deflated', 4000, 'Deflated XLSX imported');

      const tsvPath = path.join(tmpDir, 'tab_workout.csv');
      fs.writeFileSync(tsvPath, 'Block\tRepeat\tMinutes\tSeconds\tMode\tTarget\nSprints\t3\t0\t30\tresistance\t85\n\t\t1\t0\tsim\t2\n');
      await browser.uploadFile('#woFile', tsvPath);
      await browser.waitFor(() => document.getElementById('woName').value === 'tab_workout', 3000, 'Tab CSV imported');

      const jsonPath = path.join(tmpDir, 'valid.json');
      fs.writeFileSync(jsonPath, JSON.stringify({ name: 'JSON Tempo', blocks: [{ name: 'Tempo', repeat: 2, steps: [{ dur: 60, mode: 'erg', target: 220 }] }] }));
      await browser.uploadFile('#woFile', jsonPath);
      await browser.waitFor(() => document.getElementById('woName').value === 'JSON Tempo', 3000, 'JSON imported');

      // Old binary .xls file must be rejected cleanly with guidance, keeping current workout intact
      browser.dialogs.length = 0;
      const xlsPath = path.join(tmpDir, 'legacy.xls');
      fs.writeFileSync(xlsPath, Buffer.from([0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1]));
      await browser.uploadFile('#woFile', xlsPath);
      await waitUntil(() => browser.dialogs.some(d => d.toLowerCase().includes('.xls')), 2500, 'Old .xls rejection alert');
      assert.equal(await browser.eval(() => document.getElementById('woName').value), 'JSON Tempo');

      // Malformed JSON file must be rejected cleanly, keeping current workout intact
      browser.dialogs.length = 0;
      const badJsonPath = path.join(tmpDir, 'broken.json');
      fs.writeFileSync(badJsonPath, '{"name": "Broken", "blocks": [');
      await browser.uploadFile('#woFile', badJsonPath);
      await waitUntil(() => browser.dialogs.some(d => d.toLowerCase().includes('could not import')), 2500, 'Invalid JSON rejection alert');
      assert.equal(await browser.eval(() => document.getElementById('woName').value), 'JSON Tempo');
    });

    // 14. Stop Bridge Flow + 3 s Restart Polling
    await test('14. Stop bridge (FTMS Stop 0x08, shutdown) & 3 s restart polling', async () => {
      await browser.eval(() => document.getElementById('btnDisconnect').click());
      await sleep(100);
      await browser.eval(() => {
        const rows = [...document.querySelectorAll('#savedList .dev')];
        const row = rows.find(r => r.textContent.includes('Tacx Flux 46956'));
        [...row.querySelectorAll('button')].find(b => b.textContent.trim() === 'Connect').click();
      });
      await browser.waitFor(() => document.getElementById('status').textContent === 'Connected' && document.getElementById('devinfo').textContent.includes('FTMS'), 4000, 'FTMS connected');

      bridge.writes.length = 0;
      bridge.ops.length = 0;
      await browser.eval(() => document.getElementById('btnBridgeStop').click());
      await browser.waitFor(() => document.getElementById('bleVia').textContent.toLowerCase().includes('stopped'), 5000, 'Bridge stopped pill');
      assert.ok(bridge.writes.some(w => w.uuid === FTMS_CP && w.bytes[0] === 0x08), 'FTMS Stop 0x08 must be sent');
      assert.ok(bridge.ops.some(o => o.op === 'shutdown'), 'shutdown op must be sent');

      bridge.enableBridge = true;
      await browser.waitFor(() => document.getElementById('bleVia').textContent.includes('Bridge ·'), 6000, 'Bridge reconnected via polling');
    });

    // 15. Web Bluetooth Fallback & No-Bluetooth Banner
    await test('15. Web Bluetooth fallback & No-Bluetooth warning banner', async () => {
      bridge.enableBridge = false;
      const s1 = (await browser.send('Page.addScriptToEvaluateOnNewDocument', {
        source: 'Object.defineProperty(navigator, "bluetooth", { value: undefined, configurable: true });',
      })).identifier;
      await browser.goto(baseUrl);
      await browser.waitFor(() => document.getElementById('banner').textContent.toLowerCase().includes('bluetooth'), 5000, 'No-Bluetooth banner');
      await browser.send('Page.removeScriptToEvaluateOnNewDocument', { identifier: s1 });

      const s2 = (await browser.send('Page.addScriptToEvaluateOnNewDocument', {
        source: `
          window.__bleRequests = [];
          Object.defineProperty(navigator, "bluetooth", {
            configurable: true,
            value: { requestDevice: async (opts) => { window.__bleRequests.push(opts); throw new Error("User cancelled"); } }
          });
        `,
      })).identifier;
      await browser.goto(baseUrl);
      await browser.waitFor(() => document.getElementById('bleVia').textContent.toLowerCase().includes('browser'), 5000, 'Browser Bluetooth pill');
      await browser.eval(() => document.getElementById('btnScan').click());
      await browser.eval(() => document.getElementById('btnHrSearch').click());
      await sleep(100);
      const reqs = await browser.eval(() => window.__bleRequests);
      assert.equal(reqs.length, 2);
      assert.ok(reqs[0].filters.some(f => f.namePrefix === 'Tacx'));
      assert.ok(reqs[1].filters.some(f => f.services && f.services.includes(0x180D)));
      await browser.send('Page.removeScriptToEvaluateOnNewDocument', { identifier: s2 });
    });

    assert.equal(browser.pageErrors.length, 0, 'Uncaught page errors: ' + browser.pageErrors.join('\n'));
    console.log(`\nALL ${results.length} REAL-BROWSER E2E TESTS PASSED!`);
  } finally {
    await browser.close();
    await bridge.stop();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

run().catch((err) => {
  console.error('\nE2E FAILURE:', err);
  process.exit(1);
});
