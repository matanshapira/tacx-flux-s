#!/usr/bin/env node
/**
 * Independent Real-Browser End-to-End Test Suite for Tacx Flux S (`flux-s.html`).
 *
 * Uses zero npm dependencies:
 *  - Spins up a real localhost HTTP + RFC 6455 WebSocket server speaking the
 *    exact `flux_bridge.py` protocol (serving the real `flux-s.html` file).
 *  - Launches real headless Google Chrome via CDP (Chrome DevTools Protocol).
 *  - Exercises EVERY user-facing feature end-to-end:
 *      1. Bridge detection, live BLE scan, RSSI sort, Trainers & HR filter, + Add
 *      2. FTMS connect, 0x2AD6/0x2AD8 ranges, Request Control + Start, Control Point
 *         0x05 ("control not permitted") auto-recovery, ERG / Slope / Resistance
 *         commands (both uint8 & sint16 resistance ranges), 0x2AD2 binary parsing
 *         (including trainer HR fallback), keyboard shortcuts
 *      3. Simultaneous HR strap (0x180D / 0x2A37 8-bit, 16-bit, contact-not-detected,
 *         0x180F / 0x2A19 battery, header pill, zones Z1-Z5, unexpected drop reconnect)
 *      4. Saved devices per transport, separate ★ defaults per role, sequential
 *         auto-connect on reload
 *      5. FE-C over BLE (Auto fallback & FE-C only), ANT framing + XOR checksum
 *         verification for pages 0x10, 0x19, 0x30, 0x31, 0x37->0x32->0x33, and
 *         synced Rider weight re-sending FE-C Slope
 *      6. Trainer unexpected disconnect -> auto-reconnect + 3 s stale dimming
 *      7. Session validation (FTP 50-600, Rider 30-200, Max HR 100-230), Demo mode
 *         physics, recording, auto-pause, Reset, CSV export
 *      8. Workout builder (edit/move/dup/delete), live mid-workout edit, player
 *         (Start, Pause, Back <5s & >=5s, Skip, Stop), Library Save/Load/New/Delete
 *      9. Zero-library Excel (.xlsx) export -> Python ZIP/XML check -> deflated +
 *         sharedStrings + reordered-column + bad-row + clamped-target .xlsx import;
 *         CSV (semicolon & tab) import; JSON import; old .xls rejection message
 *     10. Stop bridge flow (pause workout -> stop scan -> disconnect HR -> FTMS Stop
 *         0x08 -> disconnect trainer -> shutdown) + 3 s restart polling & auto-reconnect
 *     11. Web Bluetooth fallback (mocked navigator.bluetooth) & No-Bluetooth banner
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
  for (const p of candidates) {
    if (fs.existsSync(p)) return p;
  }
  throw new Error('Could not find Chrome/Chromium binary. Set CHROME_PATH env var.');
}

/* =========================================================================
 * Minimal RFC 6455 WebSocket Server + Mock Bridge (exact flux_bridge.py protocol)
 * ========================================================================= */
function encodeWsText(str) {
  const payload = Buffer.from(str, 'utf8');
  let header;
  if (payload.length < 126) {
    header = Buffer.from([0x81, payload.length]);
  } else if (payload.length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(payload.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(payload.length), 2);
  }
  return Buffer.concat([header, payload]);
}

function attachWsDecoder(socket, onMessage, onClose) {
  let buf = Buffer.alloc(0);
  socket.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const b1 = buf[0];
      const b2 = buf[1];
      const opcode = b1 & 0x0f;
      const masked = (b2 & 0x80) !== 0;
      let len = b2 & 0x7f;
      let off = 2;
      if (len === 126) {
        if (buf.length < 4) return;
        len = buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (buf.length < 10) return;
        len = Number(buf.readBigUInt64BE(2));
        off = 10;
      }
      const need = off + (masked ? 4 : 0) + len;
      if (buf.length < need) return;
      let payload = buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) {
        const mask = buf.subarray(off, off + 4);
        const unmasked = Buffer.alloc(len);
        for (let i = 0; i < len; i++) unmasked[i] = payload[i] ^ mask[i % 4];
        payload = unmasked;
      }
      buf = buf.subarray(need);
      if (opcode === 0x8) {
        socket.end();
        return;
      }
      if (opcode === 0x9) {
        socket.write(Buffer.from([0x8a, 0x00]));
        continue;
      }
      if (opcode === 0x1) {
        onMessage(payload.toString('utf8'));
      }
    }
  });
  socket.on('close', onClose);
  socket.on('error', () => {});
}

class MockBridgeServer {
  constructor() {
    this.server = null;
    this.port = 0;
    this.wsSockets = new Set();
    this.enableBridge = true;
    this.connectedAddrs = new Set();
    this.writes = []; // { address, uuid, hex, bytes, response }
    this.ops = [];
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
        'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
      );
      this.wsSockets.add(socket);

      const send = (obj) => {
        if (!socket.destroyed) socket.write(encodeWsText(JSON.stringify(obj)));
      };

      attachWsDecoder(socket, (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }
        this.ops.push(msg);
        const { id, op } = msg;

        if (op === 'hello') {
          send({ id, ok: true, version: 3 });
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
          let name = addr;
          let services = [];
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
              {
                uuid: FEC_SVC,
                chars: [
                  { uuid: FEC_RX, props: ['notify'] },
                  { uuid: FEC_TX, props: ['write-without-response', 'write'] },
                ],
              },
            ];
          } else if (addr === TRAINER_FEC_ADDR) {
            name = 'Tacx Legacy FEC';
            services = [
              {
                uuid: FEC_SVC,
                chars: [
                  { uuid: FEC_RX, props: ['notify'] },
                  { uuid: FEC_TX, props: ['write-without-response', 'write'] },
                ],
              },
            ];
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
            let resCode = 0x01; // Success
            if (this.ftmsRejectNextControlNotPermitted && reqOp !== 0x00) {
              this.ftmsRejectNextControlNotPermitted = false;
              resCode = 0x05; // Control Not Permitted
            }
            setTimeout(() => {
              send({
                ev: 'notify',
                address: msg.address,
                uuid: FTMS_CP,
                data: Buffer.from([0x80, reqOp, resCode]).toString('hex'),
              });
            }, 10);
          }
        } else if (op === 'shutdown') {
          send({ id, ok: true });
          this.enableBridge = false;
          setTimeout(() => {
            for (const s of Array.from(this.wsSockets)) {
              try { s.end(); } catch {}
            }
            this.wsSockets.clear();
            this.connectedAddrs.clear();
          }, 40);
        }
      }, () => {
        this.wsSockets.delete(socket);
      });
    });

    await new Promise((resolve) => this.server.listen(preferredPort, '127.0.0.1', resolve));
    this.port = this.server.address().port;
  }

  broadcast(obj) {
    const frame = encodeWsText(JSON.stringify(obj));
    for (const s of this.wsSockets) {
      if (!s.destroyed) s.write(frame);
    }
  }

  pushNotify(address, uuid, bytes) {
    this.broadcast({ ev: 'notify', address, uuid, data: Buffer.from(bytes).toString('hex') });
  }

  pushDisconnect(address) {
    this.connectedAddrs.delete(address);
    this.broadcast({ ev: 'disconnected', address });
  }

  async stop() {
    for (const s of Array.from(this.wsSockets)) {
      try { s.destroy(); } catch {}
    }
    this.wsSockets.clear();
    if (this.server) {
      if (this.server.closeAllConnections) this.server.closeAllConnections();
      await new Promise((resolve) => this.server.close(resolve));
      this.server = null;
    }
  }
}

/* =========================================================================
 * Minimal CDP (Chrome DevTools Protocol) Driver using built-in WebSocket
 * ========================================================================= */
class CDPBrowser {
  constructor() {
    this.proc = null;
    this.ws = null;
    this.userDataDir = null;
    this.nextId = 1;
    this.pending = new Map();
    this.consoleErrors = [];
    this.pageErrors = [];
    this.dialogs = [];
    this.downloads = [];
  }

  async launch(downloadDir) {
    const bin = findChromeBinary();
    this.userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flux-cdp-'));
    this.proc = spawn(bin, [
      '--headless=new',
      '--disable-gpu',
      '--no-sandbox',
      '--disable-dev-shm-usage',
      '--remote-debugging-port=0',
      `--user-data-dir=${this.userDataDir}`,
      '--window-size=1440,950',
      'about:blank',
    ], { stdio: ['ignore', 'pipe', 'pipe'] });

    const wsUrl = await new Promise((resolve, reject) => {
      let stderr = '';
      const timer = setTimeout(() => reject(new Error('Chrome did not start CDP in time:\n' + stderr)), 15000);
      this.proc.stderr.on('data', (d) => {
        stderr += d.toString();
        const m = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/);
        if (m) { clearTimeout(timer); resolve(m[1]); }
      });
      this.proc.on('exit', (code) => {
        clearTimeout(timer);
        reject(new Error(`Chrome exited early (${code}):\n${stderr}`));
      });
    });

    const dbgBase = wsUrl.replace(/^ws:/, 'http:').replace(/\/devtools\/.*$/, '');
    const targets = await (await fetch(`${dbgBase}/json/list`)).json();
    const pageTarget = targets.find((t) => t.type === 'page');
    if (!pageTarget) throw new Error('No Chrome page target found');

    this.ws = new WebSocket(pageTarget.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
      this.ws.addEventListener('open', resolve);
      this.ws.addEventListener('error', reject);
    });

    this.ws.addEventListener('message', (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.id && this.pending.has(msg.id)) {
        const p = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        if (msg.error) p.reject(new Error(msg.error.message));
        else p.resolve(msg.result);
        return;
      }
      if (msg.method === 'Runtime.exceptionThrown') {
        const d = msg.params.exceptionDetails;
        this.pageErrors.push(d.exception?.description || d.text || 'Runtime exception');
      } else if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error') {
        this.consoleErrors.push(msg.params.args.map((a) => a.value ?? a.description).join(' '));
      } else if (msg.method === 'Page.javascriptDialogOpening') {
        this.dialogs.push(msg.params.message);
        this.send('Page.handleJavaScriptDialog', { accept: true }).catch(() => {});
      }
    });

    await this.send('Page.enable');
    await this.send('Runtime.enable');
    await this.send('DOM.enable');
    await this.send('Browser.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: downloadDir,
    });
  }

  send(method, params = {}) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`CDP ${method} timed out`));
      }, 15000);
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
    const res = await this.send('Runtime.evaluate', {
      expression: expr,
      awaitPromise: true,
      returnByValue: true,
    });
    if (res.exceptionDetails) {
      throw new Error('Page eval error: ' + (res.exceptionDetails.exception?.description || res.exceptionDetails.text));
    }
    return res.result.value;
  }

  async waitFor(fn, timeoutMs = 8000, label = 'condition') {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const ok = await this.eval(fn);
      if (ok) return ok;
      await sleep(60);
    }
    throw new Error(`Timed out waiting for: ${label}`);
  }

  async uploadFile(selector, filePath) {
    const doc = await this.send('DOM.getDocument', { depth: -1 });
    const q = await this.send('DOM.querySelector', { nodeId: doc.root.nodeId, selector });
    assert.ok(q.nodeId, `Element ${selector} not found for file upload`);
    await this.send('DOM.setFileInputFiles', { nodeId: q.nodeId, files: [filePath] });
  }

  async close() {
    try { this.ws?.close(); } catch {}
    if (this.proc) {
      this.proc.kill('SIGKILL');
      await new Promise((r) => this.proc.on('exit', r));
    }
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
  // flags: bit0=0 (speed), bit2=cad, bit5=res, bit6=pwr, bit7=avgPwr(2B), bit8=energy(5B), bit9=hr(optional)
  let flags = 0x0004 | 0x0020 | 0x0040 | 0x0080 | 0x0100;
  if (hrBpm !== null) flags |= 0x0200;
  const len = 2 + 2 + 2 + 2 + 2 + 2 + 5 + (hrBpm !== null ? 1 : 0);
  const buf = Buffer.alloc(len);
  let o = 0;
  buf.writeUInt16LE(flags, o); o += 2;
  buf.writeUInt16LE(Math.round(speedKmh * 100), o); o += 2;
  buf.writeUInt16LE(Math.round(cadenceRpm * 2), o); o += 2;
  buf.writeInt16LE(resistance, o); o += 2;
  buf.writeInt16LE(powerW, o); o += 2;
  buf.writeInt16LE(powerW - 10, o); o += 2; // avg power skipped
  o += 5;                                   // expended energy skipped
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
  assert.equal(buf.length, 13, 'ANT packet must be 13 bytes');
  assert.equal(buf[0], 0xa4, 'ANT sync byte must be 0xA4');
  let xor = 0;
  for (let i = 0; i < 12; i++) xor ^= buf[i];
  assert.equal(buf[12], xor, 'ANT XOR checksum mismatch');
}

/* =========================================================================
 * MAIN E2E TEST SUITE
 * ========================================================================= */
async function run() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'flux-e2e-'));
  const bridge = new MockBridgeServer();
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

    // 1. Load & Bridge Transport Detection
    await test('1. Page load, zero errors & Bridge transport detection', async () => {
      await browser.goto(baseUrl);
      await browser.waitFor(() => document.getElementById('transportPill').textContent.includes('Bridge'), 5000, 'Bridge transport pill');
      const stopVisible = await browser.eval(() => getComputedStyle(document.getElementById('btnStopBridge')).display !== 'none');
      assert.equal(stopVisible, true, 'Stop bridge button must be visible in bridge mode');
      assert.equal(browser.pageErrors.length, 0, 'Page errors: ' + browser.pageErrors.join('; '));
    });

    // 2. Live Scan, RSSI Sort, Trainers & HR Filter, + Add
    await test('2. Bridge live scan, RSSI sort, filter & + Add', async () => {
      await browser.eval(() => document.getElementById('btnSearchDevices').click());
      await browser.waitFor(() => document.querySelectorAll('#scanList .scan-item').length === 3, 4000, '3 filtered scan items');
      const names = await browser.eval(() => [...document.querySelectorAll('#scanList .dev-name')].map(el => el.textContent));
      // Sorted by RSSI: Polar H10 (-54), Tacx Flux 46956 (-58), Tacx Legacy FEC (-68); Kitchen Speaker hidden
      assert.deepEqual(names, ['Polar H10', 'Tacx Flux 46956', 'Tacx Legacy FEC']);

      // Uncheck Trainers & HR only -> Kitchen Speaker (-45) appears at top
      await browser.eval(() => {
        const cb = document.getElementById('chkTrainersOnly');
        cb.checked = false;
        cb.dispatchEvent(new Event('change'));
      });
      await browser.waitFor(() => document.querySelectorAll('#scanList .scan-item').length === 4, 2000, '4 unfiltered scan items');
      const firstUnfiltered = await browser.eval(() => document.querySelector('#scanList .dev-name').textContent);
      assert.equal(firstUnfiltered, 'Kitchen Speaker');

      // Re-enable filter and click + Add on Tacx Legacy FEC
      await browser.eval(() => {
        const cb = document.getElementById('chkTrainersOnly');
        cb.checked = true;
        cb.dispatchEvent(new Event('change'));
      });
      await browser.eval(() => {
        const items = [...document.querySelectorAll('#scanList .scan-item')];
        const fecItem = items.find(el => el.textContent.includes('Tacx Legacy FEC'));
        fecItem.querySelector('.btn-add').click();
      });
      const savedText = await browser.eval(() => document.getElementById('savedDevicesList').textContent);
      assert.ok(savedText.includes('Tacx Legacy FEC'), 'Added trainer should appear in Saved Devices');
    });

    // 3. FTMS Connect, Telemetry, Zones, Control Point 0x05 Recovery, Modes & Keyboard
    await test('3. FTMS connect, 0x2AD2 telemetry, zones, 0x05 recovery & ERG/Slope/Res', async () => {
      bridge.writes.length = 0;
      await browser.eval(() => {
        const items = [...document.querySelectorAll('#scanList .scan-item')];
        const ftmsItem = items.find(el => el.textContent.includes('Tacx Flux 46956'));
        ftmsItem.querySelector('.btn-conn-tr').click();
      });
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent.includes('FTMS'), 5000, 'FTMS connected');

      // Verify Request Control (0x00) and Start (0x07) were sent
      const cpOps = bridge.writes.filter(w => w.uuid === FTMS_CP).map(w => w.bytes[0]);
      assert.ok(cpOps.includes(0x00) && cpOps.includes(0x07), 'Expected FTMS 0x00 and 0x07, got: ' + cpOps);

      // Push FTMS 0x2AD2 packet (240 W = 96% of 250 W FTP => Z4 Threshold; trainer HR fallback = 148 bpm)
      bridge.pushNotify(TRAINER_FTMS_ADDR, FTMS_BIKE, buildFtmsBikeFrame({ speedKmh: 34.2, cadenceRpm: 92, resistance: 45, powerW: 240, hrBpm: 148 }));
      await browser.waitFor(() => document.getElementById('valPower').textContent === '240', 3000, 'Power 240 W');

      const snap = await browser.eval(() => ({
        pwr: document.getElementById('valPower').textContent,
        pctFtp: document.getElementById('valPctFtp').textContent,
        wkg: document.getElementById('valWkg').textContent,
        zone: document.getElementById('valZoneBadge').textContent,
        subFtp: document.getElementById('subFtp').textContent,
        cad: document.getElementById('valCadence').textContent,
        spd: document.getElementById('valSpeed').textContent,
        hr: document.getElementById('valHr').textContent,
        subHr: document.getElementById('subHr').textContent,
      }));
      assert.equal(snap.pctFtp, '96');
      assert.equal(snap.wkg, '3.2');
      assert.equal(snap.zone, 'Z4');
      assert.ok(snap.subFtp.includes('Threshold'));
      assert.equal(snap.cad, '92');
      assert.equal(snap.spd, '34.2');
      assert.equal(snap.hr, '148');
      assert.ok(snap.subHr.includes('trainer'));

      // Test Control Not Permitted (0x05) recovery on ERG change via keyboard Shift+ArrowUp (+25 W -> 175 W)
      bridge.writes.length = 0;
      bridge.ftmsRejectNextControlNotPermitted = true;
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowUp', shiftKey: true, bubbles: true })));
      await browser.waitFor(() => document.getElementById('cmdAckBar').textContent.includes('ERG 175 W'), 3000, 'ERG 175 W ack after 0x05 retry');
      const retryOps = bridge.writes.filter(w => w.uuid === FTMS_CP).map(w => w.bytes[0]);
      assert.deepEqual(retryOps, [0x05, 0x00, 0x05], 'Should retry 0x05 after 0x00 Request Control');
      const ergWrite = bridge.writes.filter(w => w.uuid === FTMS_CP && w.bytes[0] === 0x05).pop();
      assert.equal(ergWrite.bytes.readInt16LE(1), 175);

      // Switch to Slope mode with key '2' and adjust +2% via Shift+ArrowUp
      bridge.writes.length = 0;
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: '2', bubbles: true })));
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowUp', shiftKey: true, bubbles: true })));
      await browser.waitFor(() => document.getElementById('cmdAckBar').textContent.includes('Slope 2.0 %'), 3000, 'Slope 2.0% ack');
      const simWrite = bridge.writes.find(w => w.uuid === FTMS_CP && w.bytes[0] === 0x11);
      assert.ok(simWrite, 'Expected FTMS 0x11 Slope command');
      assert.equal(simWrite.bytes.readInt16LE(3), 200, 'Grade 2.0% should encode as 200 (0.01%)');

      // Switch to Resistance mode with key '3' -> range 0..100 fits in uint8 (2 bytes total: [0x04, 25])
      bridge.writes.length = 0;
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: '3', bubbles: true })));
      await browser.waitFor(() => document.getElementById('cmdAckBar').textContent.includes('Resistance 25 %'), 3000, 'Resistance 25% ack');
      const resWrite = bridge.writes.find(w => w.uuid === FTMS_CP && w.bytes[0] === 0x04);
      assert.ok(resWrite, 'Expected FTMS 0x04 Resistance command');
      assert.equal(resWrite.bytes.length, 2, 'Range 0..100 must send uint8 (2 bytes)');
      assert.equal(resWrite.bytes[1], 25);

      // Reconnect with sint16 resistance range (0..1000 = 0x03E8) -> must send 3 bytes [0x04, lo, hi]
      bridge.ftmsResRangeHex = '0000e8030100';
      await browser.eval(() => document.getElementById('btnDisconnect').click());
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent === 'Disconnected', 3000, 'Disconnected for sint16 test');
      bridge.writes.length = 0;
      await browser.eval(() => {
        const rows = [...document.querySelectorAll('#savedDevicesList .saved-item')];
        rows.find(r => r.textContent.includes('Tacx Flux 46956')).querySelector('.btn-conn').click();
      });
      await browser.waitFor(() => document.getElementById('cmdAckBar').textContent.includes('Resistance 25 %'), 4000, 'sint16 Resistance 25% ack');
      const resWrite16 = bridge.writes.find(w => w.uuid === FTMS_CP && w.bytes[0] === 0x04);
      assert.ok(resWrite16, 'Expected sint16 FTMS 0x04 Resistance command');
      assert.equal(resWrite16.bytes.length, 3, 'Range 0..1000 must send sint16 (3 bytes)');
      assert.equal(resWrite16.bytes.readInt16LE(1), 250, '25% of 0..1000 must be 250');
      bridge.ftmsResRangeHex = '000064000100';

      // Switch back to ERG mode with key '1'
      await browser.eval(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: '1', bubbles: true })));
      await sleep(320);
    });

    // 4. Simultaneous HR Strap (8-bit, 16-bit, contact-not-detected, battery, drop reconnect)
    await test('4. Simultaneous HR strap (0x2A37 flags, battery, zones, reconnect)', async () => {
      await browser.eval(() => document.getElementById('btnSearchHr').click());
      await browser.waitFor(() => document.querySelector('#scanList .btn-conn-hr'), 3000, 'Connect HR button in scan list');
      await browser.eval(() => document.querySelector('#scanList .btn-conn-hr').click());
      await browser.waitFor(() => document.getElementById('hrHeaderPill').style.display !== 'none', 4000, 'HR header pill');

      const pillText = await browser.eval(() => document.getElementById('hrHeaderText').textContent);
      assert.ok(pillText.includes('Polar H10') && pillText.includes('87%'), 'Expected Polar H10 + 87% battery, got: ' + pillText);

      // 8-bit HR = 162 bpm (85% of 190 => Z4 Threshold), overrides trainer HR
      bridge.pushNotify(HR_ADDR, HR_MEAS, [0x06, 162]);
      await browser.waitFor(() => document.getElementById('valHr').textContent === '162', 2500, 'HR 162 bpm');
      const hrZ4 = await browser.eval(() => ({ z: document.getElementById('valHrZoneBadge').textContent, sub: document.getElementById('subHr').textContent }));
      assert.equal(hrZ4.z, 'Z4');
      assert.ok(!hrZ4.sub.includes('trainer'), 'Strap HR should not show trainer fallback label');

      // 16-bit HR = 176 bpm (93% of 190 => Z5 Max)
      bridge.pushNotify(HR_ADDR, HR_MEAS, [0x01, 176, 0]);
      await browser.waitFor(() => document.getElementById('valHr').textContent === '176', 2500, 'HR 176 bpm (16-bit)');
      assert.equal(await browser.eval(() => document.getElementById('valHrZoneBadge').textContent), 'Z5');

      // Contact supported but NOT detected (flags = 0x04) => ignores reading!
      bridge.pushNotify(HR_ADDR, HR_MEAS, [0x04, 199]);
      await sleep(1200);
      const hrNoContact = await browser.eval(() => document.getElementById('valHr').textContent);
      assert.notEqual(hrNoContact, '199', 'Contact-not-detected HR value must be ignored');

      // Unexpected HR disconnect -> auto-reconnects!
      bridge.pushDisconnect(HR_ADDR);
      await browser.waitFor(() => document.getElementById('hrRowStatus').textContent.includes('Reconnecting'), 2000, 'HR reconnecting state');
      await browser.waitFor(() => document.getElementById('hrHeaderPill').style.display !== 'none', 5000, 'HR auto-reconnected');
    });

    // 5. Saved Devices ★ Defaults & Sequential Auto-Connect on Reload
    await test('5. Saved devices ★ defaults & auto-connect on page reload', async () => {
      // Ensure Tacx Flux 46956 is ★ default trainer (Tacx Legacy FEC was added first, so toggle star on Tacx Flux 46956)
      await browser.eval(() => {
        const rows = [...document.querySelectorAll('#savedDevicesList .saved-item')];
        const fluxRow = rows.find(r => r.textContent.includes('Tacx Flux 46956'));
        if (!fluxRow.querySelector('.star-btn').classList.contains('on')) {
          fluxRow.querySelector('.star-btn').click();
        }
      });

      // Reload page -> auto-connect should connect Tacx Flux 46956 then Polar H10!
      bridge.ops.length = 0;
      await browser.goto(baseUrl);
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent.includes('Tacx Flux 46956'), 6000, 'Auto-connected default trainer');
      await browser.waitFor(() => document.getElementById('hrHeaderText').textContent.includes('Polar H10'), 6000, 'Auto-connected default HR');
    });

    // 6. FE-C Over BLE Driver (Auto fallback, ANT framing/XOR, Rider weight sync)
    await test('6. FE-C over BLE (Auto fallback, pages 0x10/0x19/0x30/0x31/0x37-0x32-0x33 & Rider weight sync)', async () => {
      await browser.eval(() => document.getElementById('btnDisconnect').click());
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent === 'Disconnected', 3000, 'Trainer disconnected');
      await browser.eval(() => document.getElementById('btnDisconnectHr').click());
      await sleep(100);

      // Connect FE-C-only trainer with Protocol = Auto -> must fall back to FE-C!
      bridge.writes.length = 0;
      await browser.eval(() => {
        const rows = [...document.querySelectorAll('#savedDevicesList .saved-item')];
        const fecRow = rows.find(r => r.textContent.includes('Tacx Legacy FEC'));
        fecRow.querySelector('.btn-conn').click();
      });
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent.includes('FE-C'), 5000, 'FE-C fallback connected');

      // Send ANT page 0x10 (speed = 10.000 m/s = 36.0 km/h, HR = 135) and page 0x19 (cad = 88, power = 275 = 0x113)
      bridge.pushNotify(TRAINER_FEC_ADDR, FEC_RX, wrapAntPage([0x10, 0x19, 0, 0, 0x10, 0x27, 135, 0]));
      bridge.pushNotify(TRAINER_FEC_ADDR, FEC_RX, wrapAntPage([0x19, 0x01, 88, 0, 0, 0x13, 0x01, 0]));
      await browser.waitFor(() => document.getElementById('valPower').textContent === '275', 3000, 'FE-C power 275 W');
      assert.equal(await browser.eval(() => document.getElementById('valSpeed').textContent), '36.0');
      assert.equal(await browser.eval(() => document.getElementById('valCadence').textContent), '88');
      assert.equal(await browser.eval(() => document.getElementById('valHr').textContent), '135');

      // ERG -> ANT page 0x31 (0.25 W units: 200 W => 800 = 0x0320)
      bridge.writes.length = 0;
      await browser.eval(() => {
        const inp = document.getElementById('ergInput');
        inp.value = '200';
        inp.dispatchEvent(new Event('change'));
      });
      await waitUntil(() => bridge.writes.some(w => w.uuid === FEC_TX && w.bytes[4] === 0x31), 2500, 'FE-C page 0x31');
      const ergPkt = bridge.writes.find(w => w.uuid === FEC_TX && w.bytes[4] === 0x31).bytes;
      assertValidAntChecksum(ergPkt);
      assert.equal(ergPkt.readUInt16LE(10), 800, '200 W in 0.25 W units must be 800');

      // Resistance -> ANT page 0x30 (0.5% units: 40% => 80)
      bridge.writes.length = 0;
      await browser.eval(() => document.querySelector('.mode-tab[data-mode="res"]').click());
      await browser.eval(() => {
        const inp = document.getElementById('resInput');
        inp.value = '40';
        inp.dispatchEvent(new Event('change'));
      });
      await waitUntil(() => bridge.writes.some(w => w.uuid === FEC_TX && w.bytes[4] === 0x30), 2500, 'FE-C page 0x30');
      const resPkt = bridge.writes.find(w => w.uuid === FEC_TX && w.bytes[4] === 0x30).bytes;
      assertValidAntChecksum(resPkt);
      assert.equal(resPkt[11], 80, '40% in 0.5% units must be 80');

      // Slope -> ANT pages 0x37, 0x32, 0x33 in order
      bridge.writes.length = 0;
      await browser.eval(() => document.querySelector('.mode-tab[data-mode="sim"]').click());
      await waitUntil(() => bridge.writes.filter(w => w.uuid === FEC_TX).length >= 3, 2500, 'FE-C Slope 3 pages');
      const slopePages = bridge.writes.filter(w => w.uuid === FEC_TX).slice(0, 3);
      assert.deepEqual(slopePages.map(p => p.bytes[4]), [0x37, 0x32, 0x33], 'FE-C slope order must be 0x37, 0x32, 0x33');
      slopePages.forEach(p => assertValidAntChecksum(p.bytes));

      // Change Rider weight in Session card -> syncs to Slope input AND re-sends FE-C page 0x37
      bridge.writes.length = 0;
      await browser.eval(() => {
        const inp = document.getElementById('inpRiderWeight');
        inp.value = '70';
        inp.dispatchEvent(new Event('change'));
      });
      assert.equal(await browser.eval(() => document.getElementById('simRiderWeight').value), '70');
      await waitUntil(() => bridge.writes.some(w => w.uuid === FEC_TX && w.bytes[4] === 0x37), 2500, 'FE-C re-sent page 0x37 on weight change');
      const wPkt = bridge.writes.find(w => w.uuid === FEC_TX && w.bytes[4] === 0x37).bytes;
      assert.equal(wPkt.readUInt16LE(5), 7000, '70 kg must encode as 7000 (0.01 kg)');
    });

    // 7. Trainer Auto-Reconnect + Stale 3 s Dimming
    await test('7. Trainer unexpected disconnect auto-reconnect & 3 s stale dimming', async () => {
      bridge.pushDisconnect(TRAINER_FEC_ADDR);
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent.includes('Reconnecting'), 2000, 'Trainer reconnecting');
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent.includes('FE-C'), 5000, 'Trainer auto-reconnected');

      // Wait >3.2 s without sending telemetry -> Power tile should have .stale class
      await sleep(3400);
      const isStale = await browser.eval(() => document.getElementById('tilePower').classList.contains('stale'));
      assert.equal(isStale, true, 'Power tile must dim (.stale) after 3 s without data');
    });

    // 8. Session Validation, Demo Mode Physics, Recording & CSV Export
    await test('8. Settings validation, Demo mode physics, recording & CSV export', async () => {
      // Invalid FTP rejected, retains 250
      await browser.eval(() => {
        const inp = document.getElementById('inpFtp');
        inp.value = '999';
        inp.dispatchEvent(new Event('change'));
      });
      assert.ok(await browser.eval(() => document.getElementById('errFtp').textContent.includes('50')));
      assert.equal(await browser.eval(() => document.getElementById('inpFtp').value), '250');

      // Valid FTP = 200 saved
      await browser.eval(() => {
        const inp = document.getElementById('inpFtp');
        inp.value = '200';
        inp.dispatchEvent(new Event('change'));
      });
      assert.equal(await browser.eval(() => localStorage.getItem('flux_ftp_w')), '200');

      // Disconnect BLE trainer & start Demo mode
      await browser.eval(() => document.getElementById('btnDisconnect').click());
      await sleep(100);
      await browser.eval(() => document.getElementById('btnRecReset').click());
      await browser.eval(() => document.getElementById('btnDemo').click());
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent === 'Demo mode', 3000, 'Demo mode active');
      await browser.waitFor(() => parseInt(document.getElementById('samplesNote').textContent, 10) >= 2, 4500, 'At least 2 recorded samples');

      // Export CSV and verify headers + row contents
      const beforeFiles = new Set(fs.readdirSync(tmpDir));
      await browser.eval(() => document.getElementById('btnExportCsv').click());
      await sleep(400);
      const newCsv = fs.readdirSync(tmpDir).find(f => f.endsWith('.csv') && !beforeFiles.has(f));
      assert.ok(newCsv, 'Expected downloaded CSV file');
      const csvContent = fs.readFileSync(path.join(tmpDir, newCsv), 'utf8');
      const csvHeader = csvContent.split(/\r?\n/)[0];
      assert.ok(csvHeader.includes('heart_rate_bpm') && csvHeader.includes('workout_step'), 'CSV missing required columns: ' + csvHeader);
    });

    // 9. Workout Builder, Live Edit, Player Controls & Library
    await test('9. Workout builder, live mid-workout edit, player & library', async () => {
      // Sample workout loaded by default
      const wkName = await browser.eval(() => document.getElementById('wkNameInput').value);
      assert.ok(wkName.includes('Sample'), 'Sample workout should be loaded');

      // Start workout in Demo mode
      await browser.eval(() => document.getElementById('btnPlStart').click());
      await browser.waitFor(() => document.getElementById('plStepTitle').textContent.includes('Warm-up'), 2000, 'Workout running');
      assert.equal(await browser.eval(() => document.getElementById('ergReadout').textContent), '120', 'Step 1 should set ERG 120 W');

      // Skip to step 2 (165 W)
      await browser.eval(() => document.getElementById('btnPlSkip').click());
      assert.equal(await browser.eval(() => document.getElementById('ergReadout').textContent), '165');

      // Back within <5 s returns to step 1 (120 W)
      await browser.eval(() => document.getElementById('btnPlBack').click());
      assert.equal(await browser.eval(() => document.getElementById('ergReadout').textContent), '120');

      // Live mid-workout edit: change step 1 target to 140 W -> immediately takes effect!
      await browser.eval(() => {
        const inp = document.querySelector('#wkBlocksContainer .step-target');
        inp.value = '140';
        inp.dispatchEvent(new Event('change'));
      });
      assert.equal(await browser.eval(() => document.getElementById('ergReadout').textContent), '140');

      // Save to library, New, then reload from library
      await browser.eval(() => {
        const n = document.getElementById('wkNameInput');
        n.value = 'My E2E Plan';
        n.dispatchEvent(new Event('input'));
        document.getElementById('btnWkSave').click();
      });
      await browser.eval(() => document.getElementById('btnPlStop').click());
      await browser.eval(() => document.getElementById('btnWkNew').click());
      assert.equal(await browser.eval(() => document.getElementById('wkNameInput').value), 'New Workout');

      await browser.eval(() => {
        const sel = document.getElementById('wkLibrarySelect');
        sel.value = 'My E2E Plan';
        sel.dispatchEvent(new Event('change'));
      });
      assert.equal(await browser.eval(() => document.getElementById('wkNameInput').value), 'My E2E Plan');

      // Test Pause/Resume + Library Delete
      await browser.eval(() => document.getElementById('btnPlStart').click());
      await browser.eval(() => document.getElementById('btnPlPause').click());
      assert.equal(await browser.eval(() => document.getElementById('btnPlPause').textContent.includes('Resume')), true);
      await browser.eval(() => document.getElementById('btnPlPause').click());
      assert.equal(await browser.eval(() => document.getElementById('btnPlPause').textContent.includes('Pause')), true);
      await browser.eval(() => document.getElementById('btnPlStop').click());
      await browser.eval(() => document.getElementById('btnWkDelete').click());
      assert.equal(await browser.eval(() => [...document.getElementById('wkLibrarySelect').options].some(o => o.value === 'My E2E Plan')), false);
    });

    // 10. Excel (.xlsx) Export + Deflated/SharedStrings/Reordered Import + CSV + JSON + Old .xls
    await test('10. Excel .xlsx export/import (deflate-raw + sharedStrings), CSV, JSON & old .xls', async () => {
      const before = new Set(fs.readdirSync(tmpDir));
      await browser.eval(() => document.getElementById('btnWkExportXlsx').click());
      await sleep(500);
      const xlsxName = fs.readdirSync(tmpDir).find(f => f.endsWith('.xlsx') && !before.has(f));
      assert.ok(xlsxName, 'Expected downloaded .xlsx file');
      const xlsxPath = path.join(tmpDir, xlsxName);

      // Verify exported .xlsx with Python zipfile AND build a realistic Excel-resaved deflated .xlsx
      // with sharedStrings, reordered columns, Duration text, bad row, and clamped target!
      const excelResavedPath = path.join(tmpDir, 'excel_resaved.xlsx');
      execFileSync('python3', ['-c', `
import zipfile, xml.etree.ElementTree as ET
src, dst = ${JSON.stringify(xlsxPath)}, ${JSON.stringify(excelResavedPath)}
with zipfile.ZipFile(src, 'r') as zf:
    assert zf.testzip() is None
    for n in zf.namelist():
        ET.fromstring(zf.read(n))
    s1 = zf.read('xl/worksheets/sheet1.xml').decode('utf-8')
    assert 'My E2E Plan' in s1
    assert 'ySplit="4"' in s1
    assert '"ERG,SLOPE,RES"' in s1

# Build a realistic Excel-saved deflated .xlsx with sharedStrings + reordered columns + Duration + warnings
ss_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="11" uniqueCount="11">
  <si><t>Workout</t></si>
  <si><t>6Sec Deflated</t></si>
  <si><t>Target</t></si>
  <si><t>Mode</t></si>
  <si><t>Duration</t></si>
  <si><t>Block</t></si>
  <si><t>Repeat</t></si>
  <si><t>Warm-up</t></si>
  <si><t>power</t></si>
  <si><t>grade</t></si>
  <si><t>badmode</t></si>
</sst>'''
sheet_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
    <row r="4">
      <c r="A4" t="s"><v>2</v></c><c r="B4" t="s"><v>3</v></c><c r="C4" t="s"><v>4</v></c><c r="D4" t="s"><v>5</v></c><c r="E4" t="s"><v>6</v></c>
    </row>
    <row r="5">
      <c r="A5"><v>1800</v></c><c r="B5" t="s"><v>8</v></c><c r="C5" t="inlineStr"><is><t>2:30</t></is></c><c r="D5" t="s"><v>7</v></c><c r="E5"><v>2</v></c>
    </row>
    <row r="6">
      <c r="A6"><v>5</v></c><c r="B6" t="s"><v>9</v></c><c r="C6" t="inlineStr"><is><t>0:45</t></is></c>
    </row>
    <row r="7">
      <c r="A7"><v>100</v></c><c r="B7" t="s"><v>10</v></c><c r="C7" t="inlineStr"><is><t>1:00</t></is></c>
    </row>
  </sheetData>
</worksheet>'''
wb_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Workout" sheetId="1" r:id="rId1"/></sheets>
</workbook>'''
rels_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>'''
with zipfile.ZipFile(dst, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
    zout.writestr('xl/workbook.xml', wb_xml)
    zout.writestr('xl/_rels/workbook.xml.rels', rels_xml)
    zout.writestr('xl/sharedStrings.xml', ss_xml)
    zout.writestr('xl/worksheets/sheet1.xml', sheet_xml)
`]);

      // Upload deflated .xlsx into real browser
      await browser.uploadFile('#wkImportFile', excelResavedPath);
      await browser.waitFor(() => document.getElementById('wkNameInput').value === '6Sec Deflated', 4000, 'Deflated XLSX imported');
      const report = await browser.eval(() => document.getElementById('wkImportReport').textContent);
      assert.ok(report.includes('Row 5') && report.includes('1500'), 'Should report clamped target at Row 5: ' + report);
      assert.ok(report.includes('Row 7') && report.includes('badmode'), 'Should report skipped bad row at Row 7: ' + report);

      // Upload tab-delimited CSV
      const tsvPath = path.join(tmpDir, 'tab_workout.csv');
      fs.writeFileSync(tsvPath, 'Block\tRepeat\tMinutes\tSeconds\tMode\tTarget\nSprints\t3\t0\t30\tresistance\t85\n\t\t1\t0\tsim\t2\n');
      await browser.uploadFile('#wkImportFile', tsvPath);
      await browser.waitFor(() => document.getElementById('wkNameInput').value === 'tab_workout', 3000, 'Tab CSV imported');
      assert.ok((await browser.eval(() => document.getElementById('wkTotalMeta').textContent)).includes('6 steps'));

      // Upload old binary .xls -> clear "Save As .xlsx" error
      const xlsPath = path.join(tmpDir, 'legacy.xls');
      fs.writeFileSync(xlsPath, Buffer.from([0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1, 0x00, 0x00]));
      await browser.uploadFile('#wkImportFile', xlsPath);
      await browser.waitFor(() => document.getElementById('wkImportReport').textContent.includes('.xlsx'), 3000, 'Old .xls guidance');

      // Upload valid & invalid JSON workouts
      const validJsonPath = path.join(tmpDir, 'valid.json');
      fs.writeFileSync(validJsonPath, JSON.stringify({
        name: 'JSON Tempo',
        blocks: [{ name: 'Tempo', repeat: 2, steps: [{ dur: 60, mode: 'erg', target: 220 }] }],
      }));
      await browser.uploadFile('#wkImportFile', validJsonPath);
      await browser.waitFor(() => document.getElementById('wkNameInput').value === 'JSON Tempo', 3000, 'Valid JSON imported');

      const badJsonPath = path.join(tmpDir, 'invalid.json');
      fs.writeFileSync(badJsonPath, JSON.stringify({ name: 'Bad', blocks: [] }));
      await browser.uploadFile('#wkImportFile', badJsonPath);
      await browser.waitFor(() => document.getElementById('wkImportReport').classList.contains('err'), 3000, 'Invalid JSON error shown');
    });

    // 11. Stop Bridge Flow + 3 s Restart Polling & Auto-Reconnect
    await test('11. Stop bridge (pause workout, FTMS Stop 0x08, shutdown) & 3 s restart polling', async () => {
      // Connect FTMS trainer + HR strap + start workout
      await browser.eval(() => document.getElementById('btnDisconnect').click());
      await sleep(100);
      await browser.eval(() => {
        const rows = [...document.querySelectorAll('#savedDevicesList .saved-item')];
        rows.find(r => r.textContent.includes('Tacx Flux 46956')).querySelector('.btn-conn').click();
      });
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent.includes('FTMS'), 4000, 'FTMS connected');
      await browser.eval(() => {
        const rows = [...document.querySelectorAll('#savedDevicesList .saved-item')];
        rows.find(r => r.textContent.includes('Polar H10')).querySelector('.btn-conn').click();
      });
      await browser.waitFor(() => document.getElementById('hrHeaderPill').style.display !== 'none', 4000, 'HR connected');
      await browser.eval(() => document.getElementById('btnPlStart').click());

      bridge.writes.length = 0;
      bridge.ops.length = 0;

      // Click Stop bridge (CDP auto-accepts confirm dialog)
      await browser.eval(() => document.getElementById('btnStopBridge').click());
      await browser.waitFor(() => document.getElementById('transportPill').textContent.includes('Bridge stopped'), 5000, 'Bridge stopped pill');

      // Verify workout paused, FTMS Stop (0x08) sent, shutdown op sent
      assert.equal(await browser.eval(() => document.getElementById('btnPlPause').textContent.includes('Resume')), true, 'Workout must be paused');
      assert.ok(bridge.writes.some(w => w.uuid === FTMS_CP && w.bytes[0] === 0x08), 'FTMS Stop (0x08) must be sent before disconnect');
      assert.ok(bridge.ops.some(o => o.op === 'shutdown'), 'shutdown op must be sent');

      // Re-enable bridge -> within ~3.5 s page polls, reconnects bridge, and auto-connects ★ defaults!
      bridge.enableBridge = true;
      await browser.waitFor(() => document.getElementById('transportPill').textContent.includes('Bridge · in-app search'), 6000, 'Bridge reconnected via polling');
      await browser.waitFor(() => document.getElementById('trainerStatusText').textContent.includes('Tacx Flux 46956'), 6000, 'Trainer auto-reconnected after bridge restart');
      await browser.waitFor(() => document.getElementById('hrHeaderText').textContent.includes('Polar H10'), 6000, 'HR auto-reconnected after bridge restart');
    });

    // 12. Web Bluetooth Fallback & No-Bluetooth Banner
    await test('12. Web Bluetooth fallback & No-Bluetooth warning banner', async () => {
      bridge.enableBridge = false;
      // Inject a script that removes navigator.bluetooth before page load -> should show warning banner
      const scriptId1 = (await browser.send('Page.addScriptToEvaluateOnNewDocument', {
        source: 'Object.defineProperty(navigator, "bluetooth", { value: undefined, configurable: true });',
      })).identifier;
      await browser.goto(baseUrl);
      await browser.waitFor(() => document.getElementById('transportPill').textContent === 'No Bluetooth', 5000, 'No Bluetooth transport');
      assert.equal(await browser.eval(() => document.getElementById('globalBanner').classList.contains('show')), true, 'Warning banner must show when neither bridge nor Web Bluetooth is available');
      await browser.send('Page.removeScriptToEvaluateOnNewDocument', { identifier: scriptId1 });

      // Inject mock navigator.bluetooth -> should fall back to Web Bluetooth pill
      const scriptId2 = (await browser.send('Page.addScriptToEvaluateOnNewDocument', {
        source: `
          window.__bleRequests = [];
          Object.defineProperty(navigator, "bluetooth", {
            configurable: true,
            value: {
              requestDevice: async (opts) => { window.__bleRequests.push(opts); throw new Error("User cancelled"); }
            }
          });
        `,
      })).identifier;
      await browser.goto(baseUrl);
      await browser.waitFor(() => document.getElementById('transportPill').textContent === 'Web Bluetooth', 5000, 'Web Bluetooth fallback');
      await browser.eval(() => document.getElementById('btnSearchDevices').click());
      await browser.eval(() => document.getElementById('btnSearchHr').click());
      await sleep(100);
      const reqs = await browser.eval(() => window.__bleRequests);
      assert.equal(reqs.length, 2, 'Expected trainer + HR requestDevice calls');
      assert.ok(reqs[0].filters.some(f => f.namePrefix === 'Tacx'), 'Trainer filter must include namePrefix Tacx');
      assert.ok(reqs[0].filters.some(f => f.services && f.services.includes(0x1826)), 'Trainer filter must include FTMS 0x1826');
      assert.ok(reqs[0].filters.some(f => f.services && f.services.includes(FEC_SVC)), 'Trainer filter must include FE-C UUID');
      assert.ok(reqs[1].filters.some(f => f.services && f.services.includes(0x180D)), 'HR filter must include 0x180D');
      await browser.send('Page.removeScriptToEvaluateOnNewDocument', { identifier: scriptId2 });
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
