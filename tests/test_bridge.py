#!/usr/bin/env python3
"""
Independent E2E test suite for `flux_bridge.py` (v8).

Exercises the real `flux_bridge.py` over live localhost TCP sockets using
isolated temporary directories for config, workouts, rides, and logs so
user data is never touched.

Covers:
  - HTTP serving (/ -> /flux-s.html, /ping CORS, Cache-Control: no-store)
  - Origin security (/ws 403 on missing/foreign Origin; POST /shutdown header+origin)
  - port_owner() ('bridge', 'other', None) and stop_running_bridge()
  - Single-tab ownership (claim=takeover, claim=ifFree, PASSIVE_OPS, takenOver, op=claim)
  - File persistence: flux_config.json (configLoad/configSave), Workouts CRUD,
    Rides continuous append, conflict detection, replace, rideLoad, rideList,
    and finalize_stale_rides() NP/IF/TSS auto-finalization
  - clientLog forwarding to rotating log file
  - BLE scanning + 0.7 s throttling + simultaneous trainer & HR device ops
  - 3-attempt connect retries, non-empty TimeoutError text + RSSI hint,
    device-not-found, Windows _connect_direct fallback, cancel_connect, and
    shutdown BLE disconnect cleanup.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import http.server
import json
import os
import socket
import struct
import sys
import tempfile
import threading
import time
import types
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Lightweight RFC 6455 WebSocket client (stdlib only)
# ---------------------------------------------------------------------------
class SimpleWSClient:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.sock.settimeout(5.0)
        self._buf = bytearray()

    @classmethod
    def connect(cls, host: str, port: int, path: str, origin: str | None = None):
        sock = socket.create_connection((host, port), timeout=5.0)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        lines = [
            f"GET {path} HTTP/1.1",
            f"Host: {host}:{port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
        ]
        if origin is not None:
            lines.append(f"Origin: {origin}")
        sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("utf-8"))

        resp = bytearray()
        while b"\r\n\r\n" not in resp:
            chunk = sock.recv(4096)
            if not chunk:
                break
            resp.extend(chunk)
        header_end = resp.index(b"\r\n\r\n")
        head = resp[:header_end].decode("utf-8", errors="replace")
        status_code = int(head.split("\r\n", 1)[0].split(" ")[1])
        if status_code != 101:
            body = resp[header_end + 4:].decode("utf-8", errors="replace")
            sock.close()
            raise RuntimeError(f"WS handshake failed: HTTP {status_code} {body}")
        client = cls(sock)
        client._buf.extend(resp[header_end + 4:])
        return client

    def _recv_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise EOFError("WebSocket closed")
            self._buf.extend(chunk)
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out

    def send_json(self, obj: dict) -> None:
        payload = json.dumps(obj).encode("utf-8")
        mask = os.urandom(4)
        header = bytearray([0x81])
        ln = len(payload)
        if ln < 126:
            header.append(0x80 | ln)
        elif ln < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", ln))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", ln))
        header.extend(mask)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + masked)

    def recv_json(self, timeout: float = 5.0) -> dict:
        self.sock.settimeout(timeout)
        while True:
            b1, b2 = self._recv_exact(2)
            opcode = b1 & 0x0F
            masked = (b2 & 0x80) != 0
            ln = b2 & 0x7F
            if ln == 126:
                ln = struct.unpack("!H", self._recv_exact(2))[0]
            elif ln == 127:
                ln = struct.unpack("!Q", self._recv_exact(8))[0]
            mask = self._recv_exact(4) if masked else None
            data = self._recv_exact(ln)
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            if opcode == 0x8:
                raise EOFError("WS close frame")
            if opcode == 0x9:
                continue
            if opcode == 0x1:
                return json.loads(data.decode("utf-8"))

    def request(self, op: str, req_id: int, **kwargs) -> tuple[dict, list[dict]]:
        self.send_json({"id": req_id, "op": op, **kwargs})
        events = []
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            msg = self.recv_json(timeout=max(0.2, deadline - time.monotonic()))
            if msg.get("id") == req_id:
                return msg, events
            events.append(msg)
        raise TimeoutError(f"Timed out waiting for response to {op}")

    def close(self) -> None:
        try:
            self.sock.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Fake Bleak (controllable retries, direct-connect backend, multi-device)
# ---------------------------------------------------------------------------
class FakeBLEDevice:
    def __init__(self, address: str, name: str):
        self.address = address
        self.name = name


class FakeAdvData:
    def __init__(self, local_name: str, rssi: int, service_uuids: list[str]):
        self.local_name = local_name
        self.rssi = rssi
        self.service_uuids = service_uuids


class FakeChar:
    def __init__(self, uuid: str, properties: list[str]):
        self.uuid = uuid
        self.properties = properties


class FakeService:
    def __init__(self, uuid: str, chars: list[FakeChar]):
        self.uuid = uuid
        self.characteristics = chars


class FakeBleakState:
    def __init__(self):
        self.reset()

    def reset(self):
        self.devices: dict[str, tuple[FakeBLEDevice, FakeAdvData, list[FakeService]]] = {}
        self.fail_attempts: dict[str, list[Exception | None]] = {}
        self.find_returns_none: set[str] = set()
        self.direct_connect_allowed: set[str] = set()
        self.scanner_active = False
        self.scanner_cb = None
        self.connected_clients: list["FakeBleakClient"] = []
        self.writes: list[tuple[str, str, bytes, bool]] = []
        self.reads: dict[tuple[str, str], bytes] = {}
        self.notify_cbs: dict[tuple[str, str], object] = {}
        self.client_calls: list[dict] = []
        self.find_calls: list[str] = []
        self.active_connects = 0
        self.max_concurrent_connects = 0


FAKE_BLE = FakeBleakState()


class FakeBleakScanner:
    def __init__(self, detection_callback=None):
        self._cb = detection_callback

    async def start(self):
        FAKE_BLE.scanner_active = True
        FAKE_BLE.scanner_cb = self._cb

    async def stop(self):
        FAKE_BLE.scanner_active = False
        FAKE_BLE.scanner_cb = None

    @classmethod
    async def find_device_by_address(cls, addr: str, timeout: float = 10.0):
        FAKE_BLE.find_calls.append(addr)
        await asyncio.sleep(0.02)
        if addr in FAKE_BLE.find_returns_none:
            return None
        entry = FAKE_BLE.devices.get(addr)
        return entry[0] if entry else None


class FakeWinBackend:
    def __init__(self):
        self._device_info = None


class FakeBleakClient:
    def __init__(self, target, disconnected_callback=None, timeout: float = 20.0, winrt=None):
        self.address = target.address if hasattr(target, "address") else str(target)
        self._is_raw_addr = isinstance(target, str)
        self._disc_cb = disconnected_callback
        self.timeout = timeout
        self.winrt = winrt
        self.is_connected = False
        self.services: list[FakeService] = []
        self._backend = FakeWinBackend() if self.address in FAKE_BLE.direct_connect_allowed else None
        FAKE_BLE.client_calls.append({"address": self.address, "winrt": winrt, "raw": self._is_raw_addr})

    async def connect(self):
        FAKE_BLE.active_connects += 1
        FAKE_BLE.max_concurrent_connects = max(FAKE_BLE.max_concurrent_connects, FAKE_BLE.active_connects)
        try:
            await asyncio.sleep(0.02)
            if self._is_raw_addr and self.address not in FAKE_BLE.direct_connect_allowed:
                raise RuntimeError("direct connect unavailable")
            plan = FAKE_BLE.fail_attempts.get(self.address)
            if plan:
                err = plan.pop(0)
                if err is not None:
                    raise err
            entry = FAKE_BLE.devices.get(self.address)
            self.services = entry[2] if entry else []
            self.is_connected = True
            FAKE_BLE.connected_clients.append(self)
        finally:
            FAKE_BLE.active_connects -= 1

    async def disconnect(self):
        was = self.is_connected
        self.is_connected = False
        if self in FAKE_BLE.connected_clients:
            FAKE_BLE.connected_clients.remove(self)
        if was and self._disc_cb:
            self._disc_cb(self)

    def simulate_drop(self):
        self.is_connected = False
        if self in FAKE_BLE.connected_clients:
            FAKE_BLE.connected_clients.remove(self)
        if self._disc_cb:
            self._disc_cb(self)

    async def read_gatt_char(self, uuid: str) -> bytearray:
        return bytearray(FAKE_BLE.reads.get((self.address, uuid.lower()), b"\x00"))

    async def write_gatt_char(self, uuid: str, data: bytes, response: bool = False):
        FAKE_BLE.writes.append((self.address, uuid.lower(), bytes(data), response))

    async def start_notify(self, uuid: str, cb):
        FAKE_BLE.notify_cbs[(self.address, uuid.lower())] = cb


# ---------------------------------------------------------------------------
# Stdlib aiohttp shim (used only when aiohttp is not installed locally)
# ---------------------------------------------------------------------------
def _install_aiohttp_shim_if_needed():
    try:
        import aiohttp  # noqa: F401
        return False
    except ImportError:
        pass

    aio = types.ModuleType("aiohttp")
    web_mod = types.ModuleType("aiohttp.web")

    class WSMsgType:
        TEXT = 1
        BINARY = 2
        CLOSE = 8
        ERROR = 256

    class WSMessage:
        def __init__(self, mtype, data):
            self.type = mtype
            self.data = data

    class HTTPException(Exception):
        status = 500
        def __init__(self, text="", location=None):
            super().__init__(text)
            self.text = text
            self.location = location
            self.headers = {}

    class HTTPForbidden(HTTPException):
        status = 403

    class HTTPFound(HTTPException):
        status = 302
        def __init__(self, location):
            super().__init__("", location=location)
            self.headers = {"Location": location}

    class Response:
        def __init__(self, status=200, text="", body=None, content_type="text/plain", headers=None):
            self.status = status
            self.body = body if body is not None else text.encode("utf-8")
            self.headers = {"Content-Type": content_type}
            if headers:
                self.headers.update(headers)

    def json_response(data, status=200, headers=None):
        return Response(status=status, text=json.dumps(data), content_type="application/json", headers=headers)

    class WebSocketResponse:
        def __init__(self, heartbeat=20):
            self.heartbeat = heartbeat
            self.closed = False
            self._reader = None
            self._writer = None
            self.headers = {}

        async def prepare(self, request):
            self._reader = request._reader
            self._writer = request._writer
            ws_key = request.headers.get("Sec-WebSocket-Key", "")
            accept = base64.b64encode(
                hashlib.sha1((ws_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
            ).decode("ascii")
            self._writer.write((
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            ).encode("utf-8"))
            await self._writer.drain()
            request._ws_upgraded = True
            return self

        async def send_json(self, obj):
            if self.closed or not self._writer:
                return
            data = json.dumps(obj).encode("utf-8")
            hdr = bytearray([0x81])
            ln = len(data)
            if ln < 126:
                hdr.append(ln)
            elif ln < 65536:
                hdr.append(126)
                hdr.extend(struct.pack("!H", ln))
            else:
                hdr.append(127)
                hdr.extend(struct.pack("!Q", ln))
            self._writer.write(bytes(hdr) + data)
            await self._writer.drain()

        async def close(self):
            if self.closed:
                return
            self.closed = True
            if self._writer:
                try:
                    self._writer.write(b"\x88\x00")
                    await self._writer.drain()
                    self._writer.close()
                    await self._writer.wait_closed()
                except Exception:
                    pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.closed:
                raise StopAsyncIteration
            try:
                b1, b2 = await self._reader.readexactly(2)
            except Exception:
                self.closed = True
                raise StopAsyncIteration
            opcode = b1 & 0x0F
            masked = (b2 & 0x80) != 0
            ln = b2 & 0x7F
            if ln == 126:
                ln = struct.unpack("!H", await self._reader.readexactly(2))[0]
            elif ln == 127:
                ln = struct.unpack("!Q", await self._reader.readexactly(8))[0]
            mask = await self._reader.readexactly(4) if masked else None
            payload = await self._reader.readexactly(ln)
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 8:
                self.closed = True
                raise StopAsyncIteration
            if opcode == 1:
                return WSMessage(WSMsgType.TEXT, payload.decode("utf-8"))
            return WSMessage(WSMsgType.BINARY, payload)

    class AppKey(str):
        def __new__(cls, name, _t=None):
            return super().__new__(cls, name)

    class Router:
        def __init__(self):
            self.routes = []
            self.statics = []
        def add_get(self, path, handler):
            self.routes.append(("GET", path, handler))
        def add_post(self, path, handler):
            self.routes.append(("POST", path, handler))
        def add_static(self, prefix, root, show_index=False):
            self.statics.append((prefix, Path(root)))

    class Request:
        def __init__(self, app, method, path, headers, reader, writer):
            self.app = app
            self.method = method
            self.path = path
            self.headers = headers
            self._reader = reader
            self._writer = writer
            self._ws_upgraded = False

    class Application(dict):
        def __init__(self, middlewares=()):
            super().__init__()
            self.middlewares = list(middlewares)
            self.on_shutdown = []
            self.router = Router()

    def middleware(fn):
        return fn

    class AppRunner:
        def __init__(self, app):
            self.app = app
            self._servers = []
        async def setup(self):
            pass
        async def cleanup(self):
            for cb in self.app.on_shutdown:
                await cb(self.app)
            for srv in self._servers:
                srv.close()
                await srv.wait_closed()
            self._servers.clear()

    class TCPSite:
        def __init__(self, runner, host, port):
            self.runner = runner
            self.host = host
            self.port = port
        async def start(self):
            app = self.runner.app
            async def handle_client(reader, writer):
                try:
                    raw = await reader.readuntil(b"\r\n\r\n")
                except Exception:
                    writer.close()
                    return
                lines = raw.decode("utf-8", errors="replace").split("\r\n")
                parts = lines[0].split(" ")
                if len(parts) < 2:
                    writer.close()
                    return
                method, path = parts[0], parts[1].split("?", 1)[0]
                headers = {}
                for ln in lines[1:]:
                    if ":" in ln:
                        k, v = ln.split(":", 1)
                        headers[k.strip()] = v.strip()
                cl = int(headers.get("Content-Length", "0") or "0")
                if cl > 0:
                    await reader.readexactly(cl)
                req = Request(app, method, path, headers, reader, writer)

                async def base_handler(r):
                    for m, p, h in app.router.routes:
                        if m == r.method and p == r.path:
                            return await h(r)
                    if r.method == "GET":
                        for pref, root in app.router.statics:
                            if r.path.startswith(pref):
                                rel = r.path[len(pref):].lstrip("/")
                                fpath = (root / rel).resolve()
                                if fpath.is_file() and str(fpath).startswith(str(root.resolve())):
                                    ct = "text/html; charset=utf-8" if fpath.suffix == ".html" else "application/octet-stream"
                                    return Response(200, body=fpath.read_bytes(), content_type=ct)
                    return Response(404, text="Not found")

                handler = base_handler
                for mw in reversed(app.middlewares):
                    nxt = handler
                    async def wrapped(r, _mw=mw, _nxt=nxt):
                        return await _mw(r, _nxt)
                    handler = wrapped

                try:
                    resp = await handler(req)
                except HTTPException as exc:
                    resp = Response(status=exc.status, text=exc.text, headers=exc.headers)

                if not req._ws_upgraded:
                    body = getattr(resp, "body", b"")
                    hdrs = dict(getattr(resp, "headers", {}))
                    hdrs["Content-Length"] = str(len(body))
                    head = f"HTTP/1.1 {resp.status} OK\r\n" + "".join(f"{k}: {v}\r\n" for k, v in hdrs.items()) + "\r\n"
                    writer.write(head.encode("utf-8") + body)
                    await writer.drain()
                    writer.close()

            srv = await asyncio.start_server(handle_client, self.host, self.port)
            self.runner._servers.append(srv)

    web_mod.Application = Application
    web_mod.AppKey = AppKey
    web_mod.AppRunner = AppRunner
    web_mod.TCPSite = TCPSite
    web_mod.WebSocketResponse = WebSocketResponse
    web_mod.Response = Response
    web_mod.json_response = json_response
    web_mod.HTTPForbidden = HTTPForbidden
    web_mod.HTTPFound = HTTPFound
    web_mod.middleware = middleware

    aio.WSMsgType = WSMsgType
    aio.web = web_mod
    sys.modules["aiohttp"] = aio
    sys.modules["aiohttp.web"] = web_mod
    return True


_install_aiohttp_shim_if_needed()

_bleak_mod = types.ModuleType("bleak")
_bleak_mod.BleakClient = FakeBleakClient
_bleak_mod.BleakScanner = FakeBleakScanner
sys.modules["bleak"] = _bleak_mod

import flux_bridge  # noqa: E402


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class BridgeRunnerThread:
    def __init__(self, port: int):
        self.port = port
        self.app = None
        self.loop = None
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.app = flux_bridge.make_app(self.port)

        async def _boot():
            runner = flux_bridge.web.AppRunner(self.app)
            await runner.setup()
            await flux_bridge.web.TCPSite(runner, "127.0.0.1", self.port).start()
            self.ready.set()
            try:
                await self.app[flux_bridge.STOP].wait()
            finally:
                await runner.cleanup()

        self.loop.run_until_complete(_boot())
        pending = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        self.loop.close()

    def start(self):
        self.thread.start()
        assert self.ready.wait(5.0), "Bridge failed to start"

    def stop(self):
        if self.loop and self.app and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.app[flux_bridge.STOP].set)
        self.thread.join(timeout=5.0)


class TestFluxBridgeV8E2E(unittest.TestCase):
    def setUp(self):
        FAKE_BLE.reset()
        self.tmp = tempfile.TemporaryDirectory(prefix="flux-bridge-test-")
        tmp_path = Path(self.tmp.name)
        self._orig_dirs = (
            flux_bridge.CONFIG_FILE,
            flux_bridge.WORKOUT_DIR,
            flux_bridge.RIDE_DIR,
            flux_bridge.LOG_DIR,
        )
        flux_bridge.CONFIG_FILE = tmp_path / "flux_config.json"
        flux_bridge.WORKOUT_DIR = tmp_path / "Workouts"
        flux_bridge.RIDE_DIR = tmp_path / "Rides"
        flux_bridge.LOG_DIR = tmp_path / "Logs"
        self._orig_timings = (flux_bridge.RETRY_SETTLE, flux_bridge.SCAN_STOP_SETTLE, flux_bridge.BLE_CACHE_TTL)
        flux_bridge.RETRY_SETTLE = 0.05
        flux_bridge.SCAN_STOP_SETTLE = 0.02
        flux_bridge._ride_stats_cache.clear()

        self.port = _free_port()
        self.bridge = BridgeRunnerThread(self.port)
        self.bridge.start()

        trainer_svcs = [
            FakeService(flux_bridge.FTMS_SVC, [
                FakeChar("00002ad2-0000-1000-8000-00805f9b34fb", ["notify"]),
                FakeChar("00002ad6-0000-1000-8000-00805f9b34fb", ["read"]),
                FakeChar("00002ad9-0000-1000-8000-00805f9b34fb", ["write", "indicate"]),
            ])
        ]
        hr_svcs = [
            FakeService(flux_bridge.HR_SVC, [
                FakeChar("00002a37-0000-1000-8000-00805f9b34fb", ["notify"]),
            ]),
            FakeService("0000180f-0000-1000-8000-00805f9b34fb", [
                FakeChar("00002a19-0000-1000-8000-00805f9b34fb", ["read"]),
            ]),
        ]
        self.trainer_addr = "FA:C5:9A:FD:A5:45"
        self.hr_addr = "CC:B2:7A:17:E6:AF"
        FAKE_BLE.devices[self.trainer_addr] = (
            FakeBLEDevice(self.trainer_addr, "Tacx Flux 46956"),
            FakeAdvData("Tacx Flux 46956", -61, [flux_bridge.FTMS_SVC]),
            trainer_svcs,
        )
        FAKE_BLE.devices[self.hr_addr] = (
            FakeBLEDevice(self.hr_addr, "Polar H10"),
            FakeAdvData("Polar H10", -58, [flux_bridge.HR_SVC]),
            hr_svcs,
        )
        FAKE_BLE.reads[(self.trainer_addr, "00002ad6-0000-1000-8000-00805f9b34fb")] = b"\x00\x00\x64\x00\x01\x00"
        FAKE_BLE.reads[(self.hr_addr, "00002a19-0000-1000-8000-00805f9b34fb")] = bytes([88])

    def tearDown(self):
        self.bridge.stop()
        flux_bridge.CONFIG_FILE, flux_bridge.WORKOUT_DIR, flux_bridge.RIDE_DIR, flux_bridge.LOG_DIR = self._orig_dirs
        flux_bridge.RETRY_SETTLE, flux_bridge.SCAN_STOP_SETTLE, flux_bridge.BLE_CACHE_TTL = self._orig_timings
        self.tmp.cleanup()

    def _connect_ws(self, origin=None):
        return SimpleWSClient.connect("127.0.0.1", self.port, "/ws", origin=origin or f"http://127.0.0.1:{self.port}")

    def test_http_ping_and_origin_security(self):
        # /flux-s.html
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/flux-s.html", timeout=3) as r:
            self.assertEqual(r.status, 200)
            self.assertEqual(r.headers.get("Cache-Control"), "no-store")
            self.assertIn(b"Tacx Flux S", r.read(2048))

        # /ping has CORS * for file:// redirect
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/ping", timeout=3) as r:
            self.assertEqual(r.status, 200)
            self.assertEqual(r.headers.get("Access-Control-Allow-Origin"), "*")
            self.assertEqual(json.loads(r.read())["app"], "flux-bridge")

        # port_owner
        self.assertEqual(flux_bridge.port_owner(self.port), "bridge")
        self.assertIsNone(flux_bridge.port_owner(_free_port()))

        # /ws rejects missing/foreign Origin
        for bad_orig in (None, "https://evil.example"):
            with self.assertRaises(RuntimeError) as ctx:
                SimpleWSClient.connect("127.0.0.1", self.port, "/ws", origin=bad_orig)
            self.assertIn("403", str(ctx.exception))

        # POST /shutdown rejects missing header and foreign origin
        with self.assertRaises(urllib.error.HTTPError) as e1:
            urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{self.port}/shutdown", data=b"", method="POST"), timeout=3)
        self.assertEqual(e1.exception.code, 403)

        with self.assertRaises(urllib.error.HTTPError) as e2:
            urllib.request.urlopen(urllib.request.Request(
                f"http://127.0.0.1:{self.port}/shutdown", data=b"", method="POST",
                headers={"X-Flux-Bridge": "shutdown", "Origin": "https://evil.example"},
            ), timeout=3)
        self.assertEqual(e2.exception.code, 403)

    def test_port_owner_other_program(self):
        other_port = _free_port()
        class Quiet(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200); self.end_headers(); self.wfile.write(b"other")
            def log_message(self, *_a): pass
        srv = http.server.HTTPServer(("127.0.0.1", other_port), Quiet)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            self.assertEqual(flux_bridge.port_owner(other_port), "other")
        finally:
            srv.shutdown(); srv.server_close()

    def test_single_tab_ownership_and_takeover(self):
        ws1 = self._connect_ws()
        ws2 = self._connect_ws()
        try:
            # Tab 1 claims active ownership and connects trainer
            r1, _ = ws1.request("hello", 1, claim="takeover")
            self.assertTrue(r1["active"])
            self.assertEqual(r1["version"], flux_bridge.BRIDGE_VERSION)
            ws1.request("connect", 2, address=self.trainer_addr)
            self.assertEqual(len(FAKE_BLE.connected_clients), 1)

            # Tab 2 reconnects with claim=ifFree -> stays passive!
            r2, _ = ws2.request("hello", 1, claim="ifFree")
            self.assertFalse(r2["active"])
            self.assertTrue(r2["busy"])

            # Passive tab cannot scan or connect, but CAN use PASSIVE_OPS (configLoad, clientLog)
            r_scan, _ = ws2.request("scanStart", 2)
            self.assertFalse(r_scan["ok"])
            self.assertIn("another tab", r_scan["error"])
            r_cfg, _ = ws2.request("configLoad", 3)
            self.assertTrue(r_cfg["ok"])
            r_log, _ = ws2.request("clientLog", 4, level="i", text="hello from passive tab")
            self.assertTrue(r_log["ok"])

            # Tab 2 clicks "Use this tab" (op=claim) -> Tab 1 gets takenOver and BLE is released!
            r_claim, _ = ws2.request("claim", 5)
            self.assertTrue(r_claim["ok"] and r_claim["active"])
            ev_tab1 = ws1.recv_json()
            self.assertEqual(ev_tab1["ev"], "takenOver")
            self.assertEqual(len(FAKE_BLE.connected_clients), 0)

            # Now Tab 2 can scan
            r_scan2, _ = ws2.request("scanStart", 6)
            self.assertTrue(r_scan2["ok"])
        finally:
            ws1.close(); ws2.close()

    def test_config_workouts_and_rides_persistence_with_stale_finalization(self):
        ws = self._connect_ws()
        try:
            ws.request("hello", 1, claim="takeover")

            # Config save/load
            c0, _ = ws.request("configLoad", 2)
            self.assertFalse(c0["exists"])
            ws.request("configSave", 3, config={"ftp": 240, "riderKg": 72})
            c1, _ = ws.request("configLoad", 4)
            self.assertTrue(c1["exists"])
            self.assertEqual(c1["config"]["ftp"], 240)

            # Workouts save/list/load/delete
            wo = {"name": "Sweet Spot", "blocks": [{"name": "SS", "repeat": 2, "steps": [{"dur": 600, "mode": "erg", "target": 215}]}]}
            ws.request("workoutSave", 5, workout=wo)
            wl, _ = ws.request("workoutList", 6)
            self.assertEqual(wl["workouts"], ["Sweet Spot"])
            wload, _ = ws.request("workoutLoad", 7, name="Sweet Spot")
            self.assertEqual(wload["workout"]["blocks"][0]["steps"][0]["target"], 215)
            ws.request("workoutDelete", 8, name="Sweet Spot")
            wl2, _ = ws.request("workoutList", 9)
            self.assertEqual(wl2["workouts"], [])

            # Rides append + conflict check + replace + rideLoad + stale auto-finalization (NP/IF/TSS)
            lines = [f"{i},{200 + (i % 20)},90,32.5,145,{i * 9},erg,210," for i in range(1, 41)]
            r_app, _ = ws.request("rideAppend", 10, ride="2026-11-25_120000", first=True, lines=lines, meta={"ftp": 200, "startedAt": "2026-11-25T12:00:00Z"})
            self.assertTrue(r_app["ok"])

            # Same ride id with first=True returns conflict=True (never mixes two rides!)
            r_conf, _ = ws.request("rideAppend", 11, ride="2026-11-25_120000", first=True, lines=["1,100,80,25,120,7,erg,100,"])
            self.assertTrue(r_conf.get("conflict"))

            # Backdate CSV mtime > STALE_RIDE_SEC so rideList triggers finalize_stale_rides()
            csv_p = flux_bridge.RIDE_DIR / "2026-11-25_120000.csv"
            old_t = time.time() - flux_bridge.STALE_RIDE_SEC - 60
            os.utime(csv_p, (old_t, old_t))

            rl, _ = ws.request("rideList", 12)
            self.assertEqual(len(rl["rides"]), 1)
            ride_entry = rl["rides"][0]
            self.assertEqual(ride_entry["seconds"], 40)
            self.assertIsNotNone(ride_entry["np"])
            self.assertTrue(ride_entry["meta"].get("autoFinalized"))
            self.assertIsNotNone(ride_entry["meta"].get("IF"))
            self.assertIsNotNone(ride_entry["meta"].get("tss"))

            rload, _ = ws.request("rideLoad", 13, ride="2026-11-25_120000")
            self.assertIn("200,90,32.5,145", rload["csv"])
        finally:
            ws.close()

    def test_scan_multi_device_retries_and_direct_connect(self):
        self.assertEqual(flux_bridge.describe_error(TimeoutError()), "timed out")
        ws = self._connect_ws()
        try:
            ws.request("hello", 1, claim="takeover")
            ws.request("scanStart", 2)
            tr_dev, tr_adv, _ = FAKE_BLE.devices[self.trainer_addr]
            hr_dev, hr_adv, _ = FAKE_BLE.devices[self.hr_addr]
            FAKE_BLE.scanner_cb(tr_dev, tr_adv)
            FAKE_BLE.scanner_cb(tr_dev, FakeAdvData("Tacx Flux 46956", -63, [flux_bridge.FTMS_SVC]))  # throttled
            FAKE_BLE.scanner_cb(hr_dev, hr_adv)
            ev1, ev2 = ws.recv_json(), ws.recv_json()
            self.assertEqual({ev1["device"]["address"], ev2["device"]["address"]}, {self.trainer_addr, self.hr_addr})

            # Retry succeeds on attempt 2 after attempt 1 TimeoutError
            FAKE_BLE.fail_attempts[self.trainer_addr] = [asyncio.TimeoutError(), None]
            res, events = ws.request("connect", 3, address=self.trainer_addr)
            self.assertTrue(res["ok"])
            self.assertFalse(FAKE_BLE.scanner_active)
            prog = [e["text"] for e in events if e.get("ev") == "progress"]
            self.assertTrue(any("attempt 2/3" in p for p in prog))

            # Connect HR strap simultaneously + read/write/subscribe/notify/drop
            ws.request("connect", 4, address=self.hr_addr)
            self.assertEqual(len(FAKE_BLE.connected_clients), 2)
            r_batt, _ = ws.request("read", 5, address=self.hr_addr, uuid="00002a19-0000-1000-8000-00805f9b34fb")
            self.assertEqual(r_batt["data"], "58")
            ws.request("write", 6, address=self.trainer_addr, uuid="00002ad9-0000-1000-8000-00805f9b34fb", data="05c800", response=True)
            self.assertEqual(FAKE_BLE.writes[-1][2], b"\x05\xc8\x00")
            ws.request("subscribe", 7, address=self.hr_addr, uuid="00002a37-0000-1000-8000-00805f9b34fb")
            FAKE_BLE.notify_cbs[(self.hr_addr, "00002a37-0000-1000-8000-00805f9b34fb")](None, bytearray([0x06, 150]))
            self.assertEqual(ws.recv_json()["data"], "0696")

            # Drop HR -> disconnected event while trainer stays up
            [c for c in FAKE_BLE.connected_clients if c.address == self.hr_addr][0].simulate_drop()
            self.assertEqual(ws.recv_json(), {"ev": "disconnected", "address": self.hr_addr})
            ws.request("disconnect", 8, address=self.trainer_addr)

            # Windows direct-connect fallback when find_device_by_address returns None
            direct_addr = "AA:BB:CC:DD:EE:01"
            FAKE_BLE.devices[direct_addr] = (
                FakeBLEDevice(direct_addr, "Shared Trainer"),
                FakeAdvData("Shared Trainer", -60, [flux_bridge.FTMS_SVC]),
                FAKE_BLE.devices[self.trainer_addr][2],
            )
            FAKE_BLE.find_returns_none.add(direct_addr)
            FAKE_BLE.direct_connect_allowed.add(direct_addr)
            res_dir, evs_dir = ws.request("connect", 9, address=direct_addr)
            self.assertTrue(res_dir["ok"])
            self.assertTrue(any("direct connection" in e.get("text", "") for e in evs_dir if e.get("ev") == "progress"))
            ws.request("disconnect", 10, address=direct_addr)

            # 3-attempt timeout reports readable error + RSSI
            FAKE_BLE.fail_attempts[self.trainer_addr] = [TimeoutError(), TimeoutError(), TimeoutError()]
            r_fail, _ = ws.request("connect", 11, address=self.trainer_addr)
            self.assertFalse(r_fail["ok"])
            self.assertIn("timed out", r_fail["error"])
            self.assertIn("-63 dBm", r_fail["error"])
        finally:
            ws.close()

    def test_cancel_connect_filename_collisions_and_ride_id_security(self):
        ws = self._connect_ws()
        try:
            ws.request("hello", 1, claim="takeover")

            # 1. Path traversal / invalid ride_id must be rejected on both rideAppend & rideLoad
            for bad_id in ("../escape", "a/b", "ride with spaces", ""):
                r1, _ = ws.request("rideAppend", 10, ride=bad_id, lines=["0,100"])
                self.assertFalse(r1["ok"])
                self.assertIn("invalid ride id", r1["error"])
                r2, _ = ws.request("rideLoad", 11, ride=bad_id)
                self.assertFalse(r2["ok"])
                self.assertIn("invalid ride id", r2["error"])

            # 2. Workout filename collision (different names sanitizing to same stem)
            w1, _ = ws.request("workoutSave", 12, workout={"name": "Sprint/1", "blocks": []})
            w2, _ = ws.request("workoutSave", 13, workout={"name": "Sprint:1", "blocks": []})
            self.assertTrue(w1["ok"] and w2["ok"])
            self.assertNotEqual(w1["file"], w2["file"])
            lst, _ = ws.request("workoutList", 14)
            self.assertEqual(set(lst["workouts"]), {"Sprint/1", "Sprint:1"})
            loaded2, _ = ws.request("workoutLoad", 15, name="Sprint:1")
            self.assertEqual(loaded2["workout"]["name"], "Sprint:1")

            # 3. In-flight connect cancelled cleanly by disconnect
            slow_addr = "AA:BB:CC:DD:EE:99"
            FAKE_BLE.devices[slow_addr] = (
                FakeBLEDevice(slow_addr, "Slow Trainer"),
                FakeAdvData("Slow Trainer", -70, [flux_bridge.FTMS_SVC]),
                FAKE_BLE.devices[self.trainer_addr][2],
            )
            orig_connect = FakeBleakClient.connect

            async def slow_connect(cli):
                await asyncio.sleep(5)
                return await orig_connect(cli)

            FakeBleakClient.connect = slow_connect
            try:
                ws.send_json({"id": 20, "op": "connect", "address": slow_addr})
                # Wait briefly so connect enters awaiting slow_connect
                time.sleep(0.15)
                ws.send_json({"id": 21, "op": "disconnect", "address": slow_addr})
                replies = {}
                deadline = time.time() + 4
                while len(replies) < 2 and time.time() < deadline:
                    m = ws.recv_json(timeout=2)
                    if "id" in m:
                        replies[m["id"]] = m
                self.assertIn(20, replies)
                self.assertIn(21, replies)
                self.assertFalse(replies[20]["ok"])
                self.assertIn("cancelled", replies[20]["error"])
                self.assertTrue(replies[21]["ok"])
            finally:
                FakeBleakClient.connect = orig_connect
        finally:
            ws.close()

    def test_connectivity_hardening_uncached_retry_ttl_and_serialization(self):
        ws = self._connect_ws()
        try:
            ws.request("hello", 1, claim="takeover")

            # 1. Stale BLE cache (> BLE_CACHE_TTL) must force fresh scan instead of reusing stale handle
            flux_bridge.BLE_CACHE_TTL = 0.05
            ws.request("scanStart", 2)
            tr_dev, tr_adv, _ = FAKE_BLE.devices[self.trainer_addr]
            FAKE_BLE.scanner_cb(tr_dev, tr_adv)
            ws.recv_json()  # adv
            time.sleep(0.08)  # expire cache
            FAKE_BLE.find_calls.clear()
            FAKE_BLE.client_calls.clear()

            # 2. Attempt 1 fails -> attempt 2 bypasses Windows GATT cache (use_cached_services=False) and succeeds
            FAKE_BLE.fail_attempts[self.trainer_addr] = [asyncio.TimeoutError(), None]
            res, evs = ws.request("connect", 3, address=self.trainer_addr)
            self.assertTrue(res["ok"])
            self.assertGreaterEqual(FAKE_BLE.find_calls.count(self.trainer_addr), 2)
            self.assertIsNone(FAKE_BLE.client_calls[0]["winrt"])
            self.assertEqual(FAKE_BLE.client_calls[1]["winrt"], {"use_cached_services": False})
            self.assertTrue(any("fresh GATT" in e.get("text", "") for e in evs if e.get("ev") == "progress"))

            # 3. Subsequent reconnect remembers uncached_addrs and immediately skips poisoned GATT cache on attempt 1
            ws.request("disconnect", 4, address=self.trainer_addr)
            FAKE_BLE.client_calls.clear()
            res2, _ = ws.request("connect", 5, address=self.trainer_addr)
            self.assertTrue(res2["ok"])
            self.assertEqual(FAKE_BLE.client_calls[0]["winrt"], {"use_cached_services": False})
            ws.request("disconnect", 6, address=self.trainer_addr)

            # 4. Concurrent connect requests (trainer + HR) must be serialized by _ble_lock (max concurrency == 1)
            FAKE_BLE.max_concurrent_connects = 0
            orig_connect = FakeBleakClient.connect

            async def slower_connect(cli):
                FAKE_BLE.active_connects += 1
                FAKE_BLE.max_concurrent_connects = max(FAKE_BLE.max_concurrent_connects, FAKE_BLE.active_connects)
                try:
                    await asyncio.sleep(0.12)
                    entry = FAKE_BLE.devices.get(cli.address)
                    cli.services = entry[2] if entry else []
                    cli.is_connected = True
                    FAKE_BLE.connected_clients.append(cli)
                finally:
                    FAKE_BLE.active_connects -= 1

            FakeBleakClient.connect = slower_connect
            try:
                ws.send_json({"id": 30, "op": "connect", "address": self.trainer_addr})
                ws.send_json({"id": 31, "op": "connect", "address": self.hr_addr})
                replies = {}
                deadline = time.time() + 5
                while len(replies) < 2 and time.time() < deadline:
                    m = ws.recv_json(timeout=2)
                    if "id" in m:
                        replies[m["id"]] = m
                self.assertTrue(replies[30]["ok"] and replies[31]["ok"])
                self.assertEqual(FAKE_BLE.max_concurrent_connects, 1, "WinRT connects must be serialized!")
            finally:
                FakeBleakClient.connect = orig_connect
        finally:
            ws.close()

    def test_shutdown_cli_and_ws_release_ble_and_port(self):
        ws = self._connect_ws()
        ws.request("hello", 1, claim="takeover")
        ws.request("connect", 2, address=self.trainer_addr)
        self.assertEqual(len(FAKE_BLE.connected_clients), 1)
        self.assertTrue(flux_bridge.stop_running_bridge(self.port))
        self.assertIsNone(flux_bridge.port_owner(self.port))
        self.assertEqual(len(FAKE_BLE.connected_clients), 0)
        ws.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
