#!/usr/bin/env python3
"""
Independent E2E test suite for `flux_bridge.py`.

Exercises the real `flux_bridge.py` code (Session, make_app, port_owner,
stop_running_bridge, describe_error, request_stop, HTTP/WS endpoints, Origin
security, BLE scan throttling, 3-attempt connect retries, multi-device
trainer + HR operations, and shutdown cleanup).

Works out-of-the-box with zero pip dependencies (uses a lightweight stdlib
asyncio HTTP/WebSocket server shim matching aiohttp's API when aiohttp/bleak
are not installed, or real aiohttp if present).
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import http.server
import importlib
import json
import os
import socket
import struct
import sys
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
# Lightweight RFC 6455 WebSocket + HTTP/1.1 test client (stdlib only)
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
        req = ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8")
        sock.sendall(req)

        resp = bytearray()
        while b"\r\n\r\n" not in resp:
            chunk = sock.recv(4096)
            if not chunk:
                break
            resp.extend(chunk)
        header_end = resp.index(b"\r\n\r\n")
        head = resp[:header_end].decode("utf-8", errors="replace")
        status_line = head.split("\r\n", 1)[0]
        status_code = int(status_line.split(" ")[1])
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
            if opcode == 0x9:  # ping -> pong
                continue
            if opcode == 0x1:
                return json.loads(data.decode("utf-8"))

    def request(self, op: str, req_id: int, **kwargs) -> tuple[dict, list[dict]]:
        """Send an op and return (response_for_req_id, events_received_meanwhile)."""
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
# Fake Bleak objects (controllable failure modes, multi-device, notifications)
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
        self.scanner_active = False
        self.scanner_cb = None
        self.connected_clients: list["FakeBleakClient"] = []
        self.writes: list[tuple[str, str, bytes, bool]] = []
        self.reads: dict[tuple[str, str], bytes] = {}
        self.notify_cbs: dict[tuple[str, str], object] = {}


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
        await asyncio.sleep(0.02)
        if addr in FAKE_BLE.find_returns_none:
            return None
        entry = FAKE_BLE.devices.get(addr)
        return entry[0] if entry else None


class FakeBleakClient:
    def __init__(self, target, disconnected_callback=None, timeout: float = 20.0):
        self.address = target.address if hasattr(target, "address") else str(target)
        self._disc_cb = disconnected_callback
        self.timeout = timeout
        self.is_connected = False
        self.services: list[FakeService] = []

    async def connect(self):
        await asyncio.sleep(0.02)
        plan = FAKE_BLE.fail_attempts.get(self.address)
        if plan:
            err = plan.pop(0)
            if err is not None:
                raise err
        entry = FAKE_BLE.devices.get(self.address)
        self.services = entry[2] if entry else []
        self.is_connected = True
        FAKE_BLE.connected_clients.append(self)

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
# Minimal asyncio HTTP/1.1 + RFC 6455 aiohttp shim (used ONLY when aiohttp
# is not installed locally, so tests still run over real localhost sockets!)
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

    def json_response(data, status=200):
        return Response(status=status, text=json.dumps(data), content_type="application/json")

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
            resp = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            )
            self._writer.write(resp.encode("utf-8"))
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

# Always inject fake bleak so no physical Bluetooth hardware is needed for E2E tests
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
    """Runs flux_bridge.serve() in a background thread on a real TCP port."""

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
        url = f"http://localhost:{self.port}/flux-s.html"

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


class TestFluxBridgeE2E(unittest.TestCase):
    def setUp(self):
        FAKE_BLE.reset()
        self.port = _free_port()
        self.bridge = BridgeRunnerThread(self.port)
        self.bridge.start()

        # Register simulated Tacx Flux S + Polar H10
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

    def test_http_serving_and_origin_security(self):
        # 1. GET /flux-s.html returns HTML with Cache-Control: no-store
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/flux-s.html", timeout=3) as r:
            self.assertEqual(r.status, 200)
            self.assertEqual(r.headers.get("Cache-Control"), "no-store")
            self.assertIn(b"Tacx Flux S", r.read(2048))

        # 2. port_owner detects 'bridge' via GET /ws without Origin -> 403 'origin not allowed'
        self.assertEqual(flux_bridge.port_owner(self.port), "bridge")
        self.assertIsNone(flux_bridge.port_owner(_free_port()))

        # 3. WS rejects missing Origin and foreign Origin
        with self.assertRaises(RuntimeError) as ctx_no_origin:
            SimpleWSClient.connect("127.0.0.1", self.port, "/ws", origin=None)
        self.assertIn("403", str(ctx_no_origin.exception))

        with self.assertRaises(RuntimeError) as ctx_bad_origin:
            SimpleWSClient.connect("127.0.0.1", self.port, "/ws", origin="https://evil.example")
        self.assertIn("403", str(ctx_bad_origin.exception))

        # 4. POST /shutdown rejects missing header and foreign origin
        req_no_hdr = urllib.request.Request(f"http://127.0.0.1:{self.port}/shutdown", data=b"", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as e1:
            urllib.request.urlopen(req_no_hdr, timeout=3)
        self.assertEqual(e1.exception.code, 403)

        req_bad_orig = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/shutdown",
            data=b"",
            method="POST",
            headers={"X-Flux-Bridge": "shutdown", "Origin": "https://evil.example"},
        )
        with self.assertRaises(urllib.error.HTTPError) as e2:
            urllib.request.urlopen(req_bad_orig, timeout=3)
        self.assertEqual(e2.exception.code, 403)

    def test_port_owner_detects_other_program(self):
        other_port = _free_port()

        class QuietHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"other server")
            def log_message(self, *_args):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", other_port), QuietHandler)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            self.assertEqual(flux_bridge.port_owner(other_port), "other")
        finally:
            srv.shutdown()
            srv.server_close()

    def test_scan_throttling_and_multi_device_trainer_plus_hr(self):
        ws = SimpleWSClient.connect("127.0.0.1", self.port, "/ws", origin=f"http://127.0.0.1:{self.port}")
        try:
            res, _ = ws.request("hello", 1)
            self.assertTrue(res["ok"])
            self.assertEqual(res["version"], 3)

            res, _ = ws.request("scanStart", 2)
            self.assertTrue(res["ok"])
            self.assertTrue(FAKE_BLE.scanner_active)

            # Fire two adv callbacks immediately with same metadata -> second throttled by 0.7 s
            tr_dev, tr_adv, _ = FAKE_BLE.devices[self.trainer_addr]
            hr_dev, hr_adv, _ = FAKE_BLE.devices[self.hr_addr]
            FAKE_BLE.scanner_cb(tr_dev, tr_adv)
            FAKE_BLE.scanner_cb(tr_dev, FakeAdvData("Tacx Flux 46956", -63, [flux_bridge.FTMS_SVC]))
            FAKE_BLE.scanner_cb(hr_dev, hr_adv)

            ev1 = ws.recv_json()
            ev2 = ws.recv_json()
            addrs = {ev1["device"]["address"], ev2["device"]["address"]}
            self.assertEqual(addrs, {self.trainer_addr, self.hr_addr})
            for ev in (ev1, ev2):
                self.assertEqual(ev["ev"], "adv")
                if ev["device"]["address"] == self.trainer_addr:
                    self.assertTrue(ev["device"]["ftms"])
                    self.assertFalse(ev["device"]["hr"])
                else:
                    self.assertTrue(ev["device"]["hr"])

            # Connect trainer (must stop scan first!)
            res, events = ws.request("connect", 3, address=self.trainer_addr)
            self.assertTrue(res["ok"])
            self.assertFalse(FAKE_BLE.scanner_active)
            self.assertEqual(res["name"], "Tacx Flux 46956")
            self.assertTrue(any(e.get("ev") == "progress" for e in events))
            self.assertEqual(res["services"][0]["uuid"], flux_bridge.FTMS_SVC)
            self.assertIn("chars", res["services"][0])

            # Connect HR strap simultaneously
            res_hr, _ = ws.request("connect", 4, address=self.hr_addr)
            self.assertTrue(res_hr["ok"])
            self.assertEqual(len(FAKE_BLE.connected_clients), 2)

            # Read on both devices simultaneously
            r_tr, _ = ws.request("read", 5, address=self.trainer_addr, uuid="00002ad6-0000-1000-8000-00805f9b34fb")
            self.assertEqual(r_tr["data"], "000064000100")
            r_hr, _ = ws.request("read", 6, address=self.hr_addr, uuid="00002a19-0000-1000-8000-00805f9b34fb")
            self.assertEqual(r_hr["data"], "58")

            # Write to trainer control point
            w_res, _ = ws.request("write", 7, address=self.trainer_addr, uuid="00002ad9-0000-1000-8000-00805f9b34fb", data="059600", response=True)
            self.assertTrue(w_res["ok"])
            self.assertEqual(FAKE_BLE.writes[-1], (self.trainer_addr, "00002ad9-0000-1000-8000-00805f9b34fb", b"\x05\x96\x00", True))

            # Subscribe and push notify on both devices
            ws.request("subscribe", 8, address=self.trainer_addr, uuid="00002ad2-0000-1000-8000-00805f9b34fb")
            ws.request("subscribe", 9, address=self.hr_addr, uuid="00002a37-0000-1000-8000-00805f9b34fb")
            FAKE_BLE.notify_cbs[(self.hr_addr, "00002a37-0000-1000-8000-00805f9b34fb")](None, bytearray([0x06, 145]))
            n_ev = ws.recv_json()
            self.assertEqual(n_ev["ev"], "notify")
            self.assertEqual(n_ev["address"], self.hr_addr)
            self.assertEqual(n_ev["data"], "0691")

            # Simulate unexpected HR drop -> disconnected event carries HR address while trainer stays connected
            hr_client = [c for c in FAKE_BLE.connected_clients if c.address == self.hr_addr][0]
            hr_client.simulate_drop()
            d_ev = ws.recv_json()
            self.assertEqual(d_ev, {"ev": "disconnected", "address": self.hr_addr})
            self.assertEqual(len(FAKE_BLE.connected_clients), 1)
            self.assertEqual(FAKE_BLE.connected_clients[0].address, self.trainer_addr)

            # Disconnect trainer explicitly
            disc_res, _ = ws.request("disconnect", 10, address=self.trainer_addr)
            self.assertTrue(disc_res["ok"])
            self.assertEqual(len(FAKE_BLE.connected_clients), 0)
        finally:
            ws.close()

    def test_connect_retries_and_readable_errors(self):
        # Never returns empty error string for TimeoutError
        self.assertEqual(flux_bridge.describe_error(TimeoutError()), "timed out")
        self.assertEqual(flux_bridge.describe_error(asyncio.TimeoutError()), "timed out")

        ws = SimpleWSClient.connect("127.0.0.1", self.port, "/ws", origin=f"http://localhost:{self.port}")
        try:
            # Attempt 1 hangs (TimeoutError), Attempt 2 succeeds!
            FAKE_BLE.fail_attempts[self.trainer_addr] = [asyncio.TimeoutError(), None]
            res, events = ws.request("connect", 1, address=self.trainer_addr)
            self.assertTrue(res["ok"])
            prog_texts = [e["text"] for e in events if e.get("ev") == "progress"]
            self.assertTrue(any("attempt 1/3" in t for t in prog_texts))
            self.assertTrue(any("attempt 2/3" in t for t in prog_texts))
            ws.request("disconnect", 2, address=self.trainer_addr)

            # Seed RSSI via scan so error message includes signal strength
            ws.request("scanStart", 3)
            tr_dev, tr_adv, _ = FAKE_BLE.devices[self.trainer_addr]
            FAKE_BLE.scanner_cb(tr_dev, tr_adv)
            ws.recv_json()  # consume adv

            # All 3 attempts time out -> readable message with RSSI and hints (never empty!)
            FAKE_BLE.fail_attempts[self.trainer_addr] = [TimeoutError(), TimeoutError(), TimeoutError()]
            res_fail, _ = ws.request("connect", 4, address=self.trainer_addr)
            self.assertFalse(res_fail["ok"])
            self.assertTrue(res_fail["error"])
            self.assertIn("3 attempts", res_fail["error"])
            self.assertIn("timed out", res_fail["error"])
            self.assertIn("-61 dBm", res_fail["error"])

            # Device not found during find_device_by_address
            FAKE_BLE.find_returns_none.add("11:22:33:44:55:66")
            res_nf, _ = ws.request("connect", 5, address="11:22:33:44:55:66")
            self.assertFalse(res_nf["ok"])
            self.assertIn("device not found", res_nf["error"])
        finally:
            ws.close()

    def test_stop_running_bridge_disconnects_devices_and_frees_port(self):
        ws = SimpleWSClient.connect("127.0.0.1", self.port, "/ws", origin=f"http://127.0.0.1:{self.port}")
        res, _ = ws.request("connect", 1, address=self.trainer_addr)
        self.assertTrue(res["ok"])
        self.assertEqual(len(FAKE_BLE.connected_clients), 1)

        # CLI-style shutdown via POST /shutdown + wait until port is free
        stopped = flux_bridge.stop_running_bridge(self.port)
        self.assertTrue(stopped)
        self.assertIsNone(flux_bridge.port_owner(self.port))
        self.assertEqual(len(FAKE_BLE.connected_clients), 0)
        ws.close()

    def test_ws_shutdown_op_frees_port(self):
        ws = SimpleWSClient.connect("127.0.0.1", self.port, "/ws", origin=f"http://127.0.0.1:{self.port}")
        ws.request("connect", 1, address=self.trainer_addr)
        res, _ = ws.request("shutdown", 2)
        self.assertTrue(res["ok"])
        ws.close()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and flux_bridge.port_owner(self.port) is not None:
            time.sleep(0.1)
        self.assertIsNone(flux_bridge.port_owner(self.port))
        self.assertEqual(len(FAKE_BLE.connected_clients), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
