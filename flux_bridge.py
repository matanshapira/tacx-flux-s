#!/usr/bin/env python3
"""
Flux S BLE bridge.

Serves the Flux S GUI on http://localhost:8765 and exposes Bluetooth LE
(scan / connect / read / write / notify) to the page over a WebSocket at /ws.
This lets the GUI search for trainers and add them from inside the page
(live list with signal strength) and works in any browser.

Requires:  python -m pip install --user bleak aiohttp
Run:       python flux_bridge.py            (opens the GUI in your browser)
           python flux_bridge.py --no-browser --port 8765
           python flux_bridge.py --stop     (stops a running bridge and frees the port)
           python flux_bridge.py --replace  (stops a running bridge, then starts this one)
"""
import argparse
import asyncio
import json
import logging
import socket
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path

from aiohttp import WSMsgType, web
from bleak import BleakClient, BleakScanner

ROOT = Path(__file__).resolve().parent
PAGE = "flux-s.html"
FTMS_SVC = "00001826-0000-1000-8000-00805f9b34fb"
FEC_SVC = "6e40fec1-b5a3-f393-e0a9-e50e24dcca9e"
HR_SVC = "0000180d-0000-1000-8000-00805f9b34fb"
log = logging.getLogger("flux-bridge")
CONNECT_ATTEMPTS = 3
CONNECT_TIMEOUT = 20          # seconds per attempt (a good connect takes ~2-7 s)


class BridgeError(Exception):
    """An error whose message is meant for the user as-is."""


def describe_error(e):
    if isinstance(e, BridgeError):
        return str(e)
    if isinstance(e, (TimeoutError, asyncio.TimeoutError)):
        return "timed out"
    return f"{type(e).__name__}: {e}" if str(e) else type(e).__name__


class Session:
    """One GUI tab <-> several BLE devices (trainer + heart-rate monitor)."""

    def __init__(self, ws, app):
        self.ws = ws
        self.app = app
        self.loop = asyncio.get_running_loop()
        self.scanner = None
        self.clients = {}      # address -> BleakClient (trainer + heart-rate strap at the same time)
        self.seen = {}        # address -> info sent to the GUI
        self.ble_devices = {}  # address -> BLEDevice (faster connect on Windows)
        self.last_sent = {}
        self.tasks = set()

    # ---------- messaging ----------
    async def send(self, obj):
        if not self.ws.closed:
            try:
                await self.ws.send_json(obj)
            except (ConnectionResetError, RuntimeError):
                pass

    def emit(self, obj):
        """Thread-safe fire-and-forget event to the GUI."""
        asyncio.run_coroutine_threadsafe(self.send(obj), self.loop)

    def dispatch(self, msg):
        t = self.loop.create_task(self._dispatch(msg))
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    async def _dispatch(self, msg):
        mid = msg.get("id")
        try:
            res = await self.handle(msg) or {}
            await self.send({"id": mid, "ok": True, **res})
        except Exception as e:  # report every failure back to the GUI
            text = describe_error(e)
            log.warning("op %s failed: %s", msg.get("op"), text)
            await self.send({"id": mid, "ok": False, "error": text})

    # ---------- scanning ----------
    def _on_adv(self, dev, adv):
        uuids = [u.lower() for u in (adv.service_uuids or [])]
        prev = self.seen.get(dev.address, {})
        info = {
            "address": dev.address,
            "name": adv.local_name or dev.name or prev.get("name") or "",
            "rssi": adv.rssi,
            "ftms": FTMS_SVC in uuids or prev.get("ftms", False),
            "fec": FEC_SVC in uuids or prev.get("fec", False),
            "hr": HR_SVC in uuids or prev.get("hr", False),
        }
        self.seen[dev.address] = info
        self.ble_devices[dev.address] = dev
        now = time.monotonic()
        changed = {k: v for k, v in info.items() if k != "rssi"} != {k: v for k, v in prev.items() if k != "rssi"}
        if changed or now - self.last_sent.get(dev.address, 0) >= 0.7:
            self.last_sent[dev.address] = now
            self.emit({"ev": "adv", "device": info})

    async def stop_scan(self):
        if self.scanner:
            s, self.scanner = self.scanner, None
            try:
                await s.stop()
            except Exception:
                pass

    # ---------- connection ----------
    def _on_disconnect(self, addr, client):
        if self.clients.get(addr) is client:
            del self.clients[addr]
            log.info("%s disconnected", addr)
            self.emit({"ev": "disconnected", "address": addr})

    async def disconnect(self, addr=None):
        """Disconnect one device, or all when addr is None."""
        for a in ([addr] if addr else list(self.clients)):
            c = self.clients.pop(a, None)
            if c:
                try:
                    await c.disconnect()
                except Exception:
                    pass

    async def close(self):
        await self.stop_scan()
        await self.disconnect()

    async def _connect_with_retries(self, addr):
        """Windows often leaves the FIRST connection attempt hanging; a fresh scan + retry then
        connects within seconds (measured: attempt 1 timed out at 60 s, attempt 2 took 6 s).
        So: short per-attempt timeout, several attempts, progress reported to the GUI."""
        last = None
        for n in range(1, CONNECT_ATTEMPTS + 1):
            target = self.ble_devices.get(addr) if n == 1 else None
            if target is None:
                self.emit({"ev": "progress", "address": addr, "text": f"looking for {addr}…"})
                target = await BleakScanner.find_device_by_address(addr, timeout=10)
                if target is None:
                    raise BridgeError(
                        "device not found – make sure it is powered on (pedal to wake the trainer, "
                        "wear the HR strap) and not connected to another app or phone")
                self.ble_devices[addr] = target
            self.emit({"ev": "progress", "address": addr, "text": f"connecting… (attempt {n}/{CONNECT_ATTEMPTS})"})
            log.info("connecting to %s (attempt %d/%d)", addr, n, CONNECT_ATTEMPTS)
            client = BleakClient(target, disconnected_callback=lambda c, a=addr: self._on_disconnect(a, c),
                                 timeout=CONNECT_TIMEOUT)
            try:
                await client.connect()
                return client
            except Exception as e:
                last = e
                log.warning("attempt %d/%d failed: %s", n, CONNECT_ATTEMPTS, describe_error(e))
                try:
                    await client.disconnect()          # release a half-open session before retrying
                except Exception:
                    pass
                await asyncio.sleep(1)
        rssi = self.seen.get(addr, {}).get("rssi")
        raise BridgeError(
            f"found the device but it didn't finish connecting after {CONNECT_ATTEMPTS} attempts "
            f"({describe_error(last)}). Try again; if it keeps failing: pedal to wake the trainer, close other "
            f"apps/phones connected to it, move the PC or a USB Bluetooth dongle closer"
            + (f" (signal now {rssi} dBm)" if rssi is not None else "") + ", or power-cycle the trainer.")

    # ---------- operations ----------
    async def handle(self, msg):
        op = msg.get("op")
        if op == "hello":
            return {"version": 3}
        if op == "shutdown":                # "Stop bridge" button in the GUI
            request_stop(self.app, "Stop bridge pressed in the GUI")
            return {}
        if op == "scanStart":
            await self.stop_scan()
            self.last_sent.clear()
            self.scanner = BleakScanner(detection_callback=self._on_adv)
            await self.scanner.start()
            log.info("scan started")
            return {}
        if op == "scanStop":
            await self.stop_scan()
            return {}
        if op == "connect":
            await self.stop_scan()          # Windows connects more reliably when not scanning
            addr = msg["address"]
            await self.disconnect(addr)
            client = await self._connect_with_retries(addr)
            self.clients[addr] = client
            target = self.ble_devices.get(addr, addr)
            services = [
                {"uuid": s.uuid.lower(),
                 "chars": [{"uuid": c.uuid.lower(), "props": list(c.properties)} for c in s.characteristics]}
                for s in client.services
            ]
            name = getattr(target, "name", None) or self.seen.get(addr, {}).get("name") or addr
            log.info("connected to %s (%d services)", name, len(services))
            return {"name": name, "services": services}
        if op == "disconnect":
            await self.disconnect(msg.get("address"))
            return {}

        addr = msg.get("address")
        c = self.clients.get(addr)
        if not c or not c.is_connected:
            raise RuntimeError(f"device {addr} not connected")
        if op == "read":
            return {"data": bytes(await c.read_gatt_char(msg["uuid"])).hex()}
        if op == "write":
            await c.write_gatt_char(msg["uuid"], bytes.fromhex(msg["data"]), response=bool(msg.get("response")))
            return {}
        if op == "subscribe":
            uuid = msg["uuid"]
            await c.start_notify(uuid, lambda _ch, data, u=uuid, a=addr: self.emit(
                {"ev": "notify", "address": a, "uuid": u, "data": bytes(data).hex()}))
            return {}
        raise ValueError(f"unknown op {op!r}")


SESSIONS = web.AppKey("sessions", set)
STOP = web.AppKey("stop", asyncio.Event)
STOP_FAILED = ("Could not stop the running bridge automatically (it may be an older version without this "
               "feature). Close its window, or press Ctrl+C in its terminal, then try again.")


def make_app(port):
    allowed = {f"http://localhost:{port}", f"http://127.0.0.1:{port}"}

    async def ws_handler(request):
        # Only the GUI served by this bridge may control Bluetooth (blocks other websites).
        if request.headers.get("Origin") not in allowed:
            raise web.HTTPForbidden(text="origin not allowed")
        ws = web.WebSocketResponse(heartbeat=20)
        await ws.prepare(request)
        session = Session(ws, request.app)
        request.app[SESSIONS].add(session)
        log.info("GUI connected")
        try:
            async for m in ws:
                if m.type == WSMsgType.TEXT:
                    session.dispatch(json.loads(m.data))
        finally:
            request.app[SESSIONS].discard(session)
            await session.close()
            log.info("GUI disconnected")
        return ws

    async def shutdown_handler(request):
        # Used by "python flux_bridge.py --stop" / "--replace". Browsers can't send the custom header
        # cross-site without a CORS preflight (never approved here), so other websites can't stop the bridge.
        origin = request.headers.get("Origin")
        if request.headers.get("X-Flux-Bridge") != "shutdown" or (origin and origin not in allowed):
            raise web.HTTPForbidden(text="not allowed")
        request_stop(request.app, "stop requested from the command line")
        return web.json_response({"ok": True})

    async def index(_request):
        raise web.HTTPFound("/" + PAGE)

    @web.middleware
    async def no_cache(request, handler):
        resp = await handler(request)
        resp.headers["Cache-Control"] = "no-store"
        return resp

    async def on_shutdown(app):
        # Disconnect every Bluetooth device before the process exits, so the trainer / HR strap
        # are released immediately and can be reconnected (by this bridge again or another app).
        for s in list(app[SESSIONS]):
            await s.close()
            await s.ws.close()

    app = web.Application(middlewares=[no_cache])
    app[SESSIONS] = set()
    app[STOP] = asyncio.Event()
    app.on_shutdown.append(on_shutdown)
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/shutdown", shutdown_handler)
    app.router.add_get("/", index)
    app.router.add_static("/", ROOT, show_index=False)
    return app


def request_stop(app, why):
    log.info("shutting down: %s", why)
    asyncio.get_running_loop().call_later(0.3, app[STOP].set)   # let the reply reach the caller first


async def serve(app, port, url, open_browser):
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", port).start()
    except OSError as e:
        await runner.cleanup()
        raise SystemExit(f"Could not start on port {port}: {e.strerror or e}. "
                         f"Is the bridge already running in another window?")
    print(f"Flux S bridge running at {url}")
    print("Stop it with Ctrl+C, the 'Stop bridge' button in the GUI, or:  python flux_bridge.py --stop")
    if open_browser:
        asyncio.get_running_loop().call_later(0.8, webbrowser.open, url)
    try:
        await app[STOP].wait()
    finally:
        await runner.cleanup()      # closes GUI sessions -> disconnects trainer and HR strap
        print(f"Bridge stopped - port {port} is free.")


def main():
    ap = argparse.ArgumentParser(description="Flux S GUI + Bluetooth LE bridge")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-browser", action="store_true", help="don't open the GUI automatically")
    ap.add_argument("--stop", action="store_true", help="stop the bridge running on --port (frees the port), then exit")
    ap.add_argument("--replace", action="store_true", help="if a bridge is already running, stop it and start this one")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
    url = f"http://localhost:{args.port}/{PAGE}"
    owner = port_owner(args.port)

    if args.stop:
        if owner is None:
            print(f"No bridge is running on port {args.port} - nothing to stop.")
        elif owner == "other":
            sys.exit(f"Port {args.port} is used by another program, not the Flux S bridge.")
        elif stop_running_bridge(args.port):
            print(f"Stopped the Flux S bridge - port {args.port} is free.")
        else:
            sys.exit(STOP_FAILED)
        return

    # Only one program can use the port: offer to take over instead of crashing.
    if owner == "bridge":
        replace = args.replace
        if not replace and sys.stdin is not None and sys.stdin.isatty():
            try:
                answer = input(f"A Flux S bridge is already running on port {args.port}. Stop it and start here? [Y/n] ")
                replace = answer.strip().lower() in ("", "y", "yes")
            except EOFError:
                replace = False
        if not replace:
            print(f"Using the bridge that is already running: {url}")
            if not args.no_browser:
                webbrowser.open(url)
            return
        if not stop_running_bridge(args.port):
            sys.exit(STOP_FAILED)
        print("Stopped the previous bridge.")
    elif owner == "other":
        sys.exit(f"Port {args.port} is used by another program. Close it, or start the bridge on "
                 f"another port:  python flux_bridge.py --port {args.port + 5}")

    try:
        asyncio.run(serve(make_app(args.port), args.port, url, not args.no_browser))
    except KeyboardInterrupt:
        pass                        # Ctrl+C: serve() already disconnected the devices and said so


def stop_running_bridge(port):
    """Ask a running bridge to shut down and wait until the port is free."""
    req = urllib.request.Request(f"http://127.0.0.1:{port}/shutdown", data=b"", method="POST",
                                 headers={"X-Flux-Bridge": "shutdown"})
    try:
        urllib.request.urlopen(req, timeout=5).read()
    except Exception:
        return False
    for _ in range(100):            # up to 10 s for the Bluetooth disconnects and the port release
        if port_owner(port) is None:
            return True
        time.sleep(0.1)
    return False


def port_owner(port):
    """None if the port is free, 'bridge' if a Flux S bridge already listens there, else 'other'."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            pass
    except OSError:
        return None
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/ws", timeout=2)
    except urllib.error.HTTPError as e:   # our bridge refuses /ws without the GUI's Origin header
        if e.code == 403 and b"origin not allowed" in e.read():
            return "bridge"
    except Exception:
        pass
    return "other"


if __name__ == "__main__":
    main()
