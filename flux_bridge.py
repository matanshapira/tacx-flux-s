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

Files (next to flux-s.html): flux_config.json (all settings), Workouts/ (saved workouts), Rides/ (every ride),
Logs/ (bridge + app log, one file per day, last 14 days kept).
"""
import argparse
import asyncio
import collections
import csv
import json
import os
import logging
import logging.handlers
import re
import socket
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import WSMsgType, web
from bleak import BleakClient, BleakScanner

ROOT = Path(__file__).resolve().parent
PAGE = "flux-s.html"
FTMS_SVC = "00001826-0000-1000-8000-00805f9b34fb"
FEC_SVC = "6e40fec1-b5a3-f393-e0a9-e50e24dcca9e"
HR_SVC = "0000180d-0000-1000-8000-00805f9b34fb"
log = logging.getLogger("flux-bridge")
# Everything the app keeps lives next to flux-s.html (nothing is stored in the browser):
CONFIG_FILE = ROOT / "flux_config.json"   # all settings (FTP, weight, devices, options, …), see --config
WORKOUT_DIR = ROOT / "Workouts"           # saved workouts as .json files, see --workouts
RIDE_DIR = ROOT / "Rides"                 # every ride, written continuously as <id>.csv + <id>.json, see --rides
LOG_DIR = ROOT / "Logs"                   # flux_bridge.log (+ one older file per day), see --logs
LOG_DAYS = 14
STALE_RIDE_SEC = 600                      # a ride whose file hasn't changed for 10 min is over
RIDE_HEADER = "time_s,power_w,cadence_rpm,speed_kmh,heart_rate_bpm,distance_m,mode,target,workout_step"
_RIDE_ID = re.compile(r"^[0-9A-Za-z_-]{1,64}$")
_ride_stats_cache = {}
BRIDGE_VERSION = 8
CONNECT_ATTEMPTS = 3
CONNECT_TIMEOUT = 25          # seconds per attempt (bleak 3.1 WinRT default is 30 s)
CONNECT_DEADLINE = 125        # whole connect op (all 3 attempts); the page waits 150 s
BLE_CACHE_TTL = 15.0          # ignore cached BLEDevice objects older than 15 s (stale after sleep/drops)
SCAN_STOP_SETTLE = 0.4        # let Windows BT controller finish stopping scan before GATT connect
RETRY_SETTLE = 2.0            # let Windows WinRT finish tearing down half-open session between retries
OPEN_BROWSER_WAIT = 3.5       # after start: give an already open tab time to reconnect before opening a new one
# Only ONE GUI tab may use Bluetooth at a time. Two tabs = two controllers fighting over the trainer
# (conflicting ERG targets, control requests, one tab's reconnect dropping the other's link).
# Tabs that aren't the active one may only read files and save their ride.
PASSIVE_OPS = {"hello", "configLoad", "rideAppend", "rideList", "rideLoad", "workoutList", "workoutLoad", "clientLog"}
NOT_ACTIVE = "this app is open in another tab – use that tab, or click 'Use this tab'"


class BridgeError(Exception):
    """An error whose message is meant for the user as-is."""


def workout_files():
    """{workout name: (path, data)} for every valid .json file in WORKOUT_DIR."""
    WORKOUT_DIR.mkdir(parents=True, exist_ok=True)
    out = {}
    for f in sorted(WORKOUT_DIR.glob("*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("blocks"), list):
                out.setdefault(str(data.get("name") or f.stem), (f, data))
        except Exception:
            log.warning("skipping unreadable workout file %s", f.name)
    return out


def ensure_folder(path):
    """Create the folder. On Windows an existing folder that differs only in capitalisation
    (e.g. 'rides' from an older version) is renamed to the new name, keeping its files."""
    parent = path.parent
    if parent.exists():
        for p in parent.iterdir():
            if p.is_dir() and p.name.lower() == path.name.lower() and p.name != path.name:
                tmp = parent / (path.name + ".renaming")
                p.rename(tmp)
                tmp.rename(path)
                log.info("renamed folder %s -> %s", p.name, path.name)
                break
    path.mkdir(parents=True, exist_ok=True)


def ride_paths(rid):
    if not _RIDE_ID.match(str(rid or "")):
        raise BridgeError("invalid ride id")
    RIDE_DIR.mkdir(parents=True, exist_ok=True)
    return RIDE_DIR / f"{rid}.csv", RIDE_DIR / f"{rid}.json"


def read_json(path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json_atomic(path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def ride_stats(csv_path):
    """Summary of a ride CSV (cached by size/mtime): duration, avg/max power, NP, kJ, cadence, HR, km."""
    st = csv_path.stat()
    key = (str(csv_path), st.st_mtime_ns, st.st_size)
    if key in _ride_stats_cache:
        return _ride_stats_cache[key]

    def num(row, k):
        try:
            v = row.get(k)
            return float(v) if v not in (None, "") else None
        except ValueError:
            return None

    n = sp = mp = 0.0
    roll, rs, s4, n4 = collections.deque(), 0.0, 0.0, 0
    hs = hn = hm = cs = cn = 0.0
    dist = 0.0
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pw = num(row, "power_w")
            if pw is None:
                continue
            n += 1; sp += pw; mp = max(mp, pw)
            roll.append(pw); rs += pw
            if len(roll) > 30:
                rs -= roll.popleft()
            if len(roll) == 30:
                s4 += (rs / 30) ** 4; n4 += 1
            h = num(row, "heart_rate_bpm")
            if h:
                hs += h; hn += 1; hm = max(hm, h)
            c = num(row, "cadence_rpm")
            if c and c > 0:
                cs += c; cn += 1
            dist = num(row, "distance_m") or dist
    out = {"seconds": int(n), "avgPower": round(sp / n) if n else 0, "maxPower": round(mp),
           "np": round((s4 / n4) ** 0.25) if n4 else None, "kj": round(sp / 1000),
           "avgCad": round(cs / cn) if cn else None, "avgHr": round(hs / hn) if hn else None,
           "maxHr": round(hm) if hn else None, "km": round(dist / 1000, 2)}
    _ride_stats_cache[key] = out
    return out


def finalize_stale_rides():
    """Rides that never got 'End ride' (tab closed, PC slept, connection lost) have no end time / NP / IF / TSS
    in their .json. Fill them in from the CSV once the file hasn't changed for STALE_RIDE_SEC. If such a ride
    is continued later (the file changes again), it is recomputed. The page's own final summary wins."""
    if not RIDE_DIR.exists():
        return 0
    done = 0
    now = time.time()
    for csv_p in RIDE_DIR.glob("*.csv"):
        try:
            st = csv_p.stat()
            if now - st.st_mtime < STALE_RIDE_SEC:
                continue                                   # may still be recording
            meta_p = csv_p.with_suffix(".json")
            meta = read_json(meta_p, {}) or {}
            if not isinstance(meta, dict):
                meta = {}
            if meta.get("endedAt") and not meta.get("autoFinalized"):
                continue                                   # finished normally
            if meta.get("autoFinalized") and meta.get("csvMtime") == st.st_mtime_ns:
                continue                                   # already done, unchanged since
            stats = ride_stats(csv_p)
            ftp = meta.get("ftp") or 0
            np_ = stats.get("np")
            if_ = round(np_ / ftp, 3) if np_ and ftp else None
            tss = round(stats["seconds"] / 3600 * if_ * if_ * 100) if if_ else None
            meta.update({"endedAt": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z"),
                         "seconds": stats["seconds"], "np": np_, "IF": if_, "tss": tss,
                         "autoFinalized": True, "csvMtime": st.st_mtime_ns})
            write_json_atomic(meta_p, meta)
            log.info("ride %s had no end – finished it from the file (%d s, NP %s, TSS %s)", csv_p.stem, stats["seconds"], np_, tss)
            done += 1
        except Exception as e:
            log.warning("could not finish ride %s: %s", csv_p.name, describe_error(e))
    return done


def setup_logging():
    """Console + Logs/flux_bridge.log (new file every midnight, LOG_DAYS kept). The page sends its own
    log lines too (op clientLog), so one file shows both sides of every connection problem."""
    fmt = logging.Formatter("%(asctime)s  %(message)s", datefmt="%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.TimedRotatingFileHandler(LOG_DIR / "flux_bridge.log", when="midnight",
                                                       backupCount=LOG_DAYS, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s %(name)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        root.addHandler(fh)
    except OSError as e:
        print(f"Could not write the log file in {LOG_DIR}: {e}")
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)   # no line per page/file request
    logging.getLogger("bleak").setLevel(logging.WARNING)


def workout_path(name):
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(" .")[:80] or "workout"
    return WORKOUT_DIR / f"{safe}.json"


def describe_error(e):
    if isinstance(e, BridgeError):
        return str(e)
    if isinstance(e, (TimeoutError, asyncio.TimeoutError)):
        return "timed out"
    return f"{type(e).__name__}: {e}" if str(e) else type(e).__name__


async def safe_disconnect(client, timeout=5):
    """On Windows, disconnect() of a half-open (timed-out) session can wait forever.
    Never let that cleanup block the next attempt or the GUI."""
    try:
        await asyncio.wait_for(client.disconnect(), timeout)
    except Exception as e:
        if isinstance(e, (TimeoutError, asyncio.TimeoutError)):
            log.warning("Bluetooth cleanup didn't finish within %d s – continuing anyway", timeout)


class Session:
    """One GUI tab <-> several BLE devices (trainer + heart-rate monitor)."""

    def __init__(self, ws, app):
        self.ws = ws
        self.app = app
        self.loop = asyncio.get_running_loop()
        self.scanner = None
        self.clients = {}      # address -> BleakClient (trainer + heart-rate strap at the same time)
        self.seen = {}        # address -> info sent to the GUI
        self.ble_devices = {}  # address -> (BLEDevice, monotonic_ts) (fresh scan cache only)
        self.uncached_addrs = set()  # addresses where Windows GATT cache failed -> force uncached discovery
        self.last_sent = {}
        self.tasks = set()
        self.connecting = {}   # address -> task of the connect op in flight (cancelled by disconnect / a newer connect)
        self._ble_lock = asyncio.Lock()  # serialize WinRT scan/connect ops so trainer + HR never collide in HCI
        self.active = False    # True for the one tab that owns Bluetooth (see claim())
        self.closed = False

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
        except asyncio.CancelledError:  # e.g. a connect superseded by a disconnect / newer connect
            log.info("op %s cancelled", msg.get("op"))
            await self.send({"id": mid, "ok": False, "error": "cancelled"})
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
        now = time.monotonic()
        self.seen[dev.address] = info
        self.ble_devices[dev.address] = (dev, now)
        changed = {k: v for k, v in info.items() if k != "rssi"} != {k: v for k, v in prev.items() if k != "rssi"}
        if changed or now - self.last_sent.get(dev.address, 0) >= 0.7:
            self.last_sent[dev.address] = now
            self.emit({"ev": "adv", "device": info})

    def _fresh_ble_device(self, addr):
        entry = self.ble_devices.get(addr)
        if not entry:
            return None
        dev, ts = entry
        if time.monotonic() - ts <= BLE_CACHE_TTL:
            return dev
        self.ble_devices.pop(addr, None)
        return None

    async def stop_scan(self):
        if self.scanner:
            s, self.scanner = self.scanner, None
            try:
                await s.stop()
            except Exception:
                pass
            return True
        return False

    # ---------- connection ----------
    def _on_disconnect(self, addr, client):
        self.ble_devices.pop(addr, None)
        if self.clients.get(addr) is client:
            del self.clients[addr]
            log.info("%s disconnected", addr)
            self.emit({"ev": "disconnected", "address": addr})

    async def cancel_connect(self, addr=None):
        """Abort connect ops in flight (one device, or all) so they can't finish later and leave an
        orphaned connection that blocks the trainer."""
        me = asyncio.current_task()
        for a in ([addr] if addr else list(self.connecting)):
            t = self.connecting.pop(a, None)
            if t and t is not me and not t.done():
                t.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(t), 10)
                except BaseException:
                    pass

    async def disconnect(self, addr=None):
        """Disconnect one device, or all when addr is None (also aborts connects in flight)."""
        await self.cancel_connect(addr)
        for a in ([addr] if addr else list(self.clients)):
            self.ble_devices.pop(a, None)
            c = self.clients.pop(a, None)
            if c:
                log.info("disconnecting %s", a)
                await safe_disconnect(c)

    async def release(self):
        """Give up Bluetooth (stop the search, disconnect every device)."""
        await self.stop_scan()
        await self.disconnect()

    async def close(self):
        self.closed = True
        self.active = False
        await self.release()

    async def claim(self, takeover):
        """Make this tab the one that uses Bluetooth. takeover=True (a page that was just opened / reloaded,
        or 'Use this tab'): the previous tab is told to stop and its devices are disconnected first.
        takeover=False (a tab reconnecting after a bridge restart): only if no other tab is active."""
        cur = self.app[ACTIVE]["s"]
        if cur is not None and cur is not self and not cur.closed and not cur.ws.closed:
            if not takeover:
                log.info("a second GUI tab reconnected – another tab is active, it stays passive")
                return False
            log.info("a new GUI tab took over – releasing the devices of the previous tab")
            cur.active = False
            await cur.send({"ev": "takenOver"})
            await cur.release()
        self.app[ACTIVE]["s"] = self
        self.active = True
        return True

    def _make_client(self, target, addr, use_cached=True):
        kwargs = {
            "disconnected_callback": lambda c, a=addr: self._on_disconnect(a, c),
            "timeout": CONNECT_TIMEOUT,
        }
        if not use_cached:
            kwargs["winrt"] = {"use_cached_services": False}
        return BleakClient(target, **kwargs)

    async def _connect_with_retries(self, addr):
        """Windows often leaves the FIRST connection attempt hanging or hangs on a stale GATT cache;
        a fresh scan + uncached GATT discovery + settle pause then connects reliably."""
        last, tried = None, 0
        deadline = time.monotonic() + CONNECT_DEADLINE
        for n in range(1, CONNECT_ATTEMPTS + 1):
            if n > 1 and deadline - time.monotonic() < CONNECT_TIMEOUT + 5:
                log.info("no time left for attempt %d/%d", n, CONNECT_ATTEMPTS)
                break
            use_cached = (n == 1) and (addr not in self.uncached_addrs)
            target = self._fresh_ble_device(addr) if use_cached else None
            if target is None:
                self.emit({"ev": "progress", "address": addr, "text": f"looking for {addr}…"})
                log.info("looking for %s (search up to 10 s)", addr)
                target = await BleakScanner.find_device_by_address(addr, timeout=10)
                if target is None:
                    log.info("%s is not advertising – trying a direct connection", addr)
                    # Not advertising: it may already be connected to another app on THIS PC (e.g. MyWhoosh).
                    # Windows can share that link, so try connecting by address without a scan.
                    self.emit({"ev": "progress", "address": addr,
                               "text": "not advertising – trying a direct connection (it may be in use by another app on this PC)…"})
                    client = await self._connect_direct(addr, use_cached=use_cached)
                    if client:
                        return client
                    raise BridgeError(
                        "device not found – make sure it is powered on (pedal to wake the trainer, wear the HR strap). "
                        "If another app on a phone/tablet is connected to it, the device may accept only that one connection")
                self.ble_devices[addr] = (target, time.monotonic())
            tried = n
            mode_note = "" if use_cached else ", fresh GATT"
            self.emit({"ev": "progress", "address": addr, "text": f"connecting… (attempt {n}/{CONNECT_ATTEMPTS}{mode_note})"})
            log.info("connecting to %s (attempt %d/%d%s)", addr, n, CONNECT_ATTEMPTS, mode_note)
            client = self._make_client(target, addr, use_cached=use_cached)
            try:
                await asyncio.wait_for(client.connect(), CONNECT_TIMEOUT + 5)   # hard limit on top of bleak's own
                log.info("connected to %s (attempt %d/%d%s)", addr, n, CONNECT_ATTEMPTS, mode_note)
                if not use_cached:
                    self.uncached_addrs.add(addr)       # uncached discovery worked; keep using it this session
                return client
            except asyncio.CancelledError:
                self.ble_devices.pop(addr, None)
                await safe_disconnect(client)          # never leave a half-open session behind
                raise
            except Exception as e:
                last = e
                self.ble_devices.pop(addr, None)       # never reuse a failed BLEDevice handle
                self.uncached_addrs.add(addr)          # bypass potentially poisoned Windows GATT cache next try
                log.warning("attempt %d/%d failed: %s%s", n, CONNECT_ATTEMPTS, describe_error(e),
                            " – retrying with a fresh search (bypassing GATT cache)" if n < CONNECT_ATTEMPTS else "")
                await safe_disconnect(client)          # release a half-open session before retrying
                if n < CONNECT_ATTEMPTS:
                    await asyncio.sleep(RETRY_SETTLE)
        rssi = self.seen.get(addr, {}).get("rssi")
        raise BridgeError(
            f"found the device but it didn't finish connecting after {tried} attempt(s) "
            f"({describe_error(last)}). Try again; if it keeps failing: pedal to wake the trainer, close other "
            f"apps/phones connected to it, toggle Windows Bluetooth off/on (clears stale cache), move the PC or "
            f"USB Bluetooth dongle closer"
            + (f" (signal now {rssi} dBm)" if rssi is not None else "") + ", or power-cycle the trainer.")

    async def _connect_direct(self, addr, use_cached=True):
        client = self._make_client(addr, addr, use_cached=use_cached)
        backend = getattr(client, "_backend", None)
        if backend is None or not hasattr(backend, "_device_info"):
            return None                                   # only the Windows backend can skip the scan
        try:
            backend._device_info = int(addr.replace(":", ""), 16)
            await asyncio.wait_for(client.connect(), CONNECT_TIMEOUT + 5)
            log.info("direct connection to %s worked", addr)
            return client
        except asyncio.CancelledError:
            await safe_disconnect(client)
            raise
        except Exception as e:
            log.warning("direct connection to %s failed: %s", addr, describe_error(e))
            await safe_disconnect(client)
            return None

    # ---------- operations ----------
    async def handle(self, msg):
        op = msg.get("op")
        if op == "hello":
            claim = msg.get("claim", "takeover")          # pages older than version 7 don't send it
            ok = await self.claim(takeover=claim != "ifFree")
            return {"version": BRIDGE_VERSION, "active": ok, "busy": not ok}
        if op == "claim":                                 # "Use this tab" button
            await self.claim(takeover=True)
            return {"active": True}
        if not self.active and op not in PASSIVE_OPS:
            raise BridgeError(NOT_ACTIVE)
        if op == "configLoad":                            # settings file next to flux-s.html
            data = read_json(CONFIG_FILE, None)
            return {"exists": isinstance(data, dict), "config": data if isinstance(data, dict) else {}, "file": str(CONFIG_FILE)}
        if op == "configSave":
            cfg = msg.get("config")
            if not isinstance(cfg, dict):
                raise BridgeError("invalid settings")
            write_json_atomic(CONFIG_FILE, cfg)
            return {"file": str(CONFIG_FILE)}
        if op == "rideAppend":                            # called every few seconds while recording
            csv_p, meta_p = ride_paths(msg.get("ride"))
            new = not csv_p.exists()
            if msg.get("first") and not new and not msg.get("replace"):
                # a NEW ride whose file already exists = another recorder (tab) started in the same second.
                # Never mix two rides in one file: the page picks another id and retries.
                log.warning("ride %s already exists – asking the GUI for another ride id", csv_p.stem)
                return {"conflict": True}
            lines = [str(x).replace("\n", " ").replace("\r", " ") for x in (msg.get("lines") or [])]
            if msg.get("replace"):                        # whole ride at once (e.g. recovered from browser storage)
                tmp = csv_p.with_suffix(".csv.tmp")
                with open(tmp, "w", encoding="utf-8", newline="") as f:
                    f.write((msg.get("header") or RIDE_HEADER) + "\n")
                    f.writelines(x + "\n" for x in lines)
                    f.flush()
                    os.fsync(f.fileno())
                tmp.replace(csv_p)
            else:
                with open(csv_p, "a", encoding="utf-8", newline="") as f:
                    if new:
                        f.write((msg.get("header") or RIDE_HEADER) + "\n")
                    f.writelines(x + "\n" for x in lines)
                    f.flush()
                    os.fsync(f.fileno())                 # on disk now, even if the PC crashes
            if msg.get("meta"):
                meta = read_json(meta_p, {}) or {}
                meta.update(msg["meta"])
                write_json_atomic(meta_p, meta)
            if new:
                log.info("recording ride to %s", csv_p)
            return {"file": str(csv_p)}
        if op == "rideList":
            await asyncio.to_thread(finalize_stale_rides)
            rides = []
            for csv_p in sorted(RIDE_DIR.glob("*.csv"), reverse=True) if RIDE_DIR.exists() else []:
                try:
                    stats = ride_stats(csv_p)
                except Exception as e:
                    stats = {"error": describe_error(e)}
                rides.append({"id": csv_p.stem, "meta": read_json(csv_p.with_suffix(".json"), {}) or {}, **stats})
            return {"folder": str(RIDE_DIR), "rides": rides}
        if op == "rideLoad":
            csv_p, meta_p = ride_paths(msg.get("ride"))
            if not csv_p.exists():
                raise BridgeError("ride not found")
            return {"csv": csv_p.read_text(encoding="utf-8"), "meta": read_json(meta_p, {}) or {}, "file": str(csv_p)}
        if op == "workoutList":
            return {"folder": str(WORKOUT_DIR), "workouts": sorted(workout_files(), key=str.lower)}
        if op == "workoutLoad":
            hit = workout_files().get(msg["name"])
            if not hit:
                raise BridgeError(f'workout "{msg["name"]}" not found in {WORKOUT_DIR}')
            return {"workout": hit[1]}
        if op == "workoutSave":
            wo = msg.get("workout")
            if not isinstance(wo, dict) or not str(wo.get("name", "")).strip() or not isinstance(wo.get("blocks"), list):
                raise BridgeError("invalid workout")
            existing = workout_files().get(wo["name"])
            path = existing[0] if existing else workout_path(wo["name"])
            n = 2
            while not existing and path.exists():           # another workout already uses this file name
                path = workout_path(f'{wo["name"]} ({n})'); n += 1
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(wo, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(path)                                # atomic: never leaves a half-written file
            log.info("workout saved: %s", path)
            return {"file": str(path)}
        if op == "workoutDelete":
            hit = workout_files().get(msg["name"])
            if hit:
                hit[0].unlink()
                log.info("workout deleted: %s", hit[0])
            return {}
        if op == "clientLog":               # the page's log lines go into the same log file
            text = str(msg.get("text", ""))[:500].replace("\n", " ")
            level = {"e": logging.ERROR, "w": logging.WARNING}.get(msg.get("level"), logging.INFO)
            logging.getLogger("page" if self.active else "page(passive)").log(level, "%s", text)
            return {}
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
            addr = msg["address"]
            await self.disconnect(addr)     # also aborts an older connect to the same device still in flight
            me = asyncio.current_task()
            self.connecting[addr] = me
            try:
                async with self._ble_lock:
                    if await self.stop_scan():      # Windows connects more reliably after scan stops + settles
                        await asyncio.sleep(SCAN_STOP_SETTLE)
                    client = await self._connect_with_retries(addr)
            finally:
                if self.connecting.get(addr) is me:
                    del self.connecting[addr]
            if self.closed or not self.active:          # tab closed / taken over while connecting
                await safe_disconnect(client)
                raise BridgeError(NOT_ACTIVE if not self.closed else "GUI closed")
            self.clients[addr] = client
            if not client.is_connected:                 # dropped before the disconnect callback could see it
                self.clients.pop(addr, None)
                raise BridgeError("the device disconnected right after connecting – try again")
            entry = self.ble_devices.get(addr)
            target = entry[0] if entry else addr
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
ACTIVE = web.AppKey("active", dict)    # {"s": the Session that owns Bluetooth, or None}
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
        log.info("GUI connected (%d tab(s) open)", len(request.app[SESSIONS]))
        try:
            async for m in ws:
                if m.type == WSMsgType.TEXT:
                    try:
                        msg = json.loads(m.data)
                    except ValueError:
                        log.warning("ignoring a malformed message from the GUI")
                        continue
                    if isinstance(msg, dict):
                        session.dispatch(msg)
        finally:
            request.app[SESSIONS].discard(session)
            was_active = request.app[ACTIVE]["s"] is session
            if was_active:
                request.app[ACTIVE]["s"] = None
            await session.close()
            log.info("GUI disconnected%s", " – Bluetooth devices released" if was_active else " (passive tab)")
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

    async def ping(_request):
        # lets a copy of flux-s.html that was opened directly from disk (file://) find the bridge and switch to it
        return web.json_response({"app": "flux-bridge", "page": "/" + PAGE}, headers={"Access-Control-Allow-Origin": "*"})

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
    app[ACTIVE] = {"s": None}
    app[STOP] = asyncio.Event()
    app.on_shutdown.append(on_shutdown)
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/shutdown", shutdown_handler)
    app.router.add_get("/", index)
    app.router.add_get("/ping", ping)
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
    for folder in (WORKOUT_DIR, RIDE_DIR):
        ensure_folder(folder)
    print(f"Settings file:  {CONFIG_FILE}")
    print(f"Workouts:       {WORKOUT_DIR}")
    print(f"Rides:          {RIDE_DIR}")
    print(f"Log:            {LOG_DIR / 'flux_bridge.log'}")
    n = await asyncio.to_thread(finalize_stale_rides)
    if n:
        print(f"Finished {n} ride(s) that had no end.")
    print("Stop it with Ctrl+C, the 'Stop bridge' button in the GUI, or:  python flux_bridge.py --stop")
    if open_browser:
        async def open_unless_a_tab_reconnects():
            # A tab left open from an earlier run reconnects by itself within a few seconds.
            # Opening a second tab then would put two controllers on the trainer.
            await asyncio.sleep(OPEN_BROWSER_WAIT)
            if app[SESSIONS]:
                print("An open app tab reconnected - using it (no new browser tab opened).")
            else:
                webbrowser.open(url)
        open_task = asyncio.get_running_loop().create_task(open_unless_a_tab_reconnects())  # noqa: F841 (keep a reference)
    try:
        await app[STOP].wait()
    finally:
        await runner.cleanup()      # closes GUI sessions -> disconnects trainer and HR strap
        print(f"Bridge stopped - port {port} is free.")


def main():
    global WORKOUT_DIR, RIDE_DIR, CONFIG_FILE, LOG_DIR
    ap = argparse.ArgumentParser(description="Flux S GUI + Bluetooth LE bridge")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-browser", action="store_true", help="don't open the GUI automatically")
    ap.add_argument("--stop", action="store_true", help="stop the bridge running on --port (frees the port), then exit")
    ap.add_argument("--replace", action="store_true", help="if a bridge is already running, stop it and start this one")
    ap.add_argument("--workouts", default=str(WORKOUT_DIR), help="folder for saved workouts (default: ./Workouts)")
    ap.add_argument("--rides", default=str(RIDE_DIR), help="folder for recorded rides (default: ./Rides)")
    ap.add_argument("--config", default=str(CONFIG_FILE), help="settings file (default: ./flux_config.json)")
    ap.add_argument("--logs", default=str(LOG_DIR), help="folder for the log files (default: ./Logs)")
    args = ap.parse_args()
    WORKOUT_DIR = Path(os.path.abspath(args.workouts))   # abspath keeps the name as written (resolve() would
    RIDE_DIR = Path(os.path.abspath(args.rides))         # return the old 'rides' casing on Windows)
    CONFIG_FILE = Path(os.path.abspath(args.config))
    LOG_DIR = Path(os.path.abspath(args.logs))
    if not (args.stop or "--help" in sys.argv):
        setup_logging()
        log.info("bridge %d starting (Python %s, bleak %s, aiohttp %s)", BRIDGE_VERSION, sys.version.split()[0],
                 _pkg_version("bleak"), _pkg_version("aiohttp"))
    else:
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


def _pkg_version(name):
    try:
        from importlib.metadata import version
        return version(name)
    except Exception:
        return "?"


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
