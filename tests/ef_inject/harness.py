"""Shared test scaffolding for ef_inject.

Stubs out the Home Assistant and aiohttp imports, loads the module under test,
and provides a fake Shelly Modbus server plus fake ef_ble device objects. Kept in
one place so the two suites cannot drift apart on what "the device" behaves like.
"""
import sys, types, asyncio, importlib.util, struct, os, time

# ---- import stubs --------------------------------------------------------
_ha = types.ModuleType("homeassistant"); _ha.__path__ = []
_core = types.ModuleType("homeassistant.core")
class HomeAssistant: pass
_core.HomeAssistant = HomeAssistant
_helpers = types.ModuleType("homeassistant.helpers"); _helpers.__path__ = []
_ac = types.ModuleType("homeassistant.helpers.aiohttp_client")
_ac.async_get_clientsession = lambda hass: None
_aio = types.ModuleType("aiohttp")
class _CE(Exception): pass
_aio.ClientError = _CE
_aio.ClientSession = object
_aio.ClientTimeout = lambda **k: None
for _n, _m in [("homeassistant", _ha), ("homeassistant.core", _core),
               ("homeassistant.helpers", _helpers),
               ("homeassistant.helpers.aiohttp_client", _ac), ("aiohttp", _aio)]:
    sys.modules[_n] = _m


def install_pb_stub():
    """Make `from custom_components.ef_ble.eflib.pb import bk_series_pb2` work.

    run() imports it lazily, so the loop cannot be exercised without this.
    """
    names = ["custom_components", "custom_components.ef_ble",
             "custom_components.ef_ble.eflib", "custom_components.ef_ble.eflib.pb"]
    for n in names:
        if n not in sys.modules:
            m = types.ModuleType(n); m.__path__ = []
            sys.modules[n] = m
    pb = types.ModuleType("custom_components.ef_ble.eflib.pb.bk_series_pb2")

    class CloudMeter:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class ConfigWrite:
        def __init__(self, cfg_cloud_metter=None):
            self.cfg_cloud_metter = cfg_cloud_metter

    pb.CloudMeter = CloudMeter
    pb.ConfigWrite = ConfigWrite
    sys.modules["custom_components.ef_ble.eflib.pb.bk_series_pb2"] = pb
    sys.modules["custom_components.ef_ble.eflib.pb"].bk_series_pb2 = pb
    return pb


def load(path="mod_under_test.py"):
    spec = importlib.util.spec_from_file_location("efi", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---- assertions ----------------------------------------------------------
fails = []


def ck(cond, msg):
    print(("PASS  " if cond else "FAIL  ") + msg)
    if not cond:
        fails.append(msg)


def report():
    print()
    print("FAILED: %d -> %s" % (len(fails), fails) if fails else "ALL CHECKS PASSED")
    return 1 if fails else 0


# ---- fake Shelly Modbus TCP server --------------------------------------
class FakeShelly:
    """Minimal Modbus TCP server with the real device's quirks: float32 with the
    LOW word first, and an exception response for anything but FC4."""

    def __init__(self, value=0.0):
        self.value = value
        self.mode = "ok"      # ok | wrong_tid | short_count | drop | garbage
        self.requests = []
        self.connections = 0
        self.server = None
        self.port = None
        self._writers = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        # Close live handler sockets first. server.wait_closed() waits for open
        # connections on modern Python, and the client keeps one open, so closing
        # only the listener deadlocks.
        for w in self._writers:
            try:
                w.close()
            except Exception:
                pass
        self._writers.clear()
        self.server.close()
        try:
            await self.server.wait_closed()
        except Exception:
            pass

    async def _handle(self, reader, writer):
        self.connections += 1
        self._writers.append(writer)
        try:
            while True:
                head = await reader.readexactly(8)
                tid, proto, ln, unit, fc = struct.unpack(">HHHBB", head)
                rest = await reader.readexactly(ln - 2)
                addr, count = struct.unpack(">HH", rest)
                self.requests.append((tid, unit, fc, addr, count))

                if self.mode == "drop":
                    writer.close()
                    return
                if fc != 4 or self.mode == "garbage":
                    # The real meter answers FC3 with exception code 2.
                    writer.write(struct.pack(">HHHBBB", tid, 0, 3, unit, fc | 0x80, 2))
                    await writer.drain()
                    continue

                raw = struct.pack(">f", self.value)      # big-endian float32
                payload = raw[2:4] + raw[0:2]            # LOW word first
                out_tid = (tid + 7) & 0xFFFF if self.mode == "wrong_tid" else tid
                nbytes = 2 if self.mode == "short_count" else 4
                writer.write(
                    struct.pack(">HHHBBB", out_tid, 0, 3 + nbytes, unit, fc, nbytes)
                    + payload[:nbytes]
                )
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass


# ---- fake ef_ble device -------------------------------------------------
class FakeConn:
    is_connected = True


class FakeDevice:
    def __init__(self, tag="A", send_delay=0.0):
        self.tag = tag
        self._conn = FakeConn()
        self.listeners = []
        self.sends = []          # (t, phase_c_power)
        self.send_delay = send_delay
        self.fail_next = 0

    @property
    def is_connected(self):
        return self._conn is not None and self._conn.is_connected

    def on_message_processed(self, cb):
        self.listeners.append(cb)

        def cancel():
            if cb in self.listeners:
                self.listeners.remove(cb)
        return cancel

    async def _send_config_packet(self, msg):
        if self.send_delay:
            await asyncio.sleep(self.send_delay)
        if self.fail_next > 0:
            self.fail_next -= 1
            raise RuntimeError("simulated BLE write failure")
        self.sends.append((time.time(), msg.cfg_cloud_metter.phase_c_power))

    def kill(self):
        self._conn = None


class FakeEntry:
    def __init__(self, title, dev):
        self.title = title
        self.runtime_data = dev


class FakeCE:
    def __init__(self, entries):
        self._e = entries

    def async_entries(self, domain):
        return self._e


class FakeHass:
    def __init__(self, entries):
        self.config_entries = FakeCE(entries)
        self.data = {}
        self.executor_jobs = 0

    async def async_add_executor_job(self, fn, *a):
        """Mirrors HA. Present so the tests exercise the SAME call path that HA's
        loop-blocking detector forced on us, rather than a synchronous shortcut
        that would pass while the shipped code was flagged."""
        self.executor_jobs += 1
        return fn(*a)


def touch(p):
    open(p, "w").close()


def rm(p):
    try:
        os.remove(p)
    except OSError:
        pass
