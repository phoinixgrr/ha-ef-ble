import sys, types, asyncio, importlib.util, time

# --- stub the HA / aiohttp surface the module imports -----------------------
# Importing harness installs them. This used to be duplicated inline here, which meant
# every new HA import in the integration had to be stubbed in three places and the
# suites silently drifted apart on what "Home Assistant" looks like.
import harness  # noqa: F401

spec = importlib.util.spec_from_file_location("efi", "mod_under_test.py")
efi = importlib.util.module_from_spec(spec); spec.loader.exec_module(efi)

# --- fakes mirroring eflib semantics ---------------------------------------
class FakeConn:
    is_connected = True
class FakeDevice:
    def __init__(self, tag):
        self.tag = tag
        self._conn = FakeConn()
        self.listeners = []
    @property
    def is_connected(self):                      # same expression as devicebase
        return self._conn is not None and self._conn.is_connected
    def on_message_processed(self, cb):
        self.listeners.append(cb)
        def cancel(): self.listeners.remove(cb)
        return cancel
    def kill(self):                              # what disconnect() does
        self._conn = None

class FakeEntry:
    def __init__(self, title, dev): self.title = title; self.runtime_data = dev
class FakeCE:
    def __init__(self, entries): self._e = entries
    def async_entries(self, domain): return self._e
class FakeHass:
    def __init__(self, entries): self.config_entries = FakeCE(entries)

A = FakeDevice("A")
entry = FakeEntry(efi.TARGET_TITLE, A)
inj = efi.Injector(FakeHass([entry]))

fails = []
def ck(cond, msg):
    print(("PASS  " if cond else "FAIL  ") + msg)
    if not cond: fails.append(msg)

# 1. first resolution attaches to A
d = inj._ready_device()
ck(d is A, "first _ready_device returns live device A")
ck(inj.device_swaps == 1, "attach counted as swap #1 (was %d)" % inj.device_swaps)
ck(A.listeners == [inj._on_msg], "listener attached to A")

# 2. repeat calls must NOT re-attach or double-count
d = inj._ready_device(); d = inj._ready_device()
ck(inj.device_swaps == 1, "stable device does not re-attach (swaps=%d)" % inj.device_swaps)
ck(len(A.listeners) == 1, "no duplicate listeners on A (n=%d)" % len(A.listeners))

# 3. THE BUG: ef_ble reload -> A disconnected+abandoned, B is the new live object
A.kill()
B = FakeDevice("B")
entry.runtime_data = B
ck(not A.is_connected, "abandoned A reports not connected (_conn is None)")
d = inj._ready_device()
ck(d is B, "after reload we follow the NEW object B, not stale A  <-- the fix")
ck(inj.device_swaps == 2, "swap detected and counted (swaps=%d)" % inj.device_swaps)
ck(A.listeners == [], "listener detached from dead A")
ck(B.listeners == [inj._on_msg], "listener re-attached to B")

# 4. genuine outage: live object present but link down -> None, no exception
B.kill()
ck(inj._ready_device() is None, "link down yields None instead of a doomed send")

# 5. no device at all
entry.runtime_data = None
ck(inj._ready_device() is None, "missing runtime_data yields None")

# 6. backoff grows and caps, and resets
slept = []
real_sleep = asyncio.sleep
async def fake_sleep(s, *a, **k): slept.append(s)
efi.asyncio.sleep = fake_sleep
async def drive():
    for _ in range(12):
        await inj._not_ready("test")
asyncio.get_event_loop_policy().new_event_loop().run_until_complete(drive())
ck(slept[0] == efi.NOT_READY_BACKOFF_START, "backoff starts at %.0fs" % slept[0])
ck(slept[1] > slept[0], "backoff grows (%s -> %s)" % (slept[0], slept[1]))
ck(max(slept) == efi.NOT_READY_BACKOFF_MAX, "backoff caps at %.0fs" % max(slept))
ck(all(s <= efi.NOT_READY_BACKOFF_MAX for s in slept), "backoff never exceeds cap")
ck(inj.skipped_not_ready == 12, "not_ready counted (%d)" % inj.skipped_not_ready)

# 7. _pace with REASSERT_ON_CLOUD False must not recurse, and now waits to a
#    DEADLINE rather than sleeping a flat period after the cycle's work.
efi.REASSERT_ON_CLOUD = False
loop = asyncio.new_event_loop()
try:
    # Deadline already in the past (fresh injector): nothing to wait for. The old
    # flat-sleep version burned a full period here even when it was already late.
    slept.clear()
    inj._deadline = 0.0
    loop.run_until_complete(inj._pace())
    ck(slept == [], "already past the deadline -> no sleep at all (%s)" % slept)

    # Deadline one period ahead: sleeps once, for the remainder only.
    slept.clear()
    inj._deadline = time.time()
    loop.run_until_complete(inj._pace())
    ck(len(slept) == 1, "_pace sleeps once, no recursion (%s)" % slept)
    ck(slept and 0 < slept[0] <= efi.POLL_PERIOD_SEC + 1e-6,
       "waits at most the poll period (%.3fs of %.3fs), never period+work"
       % (slept[0] if slept else -1, efi.POLL_PERIOD_SEC))

    # A large arrears must not be repaid as a burst of free cycles.
    slept.clear()
    inj._deadline = time.time() - 30.0
    loop.run_until_complete(inj._pace())
    ck(slept == [], "30s behind -> resyncs to now, does not fire 120 catch-up cycles")
    ck(inj._deadline >= time.time() - 1.0, "and the deadline was moved forward")
except RecursionError:
    ck(False, "_pace recursed (RecursionError)")
finally:
    loop.close()
efi.asyncio.sleep = real_sleep

print()
print("RESULT:", "ALL %d CHECKS PASSED" % 0 if not fails else "%d FAILURES" % len(fails))
sys.exit(1 if fails else 0)
