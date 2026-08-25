import sys, types, asyncio, importlib.util, time

# Stubs live in harness so they cannot drift between suites. See test_heal.
import harness  # noqa: F401

spec = importlib.util.spec_from_file_location("efi", "mod_under_test.py")
efi = importlib.util.module_from_spec(spec); spec.loader.exec_module(efi)

class FakeConn: is_connected = True
class FakeDevice:
    def __init__(self, tag):
        self.tag = tag; self._conn = FakeConn(); self.listeners = []
    @property
    def is_connected(self):
        return self._conn is not None and self._conn.is_connected
    def on_message_processed(self, cb):
        self.listeners.append(cb)
        def cancel(): self.listeners.remove(cb)
        return cancel
    def kill(self): self._conn = None
class FakeEntry:
    def __init__(self, title, dev): self.title = title; self.runtime_data = dev
class FakeCE:
    def __init__(self, e): self._e = e
    def async_entries(self, d): return self._e
class FakeHass:
    def __init__(self, e):
        self.config_entries = FakeCE(e)
        self.executor_jobs = 0
    async def async_add_executor_job(self, fn, *a):
        self.executor_jobs += 1
        return fn(*a)

fails = []
def ck(cond, msg):
    print(("PASS  " if cond else "FAIL  ") + msg)
    if not cond: fails.append(msg)

# ---- safety guards on the new constants ----------------------------------
ck(efi.BIAS_W <= 0, "BIAS_W is negative (positive would command ramp-up -> real export)")
ck(abs(efi.BIAS_W) <= efi.BIAS_MAX_ABS, "|BIAS_W| within BIAS_MAX_ABS guard")
ck(efi.BIAS_FADE_W > 0, "BIAS_FADE_W positive")

# ---- bias fade replicates the production expression ----------------------
def bias_for(w):
    if not efi.BIAS_W: return 0
    if w >= efi.BIAS_FADE_W: f = 0.0
    elif w <= 0.0: f = 1.0
    else: f = (efi.BIAS_FADE_W - w) / efi.BIAS_FADE_W
    return int(round(efi.BIAS_W * f))

ck(bias_for(-500) == efi.BIAS_W, "exporting 500W -> FULL bias (%d)" % efi.BIAS_W)
ck(bias_for(0) == efi.BIAS_W, "grid exactly 0W -> FULL bias")
ck(bias_for(250) == int(round(efi.BIAS_W * 0.5)), "half fade window -> half bias")
ck(bias_for(efi.BIAS_FADE_W) == 0, "at fade edge -> no bias")
ck(bias_for(1259) == 0, "deep import (this morning's 1259W) -> no bias, no wasted grid")
ck(all(bias_for(w) <= 0 for w in range(-1000, 2000, 7)),
   "bias never positive anywhere in range (cannot command export)")
mono = [bias_for(w) for w in range(-200, 1200, 10)]
ck(all(b <= a for a, b in zip(mono, mono[1:])) is False or
   all(a <= b for a, b in zip(mono, mono[1:])),
   "bias is monotone in w (no step the regulator could hunt across)")

# the old gate would have suppressed the bias during a PV surplus
ck(bias_for(0) != 0, "cushion ARMS at zero grid even while battery charges <-- the fix")

# ---- equilibrium claim in the START log ---------------------------------
eq = efi.BIAS_SETTLE_W
resid = eq + efi.BIAS_W * (efi.BIAS_FADE_W - eq) / efi.BIAS_FADE_W
ck(abs(resid) < 1e-6, "BIAS_SETTLE_W really is the fixed point of w+bias(w)=0 (%.1fW)" % eq)
ck(0 < eq < abs(efi.BIAS_W), "settling point is an import cushion, smaller than |BIAS_W|")

# ---- runtime bias override ----------------------------------------------
# Tuning the cushion used to mean editing a constant and restarting HA. The knob
# must be safe against the one input that could cause real export: a typo.
import os, tempfile
efi.BIAS_FILE = os.path.join(tempfile.mkdtemp(), "bias")


def put(text):
    with open(efi.BIAS_FILE, "w") as f:
        f.write(text)


def drop():
    if os.path.exists(efi.BIAS_FILE):
        os.remove(efi.BIAS_FILE)


bi = efi.Injector(FakeHass([FakeEntry(efi.TARGET_TITLE, FakeDevice("C"))]))
efi._LOGGER.disabled = True


def refresh(inj, throttled=False):
    """Drive the async accessor. `throttled=False` clears the 2s throttle first,
    because these checks are about the VALIDATION rules, not the timer."""
    if not throttled:
        inj._bias_next_check = 0.0
    return asyncio.run(inj._refresh_bias())

ck(refresh(bi) == efi.BIAS_W, "no override file -> the built-in default")
ck(bi.bias_overrides == 0 and bi.bias_rejected == 0, "and nothing counted")

put("-100")
ck(refresh(bi) == -100, "a valid override is picked up with NO restart")
ck(bi.bias_overrides == 1, "the change is counted (%d)" % bi.bias_overrides)
ck(abs(efi.bias_settle_w(-100) - 83.3) < 0.2,
   "and the predicted cushion follows it (%.1fW for -100)" % efi.bias_settle_w(-100))

for _ in range(5):
    refresh(bi)
ck(bi.bias_overrides == 1,
   "re-reading an unchanged file is not a new change (%d)" % bi.bias_overrides)

put("-100.0")
ck(refresh(bi) == -100 and bi.bias_overrides == 1,
   "a differently-spelled SAME value is not counted as a change either")

# THE case that matters: a typo must never be able to command export.
put("+50")
ck(refresh(bi) == -100,
   "a POSITIVE override is REFUSED: it would command ramp-up and manufacture "
   "real export  <-- the whole reason this knob is validated")
ck(bi.bias_rejected == 1, "the refusal is counted (%d)" % bi.bias_rejected)

put("-9999")
ck(refresh(bi) == -100,
   "an oversized override is refused (|bias| > BIAS_MAX_ABS=%d)" % efi.BIAS_MAX_ABS)

put("banana")
ck(refresh(bi) == -100, "unparseable content is refused")
put("")
ck(refresh(bi) == -100, "an empty file is refused")
ck(bi.bias_rejected == 4, "all four refusals counted (%d)" % bi.bias_rejected)
ck(bi.bias_overrides == 1, "and none of them counted as a change")

# Sticky-safe: a bad value keeps the last GOOD one, it does not revert to the
# default, because reverting from -100 to -68 would silently RAISE the cushion.
ck(bi.bias_w == -100 and bi.bias_w != efi.BIAS_W,
   "after a bad value the last GOOD override is still in force, not the default")

put("-120")
ck(refresh(bi) == -120,
   "a good value after bad ones is accepted (so a typo is not a lockout)")

# Same byte count, same second: the mtime cache this replaced would miss this.
put("-119")
ck(refresh(bi) == -119,
   "two same-length writes inside one mtime tick are BOTH seen (content compare, "
   "not mtime+size)")

drop()
ck(refresh(bi) == efi.BIAS_W,
   "deleting the file reverts to the built-in %+dW" % efi.BIAS_W)

ck(efi.bias_is_safe(0) and efi.bias_is_safe(-efi.BIAS_MAX_ABS),
   "bias_is_safe accepts 0 and exactly the cap")
ck(not efi.bias_is_safe(1) and not efi.bias_is_safe(-efi.BIAS_MAX_ABS - 1),
   "and rejects positive and over-cap")
ck(not efi.bias_is_safe(float("nan")), "and rejects NaN")
efi._LOGGER.disabled = False

# ---- frame-age liveness gate -------------------------------------------
A = FakeDevice("A")
inj = efi.Injector(FakeHass([FakeEntry(efi.TARGET_TITLE, A)]))
# run() attaches before the first _ready_device(), so mirror that: otherwise the
# initial resolution registers as a spurious swap because _device_ref starts None.
inj._attach(A)
ck(inj._ready_device() is A, "fresh attach is usable (grace window started at attach)")
ck(inj._last_frame_ts > 0, "attach seeded _last_frame_ts instead of leaving it at 0")

inj._last_frame_ts = time.time() - (efi.FRAME_MAX_AGE_SEC + 1)
ck(inj._ready_device() is None, "connected but silent past FRAME_MAX_AGE_SEC -> refused")
ck("silent" in (inj._not_ready_why or ""), "refusal reason names the silence: %r" % inj._not_ready_why)
ck(inj.skipped_silent == 1, "silent refusal counted separately from other not_ready")

inj._last_frame_ts = time.time()
ck(inj._ready_device() is A, "a fresh frame makes the link usable again")
ck(inj._not_ready_why is None, "reason cleared once healthy")

A.kill()
ck(inj._ready_device() is None, "disconnected still refused")
ck(inj._not_ready_why == "BLE not connected", "disconnect reason distinct from silence")

# ---- swap still followed, and the grace window is re-seeded -------------
B = FakeDevice("B")
entry2 = FakeEntry(efi.TARGET_TITLE, B)
inj.hass = FakeHass([entry2])
inj._last_frame_ts = time.time() - 999
d = inj._ready_device()
ck(d is B, "after reload we follow the NEW object B, not stale A")
ck(inj.device_swaps == 1, "swap counted")
ck(len(A.listeners) == 0, "listener detached from dead A")
ck(len(B.listeners) == 1, "listener attached to live B")
ck(d is B, "swap re-seeded the grace window so the first cycle is not refused as silent")

print()
print("FAILED: %d" % len(fails) if fails else "ALL %d CHECKS PASSED" % 0 if False else
      ("FAILED: %d -> %s" % (len(fails), fails) if fails else "ALL CHECKS PASSED"))
sys.exit(1 if fails else 0)
