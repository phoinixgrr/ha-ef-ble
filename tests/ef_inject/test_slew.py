"""Tests for the rise slew limiter.

The limiter exists because of a measured failure, not a theoretical one: on
2026-08-26 two export events (-105W and -405W) happened with the meter steady
either side, nothing switching off, and injection age at 140-150ms. The loop was
healthy and on time; it was chasing a sub-second load spike and the inverter's
output arrived after the spike had gone. So the property under test is not "the
number is smaller" but the asymmetry: a RISE is rationed because overshoot lands
on the grid as export, a FALL is not because undershoot lands on the grid as
import, and only one of those is a compliance event.

The last section drives the REAL run() loop rather than only calling the method,
because the ordering inside the loop is half the correctness here: the clamp has
to be applied once per cycle, before the write gate and before the bias fade
reads the value, and after the A/B CLOUD arm has bailed out. A unit test of
_slew_limit alone would pass with every one of those wired wrong.
"""
import sys, asyncio, os, tempfile, time
from harness import (FakeShelly, FakeDevice, FakeEntry, FakeHass, ck, report,
                     load, install_pb_stub, touch, rm)

install_pb_stub()
efi = load()
efi._LOGGER.disabled = True

TMP = tempfile.mkdtemp()
efi.MODBUS_FILE = os.path.join(TMP, "modbus")
efi.SHADOW_FILE = os.path.join(TMP, "shadow")
efi.INTERLEAVE_FILE = os.path.join(TMP, "interleave")
efi.STOP_FILE = os.path.join(TMP, "stop")
efi.QUIET_FILE = os.path.join(TMP, "quiet")
efi.AB_FILE = os.path.join(TMP, "ab")
efi.CAUSATION_FILE = os.path.join(TMP, "causation")
efi.NOSLEW_FILE = os.path.join(TMP, "noslew")
# stat_flags() reads FLAG_FILES, not the module constant, so a test that only
# rebinds the constant would be testing a path nothing looks at.
efi.FLAG_FILES["noslew"] = efi.NOSLEW_FILE
efi.BIAS_FILE = os.path.join(TMP, "bias")
efi.BASELINE_SEC = 0
IDENT = (True, 3, efi.EXPECTED_SN, 0, 0)


def new_injector(dev=None):
    inj = efi.Injector(FakeHass([FakeEntry(efi.TARGET_TITLE, dev or FakeDevice())]))
    inj.identity = IDENT
    inj.flags = {}
    return inj


async def main():
    # ---- 1. the knob is registered, or it is unreachable ----------------
    ck("noslew" in efi.FLAG_FILES,
       "noslew is in FLAG_FILES, so stat_flags() actually looks for it")
    ck("noslew" in efi.stat_flags(), "stat_flags reports the noslew knob")
    ck(efi.SLEW_UP_MIN_W > 0 and efi.SLEW_UP_W_PER_SEC > 0,
       "both slew constants are positive")
    ck(efi.SLEW_RESET_SEC <= efi.STALE_AFTER_SEC,
       "the slew anchor expires no later than the reading itself does "
       "(reset=%.1fs stale=%.1fs)" % (efi.SLEW_RESET_SEC, efi.STALE_AFTER_SEC))

    # ---- 2. the anchor: first reading is never clamped ------------------
    inj = new_injector()
    t = 1000.0
    ck(inj._slew_limit(2000.0, t) == 2000.0,
       "the very first reading passes untouched, however large")
    ck(inj.slew_clamps == 0, "and is not counted as a clamp")

    # ---- 3. a fall passes instantly, at any size -----------------------
    inj = new_injector()
    inj._slew_limit(900.0, t)
    ck(inj._slew_limit(-800.0, t + 0.25) == -800.0,
       "a 1700W FALL passes through untouched and instantly")
    ck(inj.slew_clamps == 0, "a fall is never a clamp")
    ck(inj._slew_limit(-800.0, t + 0.5) == -800.0, "a flat reading is untouched")

    # ---- 4. ordinary noise is untouched, so there is no hidden cushion --
    # This is the whole argument for a rate limit over a min-over-window filter.
    # A settled reading has zero slew, so a settled house must see zero effect.
    inj = new_injector()
    noise = [90.0, 148.0, 62.0, 105.0, 38.0, 96.0, 132.0, 74.0, 118.0, 84.0]
    out = []
    for i, v in enumerate(noise):
        out.append(inj._slew_limit(v, t + 0.25 * i))
    ck(out == noise, "10 samples of real +38..+148W meter noise pass unchanged")
    ck(inj.slew_clamps == 0,
       "no clamps on noise, so the limiter adds no steady-state import")

    # The rise that noise is allowed is exactly SLEW_UP_MIN_W, tested at the edge
    # with dt small enough that the rate term cannot be what let it through.
    inj = new_injector()
    inj._slew_limit(100.0, t)
    dt = efi.SLEW_UP_MIN_W / efi.SLEW_UP_W_PER_SEC / 10.0    # rate term = MIN/10
    ck(inj._slew_limit(100.0 + efi.SLEW_UP_MIN_W, t + dt) == 100.0 + efi.SLEW_UP_MIN_W,
       "a rise of exactly SLEW_UP_MIN_W passes on the floor alone")
    inj = new_injector()
    inj._slew_limit(100.0, t)
    ck(inj._slew_limit(100.0 + efi.SLEW_UP_MIN_W + 1.0, t + dt)
       == 100.0 + efi.SLEW_UP_MIN_W,
       "one watt more is held to the floor")
    ck(inj.slew_clamps == 1 and abs(inj.slew_worst_w - 1.0) < 1e-9,
       "and the withheld watt is accounted (worst=%.1fW)" % inj.slew_worst_w)

    # ---- 4b. the floor is ONE-SHOT, so the RATE is what governs ---------
    # The first version of this granted SLEW_UP_MIN_W on every poll, which silently
    # floors the tracking rate at MIN/POLL_PERIOD_SEC whatever SLEW_UP_W_PER_SEC says:
    # 240W/s at the shipped 0.25s poll, so every rate below 240 would have behaved
    # identically to 240. Nothing misbehaved at the shipped 400W/s, because there the
    # rate term is the binding one anyway, which is exactly why no existing test
    # caught it. This is the test that does.
    inj = new_injector()
    slow = 40.0                      # W/s, far below MIN/POLL_PERIOD_SEC
    saved_rate = efi.SLEW_UP_W_PER_SEC
    efi.SLEW_UP_W_PER_SEC = slow
    try:
        inj._slew_limit(0.0, t)
        polls, out = 20, None
        for i in range(1, polls + 1):
            out = inj._slew_limit(5000.0, t + 0.25 * i)
        elapsed = 0.25 * polls
        budget = efi.SLEW_UP_MIN_W + slow * elapsed
        ck(out <= budget + 1.0,
           "%.0fs of sustained demand at %.0fW/s reaches ~%.0fW (one floor plus "
           "rate*time), not %d floors = %.0fW; got %.0fW"
           % (elapsed, slow, budget, polls, polls * efi.SLEW_UP_MIN_W, out))
        ck(out > efi.SLEW_UP_MIN_W,
           "and it is still climbing, so the one-shot did not become a hard cap "
           "(%.0fW)" % out)
        # The floor has to come back, or the second spike of the day would be met
        # with a bare rate term and the noise immunity above would be a one-off.
        inj._slew_limit(0.0, t + 6.0)                    # a fall ends the episode
        again = inj._slew_limit(5000.0, t + 6.25)
        ck(abs(again - efi.SLEW_UP_MIN_W) < 1e-6,
           "after a fall the floor is granted again (%.0fW)" % again)
    finally:
        efi.SLEW_UP_W_PER_SEC = saved_rate

    # Spent only when actually needed. A wobble that fits inside the rate term must
    # not consume the floor, or a quiet spell would leave the next real spike
    # unprotected for having done nothing wrong.
    inj = new_injector()
    inj._slew_limit(100.0, t)
    for i in range(1, 6):
        inj._slew_limit(100.0 + (10 if i % 2 else -10), t + 0.25 * i)
    ck(inj._slew_floor_ready,
       "wobbles inside the rate term leave the one-shot floor unspent")

    # ---- 5. the measured failure: a brief spike is attenuated ----------
    # The 08:53:27 event, reconstructed: steady ~90W, a 500W spike for one poll
    # period, then back. Without the limiter the injected value carries the whole
    # spike and the inverter ramps 500W into a load that has already gone.
    inj = new_injector()
    inj._slew_limit(90.0, t)
    spiked = inj._slew_limit(590.0, t + 0.25)
    ck(spiked < 590.0, "a 500W spike is not injected in full (%.0fW)" % spiked)
    ck(spiked <= 90.0 + max(efi.SLEW_UP_MIN_W, efi.SLEW_UP_W_PER_SEC * 0.25),
       "it is held to the anchor plus one period's allowance")
    ck(inj._slew_limit(90.0, t + 0.5) == 90.0,
       "and when the spike ends the value drops back instantly, no ramp down")

    # ---- 6. a genuine sustained step is still tracked, just later ------
    # The heat pump starting: +1350W and it stays. The limiter must not cap the
    # value permanently, or the loop would under-supply for as long as the load
    # lasted and import would sit ~1kW high all cycle.
    inj = new_injector()
    inj._slew_limit(90.0, t)
    v, elapsed = 0.0, 0.0
    for i in range(1, 40):
        elapsed = 0.25 * i
        v = inj._slew_limit(1440.0, t + elapsed)
        if v >= 1440.0:
            break
    ck(v >= 1440.0, "a sustained 1350W step is fully tracked in the end (%.0fW)" % v)
    ck(elapsed <= 4.0,
       "and it gets there in %.2fs, not eventually" % elapsed)
    # Cost of that delay, stated rather than assumed: the grid serves the deficit.
    ck(elapsed * 1350.0 / 3600.0 < 2.0,
       "the catch-up costs under 2Wh of import per step (%.2fWh)"
       % (elapsed * 1350.0 / 3600.0))

    # ---- 7. the anchor expires, and a stale anchor never holds a value down
    inj = new_injector()
    inj._slew_limit(50.0, t)
    after_gap = inj._slew_limit(1800.0, t + efi.SLEW_RESET_SEC + 0.01)
    ck(after_gap == 1800.0,
       "after a read gap longer than SLEW_RESET_SEC the anchor is abandoned")
    ck(inj.slew_clamps == 0, "and the re-anchor is not counted as a clamp")
    inj = new_injector()
    inj._slew_limit(50.0, t)
    ck(inj._slew_limit(1800.0, t) < 1800.0,
       "dt of exactly zero clamps rather than dividing by it")
    inj = new_injector()
    inj._slew_limit(50.0, t)
    ck(inj._slew_limit(1800.0, t - 5.0) == 1800.0,
       "a clock that went backwards re-anchors instead of clamping forever")

    # ---- 8. the kill switch, in both directions ------------------------
    inj = new_injector()
    inj.flags = {"noslew": True}
    inj._slew_limit(90.0, t)
    ck(inj._slew_limit(1590.0, t + 0.25) == 1590.0,
       "with the noslew file present nothing is clamped")
    ck(inj.slew_clamps == 0, "and nothing is counted")
    # Re-enabling must not clamp against an anchor from before the disable, which
    # is why the disabled path still tracks it.
    inj.flags = {}
    ck(inj._slew_limit(1600.0, t + 0.5) == 1600.0,
       "re-enabling clamps against the LAST seen value, not a stale one")

    # ---- 9. the invariant that makes a bug here safe ------------------
    # Whatever else is wrong, the output must never exceed the input: the limiter
    # can only ever command less inverter output, so its failure mode is import.
    inj = new_injector()
    seq = [0.0, 500.0, -500.0, 2000.0, 90.0, 90.0, -3000.0, 1200.0, 1200.0, 0.0]
    worst = None
    for i, v in enumerate(seq):
        o = inj._slew_limit(v, t + 0.25 * i)
        if o > v:
            worst = (v, o)
    ck(worst is None,
       "across a mixed sequence the output never exceeds the reading (%r)" % (worst,))

    # ---- 10. wired into the real loop, in the right place --------------
    srv = await FakeShelly(100.0).start()
    efi.MODBUS_PORT = srv.port
    touch(efi.MODBUS_FILE)
    rm(efi.NOSLEW_FILE)
    rm(efi.AB_FILE)
    rm(efi.STOP_FILE)
    efi.write_text_atomic(efi.BIAS_FILE, "0")   # isolate the clamp from the bias
    # Faster than shipped so a step and its recovery both fit in a test window. The
    # rate is scaled up to match: now that the floor is a one-shot it cannot carry a
    # sustained rise, so at the shipped 400W/s a 1500W step would need 3.6s to track
    # and this section would be testing the test's own timeout. 4000W/s over a 0.02s
    # poll gives 80W per cycle, which clamps the step hard on arrival and still
    # catches up inside the window.
    efi.POLL_PERIOD_SEC = 0.02
    efi.KEEPALIVE_SEC = 0.04
    efi.SLEW_UP_W_PER_SEC = 4000.0

    def bind(inj):
        """The Injector points at the real Shellys; aim both meters at the fake."""
        inj._mb = efi.ModbusMeter("127.0.0.1", "solar")
        inj._mb2 = efi.ModbusMeter("127.0.0.1", "grid")
        return inj

    dev = FakeDevice()
    inj = bind(new_injector(dev))
    task = asyncio.create_task(inj.run())
    await asyncio.sleep(0.3)
    sent_before = len(dev.sends)
    srv.value = 1600.0                          # a 1500W step, held
    await asyncio.sleep(0.1)                    # ~5 cycles: not enough to catch up
    mid = [w for _t, w in dev.sends[sent_before:]]
    await asyncio.sleep(0.4)                    # now let it catch up
    caught = [w for _t, w in dev.sends[sent_before:]]
    srv.value = 100.0
    n_drop = len(dev.sends)
    await asyncio.sleep(0.3)
    inj.running = False
    try:
        await asyncio.wait_for(task, timeout=3.0)
    except asyncio.TimeoutError:
        task.cancel()
        ck(False, "run() did not stop within 3s of running=False")

    ck(inj.slew_clamps > 0,
       "the live loop actually clamped the step (%d clamps)" % inj.slew_clamps)
    ck(mid and max(mid) < 1600.0,
       "no injected value carried the full 1500W step immediately (max %s)"
       % (max(mid) if mid else None))
    ck(caught and max(caught) >= 1600.0,
       "but the value does reach the real reading once the step persists (max %s)"
       % (max(caught) if caught else None))
    ck(inj.slew_worst_w > 0 and inj.slew_held_w >= inj.slew_worst_w,
       "worst and cumulative held watts are both recorded (worst=%.0f held=%.0f)"
       % (inj.slew_worst_w, inj.slew_held_w))
    # Every send after the drop, not the last N: MIN_SEND_GAP_SEC throttles writes to
    # ~6/s, so a fixed tail window silently includes the pre-drop keepalive and the
    # assertion becomes a function of the sleep length rather than of the behaviour.
    tail = [w for _t, w in dev.sends[n_drop:]]
    ck(tail and max(tail) <= 110.0,
       "and the loop came back down to the low reading, so nothing latched high "
       "(%d sends after the drop, max %s)" % (len(tail), max(tail) if tail else None))

    # The CLOUD arm must not move the counters: it injects nothing, so counting
    # its cycles would make an A/B comparison of the limiter meaningless.
    #
    # ab_mode is recomputed from the wall clock every cycle, so assigning it by hand
    # is overwritten on the first pass. Pick a block length that puts NOW in an odd
    # block instead, which drives the real selection down the CLOUD arm, and one long
    # enough that the block cannot flip mid-test.
    blk = next(c for c in range(600, 1200) if int(time.time() // c) % 2 == 1
               and (c - time.time() % c) > 5.0)
    efi.AB_BLOCK_SEC = blk
    touch(efi.AB_FILE)
    srv.value = 100.0
    inj2 = bind(new_injector(FakeDevice()))
    task = asyncio.create_task(inj2.run())
    await asyncio.sleep(0.2)
    srv.value = 1600.0
    await asyncio.sleep(0.3)
    inj2.running = False
    try:
        await asyncio.wait_for(task, timeout=3.0)
    except asyncio.TimeoutError:
        task.cancel()
    ck(inj2.ab_mode == "CLOUD",
       "the A/B block really selected the CLOUD arm (%r)" % inj2.ab_mode)
    ck(inj2.polls > 0 and inj2.slew_clamps == 0,
       "the A/B CLOUD arm polls (%d) but records no clamps, because it injects "
       "nothing" % inj2.polls)

    await srv.stop()
    rm(efi.MODBUS_FILE)
    rm(efi.AB_FILE)
    return report()


sys.exit(asyncio.run(main()))
