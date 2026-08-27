"""Tests for the cloud overwrite counter-kick.

Like the slew limiter, this exists because of a measurement rather than a theory. The
Ultras stop feeding the house the instant their cloud link drops, so the cloud is inside
their control loop and writes phase C itself: 2% of telemetry frames on 2026-08-26
carried a value we did not send, ~51 an hour, and correlating every reverse-flow sample
that day against those events put 9 of 16 within one status tick, carrying 85% of the
total reverse magnitude against a 14% base rate. Those frames never pass through
_slew_limit, so the limiter is structurally blind to them.

The property under test is therefore NOT "the kick fires". It is the set of things that
make a kick safe to fire at all:

  - it only ever LOWERS the injected value, so every failure mode is import
  - it is armed only by a POSITIVE delta, because a cloud value below ours already
    commands less output and needs no help
  - its size is the MEASURED delta, not a constant, and it is bounded
  - a burst of overwrites replaces the kick rather than stacking it, so the pull-down
    cannot be driven arbitrarily deep by something outside our control
  - it is spent on confirmed WRITES, not on cycles, so a failed send does not let the
    correction expire while the ramp it corrects is still up

The last section drives the real run() loop with a device that actually emits telemetry,
because arming happens in the frame callback and spending happens in the send path, and
a unit test of _arm_kick alone would pass with those wired to nothing.
"""
import sys, asyncio, os, tempfile, time
from harness import (FakeShelly, FakeDevice, FakeEntry, FakeHass, ck, report,
                     load, install_pb_stub, touch, rm)

pb = install_pb_stub()
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
efi.NOKICK_FILE = os.path.join(TMP, "nokick")
# stat_flags() reads FLAG_FILES, which was built at import from the real paths, so a
# test that only rebound the constant would be watching a file nothing looks at.
efi.FLAG_FILES["noslew"] = efi.NOSLEW_FILE
efi.FLAG_FILES["nokick"] = efi.NOKICK_FILE
efi.BIAS_FILE = os.path.join(TMP, "bias")
efi.BASELINE_SEC = 0
IDENT = (True, 3, efi.EXPECTED_SN, 0, 0)


def new_injector(dev=None):
    inj = efi.Injector(FakeHass([FakeEntry(efi.TARGET_TITLE, dev or FakeDevice())]))
    inj.identity = IDENT
    inj.flags = {}
    return inj


class _Desc:
    name = "DisplayPropertyUpload"


class Frame:
    """A DisplayPropertyUpload as the regulator reads it.

    Mirrors the three things _on_msg actually inspects: the descriptor name, the
    presence of the `cloud_metter` block (note the device's spelling, which differs from
    the `cfg_cloud_metter` field we WRITE), and the two power figures logged alongside.
    """

    DESCRIPTOR = _Desc()

    def __init__(self, phase_c, grid=0.0, batt=0.0, has_meter=True):
        self.cloud_metter = pb.CloudMeter(
            has_meter=has_meter, model=3, sn=efi.EXPECTED_SN,
            phase_a_power=0, phase_b_power=0, phase_c_power=phase_c,
        )
        self.pow_get_sys_grid = grid
        self.pow_get_bp_cms = batt

    def HasField(self, name):
        return name == "cloud_metter"


async def main():
    # ---- 1. the knob is registered, or it is unreachable ----------------
    ck("nokick" in efi.FLAG_FILES,
       "nokick is in FLAG_FILES, so stat_flags() actually looks for it")
    ck("nokick" in efi.stat_flags(), "stat_flags reports the nokick knob")
    ck(efi.CLOUD_KICK_MAX_W > 0 and efi.CLOUD_KICK_CYCLES >= 1,
       "the bound and the duration are both meaningful")
    ck(0.0 < efi.CLOUD_KICK_GAIN <= 1.0,
       "gain is a fraction of the measured delta, never an amplification "
       "(%.2f)" % efi.CLOUD_KICK_GAIN)
    # A kick that outlasted the anchor would be correcting a ramp the regulator has
    # already forgotten about.
    ck(efi.CLOUD_KICK_CYCLES * efi.KEEPALIVE_SEC <= efi.SLEW_RESET_SEC,
       "a full kick finishes within SLEW_RESET_SEC (%.2fs vs %.2fs)"
       % (efi.CLOUD_KICK_CYCLES * efi.KEEPALIVE_SEC, efi.SLEW_RESET_SEC))

    # ---- 2. arming: only the export-causing direction ------------------
    # Deltas here stay under CLOUD_KICK_MAX_W on purpose, so these check the
    # proportionality. The cap gets its own section below.
    inj = new_injector()
    got = inj._arm_kick(300.0, 100.0)
    ck(abs(got - 200.0 * efi.CLOUD_KICK_GAIN) < 1e-9,
       "a cloud value 200W above ours arms a kick of the measured delta (%.0fW)" % got)
    ck(inj._kick_left == efi.CLOUD_KICK_CYCLES and inj.kick_events == 1,
       "and it is armed for CLOUD_KICK_CYCLES writes")

    inj = new_injector()
    ck(inj._arm_kick(100.0, 500.0) == 0.0,
       "a cloud value BELOW ours arms nothing: that already commands less output")
    ck(inj._kick_left == 0 and inj.kick_skipped == 1,
       "and it is counted as skipped rather than silently ignored")

    inj = new_injector()
    ck(inj._arm_kick(300.0, 300.0) == 0.0, "an identical value arms nothing")

    # NaN is the real shape of "no local reading yet": last_local is None and the
    # caller passes float('nan'). Every comparison with it is False, so a naive
    # `if delta > 0` would be correct by accident; this pins it deliberately.
    inj = new_injector()
    ck(inj._arm_kick(500.0, float("nan")) == 0.0,
       "no local reading yet arms nothing rather than kicking on a NaN delta")
    ck(inj._kick_left == 0, "and leaves nothing armed")

    # ---- 3. bounded, so a wild cloud value cannot drive it arbitrarily deep
    inj = new_injector()
    huge = inj._arm_kick(50000.0, 0.0)
    ck(huge == efi.CLOUD_KICK_MAX_W,
       "a 50kW delta is capped at CLOUD_KICK_MAX_W (%.0fW)" % huge)

    # ---- 4. a burst does not STACK, and does not step DOWN either ------
    # Overwrite frequency is not ours to control, so if kicks accumulated, the cloud
    # could drive our pull-down as deep as it liked simply by writing often. The
    # standing correction is therefore the largest single measured delta, never the sum.
    inj = new_injector()
    inj._arm_kick(300.0, 100.0)      # 200W
    inj._arm_kick(250.0, 100.0)      # 150W
    inj._arm_kick(280.0, 100.0)      # 180W
    ck(inj._kick_w <= efi.CLOUD_KICK_MAX_W and abs(inj._kick_w - 200.0) < 1e-9,
       "three overwrites leave the LARGEST measured delta, not their sum (%.0fW)"
       % inj._kick_w)
    # And specifically not 180W, the last one. Taking the last would mean a smaller
    # overwrite mid-correction LOWERS the standing kick in a single step, and a lower
    # kick is a higher value on the wire: the exact release-edge bug this version
    # exists to fix, just triggered by the cloud instead of by expiry.
    ck(inj._kick_w > 180.0,
       "a smaller later delta does not step the correction down (%.0fW)" % inj._kick_w)
    ck(inj._kick_left == efi.CLOUD_KICK_CYCLES,
       "and the hold is refreshed, not extended cumulatively")
    ck(inj.kick_events == 3, "all three are still counted as events")

    # ---- 5. arming is wired to the frame callback ----------------------
    # _on_msg is where the delta is actually observed, and it is also where the
    # kill switch and the pause interlock are read.
    inj = new_injector()
    inj.last_local = (time.time(), 100.0)
    inj._on_msg(Frame(300.0))
    ck(inj.kick_events == 1 and abs(inj._kick_w - 200.0 * efi.CLOUD_KICK_GAIN) < 1e-6,
       "an unrecognised telemetry value arms a kick from the real delta (%.0fW)"
       % inj._kick_w)
    ck(inj.echo_cloud == 1, "and is still classified as a cloud overwrite")

    # Our own echo must not arm anything, or every write we made would kick itself.
    inj = new_injector()
    inj.last_local = (time.time(), 100.0)
    inj._recent_sent = [(time.time(), 900.0)]
    inj._on_msg(Frame(900.0))
    ck(inj.echo_ours == 1 and inj.kick_events == 0,
       "an echo of our OWN write arms nothing")

    inj = new_injector()
    inj.flags = {"nokick": True}
    inj.last_local = (time.time(), 100.0)
    inj._on_msg(Frame(900.0))
    ck(inj.kick_events == 0 and inj._kick_left == 0,
       "with the nokick file present nothing is armed")
    ck(inj.echo_cloud == 1,
       "but the overwrite is still counted, so the file changes the response and not "
       "the measurement")

    # The causation test owns the link while it runs; kicking underneath it would
    # corrupt the very measurement it exists to make.
    inj = new_injector()
    inj.paused = True
    inj.last_local = (time.time(), 100.0)
    inj._on_msg(Frame(900.0))
    ck(inj.kick_events == 0, "nothing is armed while the causation test holds the link")

    # ---- 5b. the release is a RAMP, not a step -------------------------
    # This is the regression test for the bug that made the first version worse than
    # nothing. `_slew_limit` governs the meter reading and the kick is subtracted after
    # it, so the limiter never sees the kick and `_slew_ref_w` never moves when it
    # changes. Zeroing the correction on expiry was therefore a step UP of the whole
    # kick in one write, on a dead-flat meter, which commands a hard ramp up whose
    # overshoot is export. Measured: 0.29Wh/h before the kick went live, 1.21Wh/h in
    # the 69min after.
    rate = efi.SLEW_UP_W_PER_SEC
    inj = new_injector()
    t = 5000.0
    inj._arm_kick(300.0, 100.0, t)                   # 200W armed
    ck(inj._kick_now(t) == 200, "the kick starts at the measured delta")
    # The hold phase is spent on WRITES, so a cycle that does not write leaves it owed.
    # Nothing decays while cycles are owed, however long the caller takes.
    held = inj._kick_now(t + 10.0)      # stateful, so called exactly once
    ck(held == 200,
       "no decay at all while hold cycles are still owed (%d)" % held)
    inj._kick_left = 0                               # the hold is now spent
    half = 100.0 / rate                              # long enough to bleed 100W
    got = inj._kick_now(t + 10.0 + half)
    ck(abs(got - 100.0) <= 1.0,
       "once released it bleeds off at SLEW_UP_W_PER_SEC: 200W - %.0fW/s*%.3fs = "
       "%dW" % (rate, half, got))
    ck(inj._kick_now(t + 10.0 + half * 4) == 0,
       "and it reaches exactly zero rather than overshooting negative")
    ck(inj._kick_w == 0.0, "with no residue left armed")

    # Whatever the caller does with the clock, a single call may not RAISE the value on
    # the wire by more than the limiter would have allowed a real load. Since
    # val = w + bias - kick, that means the kick may not FALL faster than the rate.
    inj = new_injector()
    inj._arm_kick(1000.0, 0.0, t)                    # capped at CLOUD_KICK_MAX_W
    inj._kick_left = 0
    prev, worst_rise, tt = float(inj._kick_now(t)), 0.0, t
    for step in (0.001, 0.02, 0.25, 0.5, 0.05, 0.25, 0.25, 2.0):
        tt += step
        cur = float(inj._kick_now(tt))
        worst_rise = max(worst_rise, (prev - cur) / step)   # a kick FALL is a rise
        prev = cur
    ck(worst_rise <= rate + 1.0,
       "across a ragged cycle cadence no release step exceeds %.0fW/s (worst %.0fW/s)"
       % (rate, worst_rise))
    ck(prev == 0.0, "and the ramp still finishes (%.0fW left)" % prev)

    # A clock that went backwards must stall the release, not jump it: negative dt
    # through the same arithmetic would ADD to the correction, which is import, but it
    # would also make the next real step arbitrarily large.
    inj = new_injector()
    inj._arm_kick(300.0, 100.0, t)
    inj._kick_left = 0
    inj._kick_now(t)
    ck(inj._kick_now(t - 60.0) == 200,
       "a backwards clock holds the correction rather than moving it either way")
    ck(inj._kick_now(t - 60.0 + half) <= 101,
       "and the release resumes from the new reference, still at the rate")

    # ---- 6. the invariant that makes a bug here safe ------------------
    # Whatever else is wrong, a kick may only ever SUBTRACT. Checked on the arithmetic
    # the loop actually performs, over a spread of readings and deltas.
    worst = None
    for w in (-800.0, 0.0, 90.0, 1500.0):
        for bias in (0, -83, -143):
            for delta in (1.0, 400.0, 50000.0):
                inj = new_injector()
                k = int(round(min(inj._arm_kick(w + delta, w), efi.CLOUD_KICK_MAX_W)))
                with_kick = int(round(w + bias - k))
                without = int(round(w + bias))
                if with_kick > without:
                    worst = (w, bias, delta, with_kick, without)
    ck(worst is None,
       "across every reading, bias and delta a kick never raises the injected value "
       "(%r)" % (worst,))

    # ---- 7. wired into the real loop, spent on WRITES ------------------
    srv = await FakeShelly(100.0).start()
    efi.MODBUS_PORT = srv.port
    touch(efi.MODBUS_FILE)
    rm(efi.NOKICK_FILE)
    rm(efi.NOSLEW_FILE)
    rm(efi.AB_FILE)
    rm(efi.STOP_FILE)
    efi.write_text_atomic(efi.BIAS_FILE, "0")   # isolate the kick from the cushion
    efi.POLL_PERIOD_SEC = 0.02
    efi.KEEPALIVE_SEC = 0.04
    # Left at the SHIPPED rate on purpose. The meter is flat at 100W here so the
    # limiter itself never clamps anything (it governs the reading, not the value on
    # the wire), but this same constant is the release rate, so a realistic value is
    # what makes the ramp below observable as more than one write.
    efi.SLEW_UP_W_PER_SEC = 400.0

    def bind(inj):
        inj._mb = efi.ModbusMeter("127.0.0.1", "solar")
        inj._mb2 = efi.ModbusMeter("127.0.0.1", "grid")
        return inj

    dev = FakeDevice()
    inj = bind(new_injector(dev))
    task = asyncio.create_task(inj.run())
    await asyncio.sleep(0.3)                     # settle at 100W, no overwrites yet
    ck(inj.kicks == 0, "a quiet link produces no kicks at all (%d)" % inj.kicks)
    base = [w for _t, w in dev.sends[-3:]]
    ck(base and max(base) <= 101,
       "and the injected value is just the reading (%r)" % base)

    n_before = len(dev.sends)
    # The cloud writes 700W while the meter really reads 100W: a 600W overshoot
    # command, which is the shape of the events that carried 85% of the export.
    inj._on_msg(Frame(700.0))
    await asyncio.sleep(0.25)
    after = [w for _t, w in dev.sends[n_before:]]
    ck(inj.kick_events >= 1, "the loop armed a kick (%d events)" % inj.kick_events)
    ck(inj.kicks >= 1, "and actually carried it on a write (%d sends)" % inj.kicks)
    ck(after and min(after) < 0,
       "the injected value went BELOW the reading, pulling output down rather than "
       "merely restoring it (min %s)" % (min(after) if after else None))
    ck(inj.kick_worst_w <= efi.CLOUD_KICK_MAX_W,
       "no kick exceeded the cap (worst=%.0fW)" % inj.kick_worst_w)

    # And it EXPIRES. A kick that latched would be a hidden permanent cushion, which
    # is the failure the slew limiter was deliberately designed as a rate limit to
    # avoid, so it must not be reintroduced here. Waited on `_kick_w`, not on
    # `_kick_left`: the hold running out is now the START of the release, not the end
    # of the kick, and waiting on the counter would let this section finish while the
    # ramp was still on the wire.
    for _ in range(400):
        if inj._kick_w == 0.0:
            break
        await asyncio.sleep(0.01)
    ck(inj._kick_w == 0.0, "the kick is fully released (%.1fW left)" % inj._kick_w)
    ck(inj._kick_left == 0, "and the hold is spent (%d left)" % inj._kick_left)

    # The release, as the DEVICE saw it. This is the assertion that would have caught
    # the original bug: the recovery has to arrive as several small writes, and no
    # single one of them may raise the injected value faster than SLEW_UP_W_PER_SEC.
    series = dev.sends[n_before:]
    ups = [(series[i][1] - series[i - 1][1],
            series[i][0] - series[i - 1][0])
           for i in range(1, len(series)) if series[i][1] > series[i - 1][1]]
    ck(len(ups) >= 2,
       "the recovery arrives as a ramp, not one step (%d rising writes: %r)"
       % (len(ups), [int(d) for d, _ in ups]))
    worst = max((d / dt for d, dt in ups if dt > 0), default=0.0)
    ck(worst <= efi.SLEW_UP_W_PER_SEC * 1.35,
       "no rising write exceeded the release rate of %.0fW/s (worst %.0fW/s)"
       % (efi.SLEW_UP_W_PER_SEC, worst))
    ck(max(d for d, _ in ups) < efi.CLOUD_KICK_MAX_W,
       "and none of them carried the whole correction, which is what the first "
       "version did (largest step %dW of a %.0fW cap)"
       % (max(d for d, _ in ups), efi.CLOUD_KICK_MAX_W))
    # Indexed from the moment it was spent, not a fixed tail window: MIN_SEND_GAP_SEC
    # throttles writes to ~6/s, so "the last 3 sends" still contains the kick itself
    # and the assertion would be measuring the sleep length.
    n_settled = len(dev.sends)
    await asyncio.sleep(0.3)
    tail = [w for _t, w in dev.sends[n_settled:]]
    ck(tail and min(tail) >= 99 and max(tail) <= 101,
       "and every send after it is back to the plain reading (%d sends, %r)"
       % (len(tail), tail[:5]))

    inj.running = False
    try:
        await asyncio.wait_for(task, timeout=3.0)
    except asyncio.TimeoutError:
        task.cancel()
        ck(False, "run() did not stop within 3s of running=False")

    # ---- 8. a failed write does not consume the correction -------------
    # Spending on cycles rather than on confirmed sends would let the kick expire
    # against a device that never received it, leaving the ramp up and the counters
    # claiming it had been dealt with.
    # A failed write triggers the 1s link backoff by design, and sitting through that
    # would make this a test of the backoff. Shortened here only; it has to be set
    # before the Injector is built, because _backoff is seeded in __init__.
    efi.NOT_READY_BACKOFF_START = 0.05
    dev2 = FakeDevice()
    inj2 = bind(new_injector(dev2))
    task = asyncio.create_task(inj2.run())
    await asyncio.sleep(0.2)
    dev2.fail_next = 1
    inj2._on_msg(Frame(700.0))
    for _ in range(200):
        if inj2.write_err > 0:
            break
        await asyncio.sleep(0.005)
    ck(inj2.write_err > 0, "the write really failed (%d)" % inj2.write_err)
    ck(inj2.kicks == 0,
       "the failed cycle was not counted as a kick delivered (%d)" % inj2.kicks)
    ck(inj2._kick_left > 0,
       "and the correction is still owed rather than written off (%d cycles left)"
       % inj2._kick_left)
    for _ in range(400):
        if inj2.kicks >= 1:
            break
        await asyncio.sleep(0.005)
    ck(inj2.kicks >= 1,
       "once writes succeed again the correction is delivered (%d)" % inj2.kicks)
    ck(dev2.sends and min(w for _t, w in dev2.sends) < 0,
       "and it really went below the reading when it landed (min %s)"
       % min(w for _t, w in dev2.sends))
    inj2.running = False
    try:
        await asyncio.wait_for(task, timeout=3.0)
    except asyncio.TimeoutError:
        task.cancel()

    await srv.stop()
    rm(efi.MODBUS_FILE)
    return report()


sys.exit(asyncio.run(main()))
