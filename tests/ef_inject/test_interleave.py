"""Tests for the three changes made on top of the shipped Modbus build:

  1. deadline-based pacing (the loop was running at 1.6 Hz against a configured 2 Hz)
  2. poll/write decoupling (poll fast, write only on a new reading or a due keepalive)
  3. the second-meter interleave, its redundancy fallback, and its divergence latch

The pacing and write-gate checks drive the REAL run() loop against real fake
sockets rather than re-implementing the predicate, because a re-implemented gate
would pass while the shipped one was wrong.
"""
import sys, asyncio, os, tempfile, time
from harness import (FakeShelly, FakeDevice, FakeEntry, FakeHass, ck, report,
                     load, install_pb_stub, touch, rm)

install_pb_stub()
efi = load()
efi._LOGGER.disabled = True          # run() is deliberately chatty at WARNING

TMP = tempfile.mkdtemp()
efi.MODBUS_FILE = os.path.join(TMP, "modbus")
efi.SHADOW_FILE = os.path.join(TMP, "shadow")
efi.INTERLEAVE_FILE = os.path.join(TMP, "interleave")
efi.STOP_FILE = os.path.join(TMP, "stop")
efi.QUIET_FILE = os.path.join(TMP, "quiet")
efi.AB_FILE = os.path.join(TMP, "ab")
efi.CAUSATION_FILE = os.path.join(TMP, "causation")
efi.BASELINE_SEC = 0
IDENT = (True, 3, efi.EXPECTED_SN, 0, 0)


def clear_toggles():
    for p in (efi.MODBUS_FILE, efi.SHADOW_FILE, efi.INTERLEAVE_FILE, efi.STOP_FILE,
              efi.AB_FILE, efi.CAUSATION_FILE):
        rm(p)


def new_injector(dev):
    inj = efi.Injector(FakeHass([FakeEntry(efi.TARGET_TITLE, dev)]))
    inj.identity = IDENT
    return inj


async def drive(inj, seconds):
    """Run the real loop for a wall-clock window, then stop it cleanly."""
    task = asyncio.create_task(inj.run())
    await asyncio.sleep(seconds)
    inj.running = False
    try:
        await asyncio.wait_for(task, timeout=3.0)
    except asyncio.TimeoutError:
        task.cancel()
        ck(False, "run() did not stop within 3s of running=False")


# =========================================================================
# 1. toggle matrix
# =========================================================================
def test_toggles():
    clear_toggles()
    inj = new_injector(FakeDevice())

    ck(not inj._second_wanted(), "no toggles -> second meter not polled at all")
    ck(not inj._interleave_wanted(), "no toggles -> not interleaving")

    touch(efi.SHADOW_FILE)
    ck(not inj._second_wanted(),
       "shadow WITHOUT modbus does nothing (the second meter is Modbus-only)")
    touch(efi.MODBUS_FILE)
    ck(inj._second_wanted(), "modbus + shadow -> second meter polled")
    ck(not inj._interleave_wanted(), "shadow alone never changes the injected value")

    rm(efi.SHADOW_FILE)
    touch(efi.INTERLEAVE_FILE)
    ck(inj._second_wanted(), "interleave implies observing, no shadow file needed")
    ck(inj._interleave_wanted(), "modbus + interleave -> interleaving")

    inj.interleave_locked = True
    ck(not inj._interleave_wanted(),
       "the latch beats mere PRESENCE of the toggle file: a present file cannot "
       "keep clearing the latch, or it could never hold at all")
    ck(inj._second_wanted(),
       "still POLLED after the latch, so the divergence stays visible in the log")
    ck(inj._interleave_state().startswith("OFF"),
       "and the log says OFF in words, not locked=True (%s)" % inj._interleave_state())
    inj.interleave_locked = False
    ck(inj._interleave_state() == "ON", "state reads ON when it is actually working")
    rm(efi.INTERLEAVE_FILE)
    ck("not enabled" in inj._interleave_state(),
       "and distinguishes 'never turned on' from 'turned off by the guard'")
    clear_toggles()


# =========================================================================
# 2. divergence guard
# =========================================================================
W = efi.INTERLEAVE_DIVERGE_WINDOW
LIM = efi.INTERLEAVE_DIVERGE_W
DT = 0.25                            # POLL_PERIOD_SEC, as shipped


class Clock:
    """Synthetic time, because the quiescence gate is a function of WALL TIME and a
    test loop that runs in microseconds would never satisfy it."""

    def __init__(self, t=1000.0):
        self.t = t

    def feed(self, inj, n, v1, v2, step=0.0, dt=DT):
        """n samples, v1 optionally ramping by `step` each sample."""
        for _ in range(n):
            inj._track_divergence(v1, v2, now=self.t)
            self.t += dt
            v1 += step
            v2 += step
        return v1


def warm(inj, clk, v=500.0, d=0.0):
    """Enough flat samples to satisfy the quiet gate, without completing a verdict."""
    n = int(efi.INTERLEAVE_QUIET_SEC / DT) + 1
    clk.feed(inj, n, v, v - d)
    return n


def relevel(inj, clk, v1, v2):
    """Re-open the quiet gate after a LEVEL CHANGE, then start the count clean.

    Changing the divergence is itself a step, and the gate abstains across a step in
    EITHER meter. Those settle samples are the transition, not evidence about the
    instruments, so they must not be left in the pool to skew the verdict arithmetic
    the escalation checks depend on.
    """
    inj.sec_diffs = []
    clk.feed(inj, int(efi.INTERLEAVE_QUIET_SEC / DT) + 1, v1, v2)
    inj.sec_diffs = []


def test_divergence():
    # --- the quiescence gate, which is THE fix for the 2026-08-24 false trip ----
    # A signal moving at ~270 W/s while the two meters sample 0.412s apart produces
    # a large, entirely legitimate mean difference. The old guard averaged that over
    # ten seconds and latched off. Now a moving signal yields NO VERDICT AT ALL.
    inj = new_injector(FakeDevice())
    clk = Clock()
    clk.feed(inj, W * 4, 100.0, 100.0 - (LIM + 12), step=70.0)   # 280 W/s ramp
    ck(inj.sec_verdicts == 0,
       "a ramping signal produces NO verdict, however long it ramps (%d verdicts) "
       "<-- the false trip" % inj.sec_verdicts)
    ck(not inj.interleave_locked,
       "so a %+.0fW difference during a fast ramp cannot latch the interleave off"
       % (LIM + 12))
    ck(inj.sec_skipped_busy == W * 4,
       "every one of those samples is discarded as not-quiet and counted (%d)"
       % inj.sec_skipped_busy)

    # --- a STEP in the OTHER meter: the 2026-08-25 espresso false trip ----------
    # A resistive element switches instantaneously. The solar meter can read a
    # perfectly flat 400W across the whole window while the grid meter, having
    # already published the edge 0.412s earlier, reads 1600W. Gating on the solar
    # meter alone called that quiet and averaged the step into a verdict: -128W of
    # exactly this locked the interleave off in production at 07:07:56.
    inj = new_injector(FakeDevice())
    clk = Clock()
    warm(inj, clk, 400.0)                     # both flat and agreeing at the baseline
    for _ in range(W * 2):
        clk.feed(inj, 3, 400.0, 400.0)        # element off, both meters agree
        clk.feed(inj, 1, 400.0, 1600.0)       # element on, only the grid has it yet
    ck(inj.sec_verdicts == 0,
       "a 1200W step in the GRID meter while the solar meter reads flat yields NO "
       "verdict (%d), so a switching resistive load cannot lock the guard  <-- the "
       "espresso false trip" % inj.sec_verdicts)
    ck(not inj.interleave_locked,
       "and the interleave stays ON through sustained pulsing of that load")
    ck(inj.sec_diff_absmax >= 1200 - 1,
       "while the excursion is still reported in d_absmax (%.0fW) for the operator"
       % inj.sec_diff_absmax)

    # The gate must not be so strict it never has an opinion: once the load stops,
    # agreement has to become provable again, or the guard is decorative.
    relevel(inj, clk, 400.0, 400.0)
    clk.feed(inj, W, 400.0, 400.0)
    ck(inj.sec_verdicts == 1,
       "once the pulsing stops, a settled window completes a verdict again (%d): the "
       "gate abstains during the load, it does not go blind permanently"
       % inj.sec_verdicts)

    # A transient is still the normal case and must not trip anything either.
    inj = new_injector(FakeDevice())
    clk = Clock()
    warm(inj, clk)
    clk.feed(inj, 1, 500.0, 500.0 - 5 * LIM)
    ck(not inj.interleave_locked,
       "one transient %.0fW disagreement does NOT trip the guard" % (5 * LIM))
    ck(inj.sec_diff_absmax >= 5 * LIM - 1,
       "but the transient IS recorded in d_absmax (%.0fW) for the operator to see"
       % inj.sec_diff_absmax)

    # --- strikes: one bad window is not enough --------------------------------
    inj = new_injector(FakeDevice())
    clk = Clock()
    warm(inj, clk, 1000.0, LIM + 50)
    clk.feed(inj, W, 1000.0, 1000.0 - (LIM + 50))
    ck(inj.sec_verdicts == 1, "a full window of QUIET samples completes a verdict")
    ck(inj.sec_bad_run == 1, "the verdict is recorded as a strike")
    ck(not inj.interleave_locked,
       "but ONE bad window does not latch it off (%d strikes needed)"
       % efi.INTERLEAVE_STRIKES)
    ck(len(inj.sec_diffs) < W // 4,
       "and the window is CLEARED at the verdict (%d samples held, only the ones "
       "taken since), so the next strike is independent evidence and not the same "
       "bad samples counted a second time" % len(inj.sec_diffs))

    clk.feed(inj, W, 1000.0, 1000.0 - (LIM + 50))
    ck(inj.interleave_locked,
       "%d consecutive bad windows DO latch the interleave off" % efi.INTERLEAVE_STRIKES)
    ck(inj.interleave_locks == 1, "the lock is counted")
    touch(efi.MODBUS_FILE)          # _interleave_state reports "not enabled" without
    touch(efi.INTERLEAVE_FILE)      # the toggles, which would hide the latch wording
    state = inj._interleave_state()
    clear_toggles()
    ck("self-recovers" in state,
       "and the reported state SAYS it recovers by itself, so nobody reading the log "
       "thinks a human is needed: %r" % state)

    # --- auto recovery ---------------------------------------------------------
    relevel(inj, clk, 1000.0, 1000.0)
    clk.feed(inj, W, 1000.0, 1000.0)
    ck(inj.interleave_locked,
       "one good window is not enough to recover either (recovery is deliberately "
       "harder than tripping)")
    clk.feed(inj, W, 1000.0, 1000.0)
    ck(not inj.interleave_locked,
       "%d consecutive good windows RE-ENABLE the interleave with no restart"
       % efi.INTERLEAVE_RECOVER_STRIKES)
    ck(inj.interleave_recoveries == 1, "the recovery is counted")

    # Recovery needs the TIGHTER band: merely stopping being awful is not agreement.
    inj = new_injector(FakeDevice())
    clk = Clock()
    inj.interleave_locked = True
    mid = (efi.INTERLEAVE_RECOVER_W + LIM) / 2
    warm(inj, clk, 1000.0, mid)
    clk.feed(inj, W * efi.INTERLEAVE_RECOVER_STRIKES, 1000.0, 1000.0 - mid)
    ck(inj.interleave_locked,
       "a %+.0fW mean sits in the hysteresis dead zone: too tight to strike, too "
       "loose to recover, so nothing changes" % mid)
    ck(inj.sec_bad_run == 0 and inj.sec_good_run == 0,
       "the dead zone breaks a bad streak without counting toward recovery")

    # --- ESCALATING hysteresis: no permanent state, ever ----------------------
    # A flapping pair must throttle itself WITHOUT ever needing a human, because
    # nobody is watching. Each recovery doubles the proof the next one must show.
    inj = new_injector(FakeDevice())
    clk = Clock()
    warm(inj, clk, 1000.0)
    base = efi.INTERLEAVE_RECOVER_STRIKES
    ck(inj._recover_strikes_needed() == base,
       "a guard that has never recovered asks for the cheap %d verdicts" % base)

    needs = []
    for cycle in range(4):
        need = inj._recover_strikes_needed()
        needs.append(need)
        relevel(inj, clk, 1000.0, 1000.0 - (LIM + 50))
        clk.feed(inj, W * efi.INTERLEAVE_STRIKES, 1000.0, 1000.0 - (LIM + 50))
        ck(inj.interleave_locked, "cycle %d: it locked off" % cycle)
        # one short of the requirement must NOT be enough
        relevel(inj, clk, 1000.0, 1000.0)
        clk.feed(inj, W * (need - 1), 1000.0, 1000.0)
        ck(inj.interleave_locked,
           "cycle %d: %d good verdicts is one short of the %d now demanded, still off"
           % (cycle, need - 1, need))
        clk.feed(inj, W, 1000.0, 1000.0)
        ck(not inj.interleave_locked,
           "cycle %d: the %dth good verdict recovers it BY ITSELF, no human, no "
           "restart" % (cycle, need))

    ck(needs == [base * 2 ** i for i in range(4)],
       "each recovery DOUBLES the next requirement: %r verdicts" % needs)
    ck(inj.interleave_recoveries == 4,
       "all four recoveries happened unattended (%d)" % inj.interleave_recoveries)

    # The doubling is capped, so the answer is always a finite wait away.
    inj._recovery_times = [inj._recovery_times[-1]] * 40
    ck(inj._recover_strikes_needed() == efi.INTERLEAVE_RECOVER_MAX_STRIKES,
       "after many recoveries the requirement is CAPPED at %d, not infinity: a "
       "pathological pair waits, it is never locked out (%d)"
       % (efi.INTERLEAVE_RECOVER_MAX_STRIKES, inj._recover_strikes_needed()))
    relevel(inj, clk, 1000.0, 1000.0 - (LIM + 50))
    clk.feed(inj, W * efi.INTERLEAVE_STRIKES, 1000.0, 1000.0 - (LIM + 50))
    ck(inj.interleave_locked, "and it locks with the escalation maxed out")
    relevel(inj, clk, 1000.0, 1000.0)
    clk.feed(inj, W * efi.INTERLEAVE_RECOVER_MAX_STRIKES, 1000.0, 1000.0)
    ck(not inj.interleave_locked,
       "even at the ceiling, sustained agreement STILL recovers it  <-- there is no "
       "state this guard can reach that requires babysitting")

    # And the escalation DECAYS, so a one-off glitch weeks later is cheap again.
    inj._recovery_times = [time.time() - efi.INTERLEAVE_RECOVERY_DECAY_SEC - 1] * 40
    ck(inj._recover_strikes_needed() == base,
       "recoveries older than %.0f min stop escalating, so an isolated glitch pays "
       "the cheap %d verdicts again rather than inheriting old history"
       % (efi.INTERLEAVE_RECOVERY_DECAY_SEC / 60.0, base))
    ck(inj._recent_recoveries() == 0, "and the expired entries are pruned")

    # --- agreement and partial data -------------------------------------------
    inj = new_injector(FakeDevice())
    clk = Clock()
    clk.feed(inj, W * 3, 500.0, 500.0)
    ck(not inj.interleave_locked, "perfect agreement never trips the guard")
    ck(len(inj.sec_diffs) < W,
       "the window is bounded: it is emptied at every verdict (%d held)"
       % len(inj.sec_diffs))

    inj = new_injector(FakeDevice())
    clk = Clock()
    warm(inj, clk, 0.0, LIM + 500)
    clk.feed(inj, W - 2, 0.0, -(LIM + 500))
    ck(not inj.interleave_locked,
       "guard needs a FULL window before it can trip (no verdict on partial data)")
    ck(inj.sec_verdicts == 0, "and no verdict was recorded (%d)" % inj.sec_verdicts)


# =========================================================================
# 2b. manual re-arm, the escape hatch that needs no HA restart
# =========================================================================
async def test_rearm():
    clear_toggles()
    touch(efi.MODBUS_FILE)
    touch(efi.INTERLEAVE_FILE)
    inj = new_injector(FakeDevice())

    inj._check_interleave_rearm()
    ck(inj._interleave_file_seen is True and inj.interleave_rearms == 0,
       "the FIRST sight of an already-present toggle file is not an edge, so a "
       "restart-time observation cannot be mistaken for a re-arm")

    inj.interleave_locked = True
    inj._recovery_times = [time.time()] * 3      # escalation already raised
    inj.sec_bad_run = 7
    for _ in range(5):
        inj._check_interleave_rearm()
    ck(inj.interleave_locked and inj.interleave_rearms == 0,
       "a file that just SITS there does not re-arm anything, however many cycles "
       "pass: presence alone would defeat the latch entirely")

    rm(efi.INTERLEAVE_FILE)
    inj._check_interleave_rearm()
    ck(inj.interleave_locked, "removing the file does not by itself re-arm")

    touch(efi.INTERLEAVE_FILE)
    inj._check_interleave_rearm()
    ck(not inj.interleave_locked,
       "the absent -> present TRANSITION clears the latch  <-- rm then touch, no "
       "HA restart needed")
    ck(inj._recover_strikes_needed() == efi.INTERLEAVE_RECOVER_STRIKES,
       "a deliberate operator act also resets the ESCALATION back to %d, since you "
       "only do this when you know why it tripped"
       % efi.INTERLEAVE_RECOVER_STRIKES)
    ck(inj._recent_recoveries() == 0, "and clears the recent-recovery history")
    ck(inj.sec_bad_run == 0 and inj.sec_diffs == [],
       "judging restarts from scratch rather than resuming a stale bad streak")
    ck(inj.interleave_rearms == 1, "the re-arm is counted (%d)" % inj.interleave_rearms)
    ck(inj._interleave_wanted(), "and the interleave is actually back in use")

    # An unlocked guard must not be disturbed by toggling.
    rm(efi.INTERLEAVE_FILE)
    inj._check_interleave_rearm()
    touch(efi.INTERLEAVE_FILE)
    inj._check_interleave_rearm()
    ck(inj.interleave_rearms == 1,
       "toggling while it is already ON is not counted as a re-arm (%d)"
       % inj.interleave_rearms)

    # The re-arm is wired into the real read path, not just callable in a test.
    inj2 = new_injector(FakeDevice())
    inj2._check_interleave_rearm()          # seed the edge detector as present
    rm(efi.INTERLEAVE_FILE)

    async def r1():
        return 100.0

    async def r2():
        return 101.0
    inj2._mb.read = r1
    inj2._mb2.read = r2
    await inj2._read_meters()               # observes the file as ABSENT
    inj2.interleave_locked = True
    touch(efi.INTERLEAVE_FILE)
    await inj2._read_meters()               # observes the edge
    ck(not inj2.interleave_locked,
       "and _read_meters checks for the re-arm every cycle, so the escape hatch "
       "works on the LIVE loop")
    clear_toggles()


# =========================================================================
# 3. meter selection
# =========================================================================
async def test_selection():
    clear_toggles()
    touch(efi.MODBUS_FILE)

    def rig(v1, v2, ts1, ts2):
        inj = new_injector(FakeDevice())

        async def r1():
            inj._mb.last_change_ts = ts1
            return v1

        async def r2():
            inj._mb2.last_change_ts = ts2
            return v2
        inj._mb.read = r1
        inj._mb2.read = r2
        return inj

    async def go():
        # interleave: freshest publication wins
        touch(efi.INTERLEAVE_FILE)
        inj = rig(100.0, 102.0, 10.0, 20.0)
        w, src, ts = await inj._read_meters()
        ck((w, src, ts) == (102.0, "grid", 20.0),
           "interleave picks the meter that published LAST (%r)" % ((w, src, ts),))
        ck(inj.sec_used == 1, "use of the second meter is counted")

        inj = rig(100.0, 102.0, 30.0, 20.0)
        w, src, ts = await inj._read_meters()
        ck((w, src) == (100.0, "solar"),
           "when the solar meter is fresher it is used (%r)" % ((w, src),))
        ck(inj.sec_used == 0, "not counted as a second-meter use")

        # redundancy: this is the failure independence a wired link would have bought
        inj = rig(None, 77.0, 0.0, 20.0)
        w, src, _ = await inj._read_meters()
        ck((w, src) == (77.0, "grid"),
           "solar meter DEAD -> the second meter keeps regulation alive (%r)" % ((w, src),))

        inj = rig(55.0, None, 20.0, 0.0)
        w, src, _ = await inj._read_meters()
        ck((w, src) == (55.0, "solar"), "second meter dead -> solar meter used")

        inj = rig(None, None, 0.0, 0.0)
        w, src, ts = await inj._read_meters()
        ck((w, src, ts) == (None, None, 0.0),
           "both dead -> None, so _read_local can fall back to HTTP")

        # shadow: observe only, never substitute
        rm(efi.INTERLEAVE_FILE)
        touch(efi.SHADOW_FILE)
        inj = rig(100.0, 102.0, 10.0, 20.0)
        w, src, _ = await inj._read_meters()
        ck((w, src) == (100.0, "solar"),
           "SHADOW returns the solar meter even when the other is fresher (%r)"
           % ((w, src),))
        ck(inj.sec_diff_absmax == 2.0,
           "shadow still polled the second meter and recorded the difference (%.1fW)"
           % inj.sec_diff_absmax)
        ck(inj.sec_skipped_busy == 1,
           "and a first sample with no history behind it is not yet EVIDENCE: "
           "flatness is unproven, so it does not enter the verdict window")
        ck(inj.sec_used == 0, "shadow never counts a substitution")

        inj = rig(None, 102.0, 0.0, 20.0)
        w, src, _ = await inj._read_meters()
        ck((w, src) == (None, None),
           "SHADOW with a dead solar meter does NOT silently switch source")

        # locked latch behaves exactly like shadow
        touch(efi.INTERLEAVE_FILE)
        inj = rig(100.0, 102.0, 10.0, 20.0)
        inj.interleave_locked = True
        w, src, _ = await inj._read_meters()
        ck((w, src) == (100.0, "solar"), "once latched, the solar meter is used again")

        # second meter not polled at all when off
        clear_toggles()
        touch(efi.MODBUS_FILE)
        inj = rig(100.0, 102.0, 10.0, 20.0)
        called = []

        async def r2_spy():
            called.append(1)
            return 102.0
        inj._mb2.read = r2_spy
        await inj._read_meters()
        ck(not called, "with both toggles off the second meter is never contacted")

    await go()
    clear_toggles()


# =========================================================================
# 4. deadline pacing, through the real loop
# =========================================================================
async def test_pacing():
    clear_toggles()
    srv = await FakeShelly(800.0).start()
    efi.MODBUS_PORT = srv.port
    touch(efi.MODBUS_FILE)

    efi.POLL_PERIOD_SEC = 0.05
    efi.KEEPALIVE_SEC = 0.04          # below the poll period, so every poll writes
    dev = FakeDevice("A", send_delay=0.02)
    inj = new_injector(dev)
    inj._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj._mb2 = efi.ModbusMeter("127.0.0.1", "grid")

    t0 = time.time()
    await drive(inj, 1.5)
    elapsed = time.time() - t0
    rate = inj.polls / elapsed
    configured = 1.0 / efi.POLL_PERIOD_SEC
    old_rate = 1.0 / (efi.POLL_PERIOD_SEC + dev.send_delay)

    ck(inj.sent > 10, "the loop actually wrote (%d sends)" % inj.sent)
    ck(rate >= 0.85 * configured,
       "poll rate %.1f/s reaches the configured %.1f/s (deadline pacing absorbs the "
       "%.0fms write instead of adding to it)" % (rate, configured, dev.send_delay * 1000))
    ck(rate > old_rate * 1.15,
       "and is clearly better than sleep-after-work would give (%.1f/s vs %.1f/s)"
       % (rate, old_rate))
    ck(inj.write_err == 0, "no write errors")
    ck(inj._mb.err == 0, "no modbus errors over %d reads" % inj._mb.reads)

    # A stall must not be repaid as a burst: bursting BLE writes is what
    # MIN_SEND_GAP_SEC exists to prevent.
    inj._deadline = time.time() - 10.0
    t1 = time.time()
    await inj._pace()
    ck(time.time() - t1 >= efi.POLL_PERIOD_SEC * 0.5,
       "after falling 10s behind, _pace resyncs to now instead of firing free cycles")

    await srv.stop()
    clear_toggles()


# =========================================================================
# 5. write gate, through the real loop
# =========================================================================
async def test_write_gate():
    clear_toggles()
    srv = await FakeShelly(800.0).start()
    efi.MODBUS_PORT = srv.port
    touch(efi.MODBUS_FILE)

    efi.POLL_PERIOD_SEC = 0.02
    efi.KEEPALIVE_SEC = 0.20
    dev = FakeDevice("A")
    inj = new_injector(dev)
    inj._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj._mb2 = efi.ModbusMeter("127.0.0.1", "grid")

    await drive(inj, 0.9)

    ck(inj.polls > 20, "polled often (%d polls in 0.9s)" % inj.polls)
    ck(inj.polls > 3 * inj.sent,
       "a STEADY reading is polled far more often than it is written "
       "(%d polls vs %d writes): no point re-sending a value the device has"
       % (inj.polls, inj.sent))
    ck(inj.polls_no_write > 0, "skipped writes are counted (%d)" % inj.polls_no_write)
    ck(inj.writes_keepalive >= 2,
       "the keepalive still fires on a steady reading (%d), so the cloud stays "
       "contested" % inj.writes_keepalive)
    ck(inj.writes_on_change == 1,
       "exactly one change-triggered write: the very first one (%d)"
       % inj.writes_on_change)
    ck(inj.writes_on_change + inj.writes_keepalive == inj.sent,
       "every send is attributed to exactly one reason (%d+%d vs %d)"
       % (inj.writes_on_change, inj.writes_keepalive, inj.sent))

    gaps = [b[0] - a[0] for a, b in zip(dev.sends, dev.sends[1:])]
    ck(gaps and min(gaps) >= efi.KEEPALIVE_SEC * 0.85,
       "no two writes closer than the keepalive on a steady reading (min %.0fms)"
       % (min(gaps) * 1000 if gaps else 0))

    # ---- a changed reading is written promptly ---------------------------
    dev2 = FakeDevice("B")
    inj2 = new_injector(dev2)
    inj2._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj2._mb2 = efi.ModbusMeter("127.0.0.1", "grid")

    async def wiggle():
        # Step the meter well inside the keepalive window. If the gate were
        # keepalive-only, this change would wait up to 200ms to reach the device.
        await asyncio.sleep(0.35)
        srv.value = 300.0
        t_change = time.time()
        await asyncio.sleep(0.35)
        return t_change

    task = asyncio.create_task(inj2.run())
    t_change = await wiggle()
    inj2.running = False
    await asyncio.wait_for(task, timeout=3.0)

    after = [t for t, c in dev2.sends if t > t_change]
    ck(bool(after), "the changed reading did reach the device")
    if after:
        delay = after[0] - t_change
        ck(delay < efi.KEEPALIVE_SEC * 0.75,
           "a new reading is written in %.0fms, well inside the %.0fms keepalive, "
           "so detection latency is the poll period not the write period"
           % (delay * 1000, efi.KEEPALIVE_SEC * 1000))
    ck(inj2.writes_on_change >= 2,
       "the step is attributed to a CHANGE, not to the keepalive (%d)"
       % inj2.writes_on_change)

    # The cushion must never make the injected value EXCEED the truth: telling the
    # inverter it is importing more than it is commands ramp-up, i.e. real export.
    pre = [c for t, c in dev2.sends if t <= t_change]
    post = [c for t, c in dev2.sends if t > t_change]
    ck(pre and max(pre) <= 800,
       "before the step, injected <= the true 800W (%r)" % (sorted(set(pre)),))
    ck(post and max(post) <= 300,
       "after the step, injected <= the true 300W (%r)" % (sorted(set(post)),))
    ck(efi.BIAS_W == 0 or (post and min(post) < 300),
       "and at 300W the cushion IS armed, so the value is pushed below the truth")

    await srv.stop()
    clear_toggles()


# =========================================================================
# 6. freshness metric and interleave through the real loop
# =========================================================================
async def test_live_interleave():
    clear_toggles()
    solar = await FakeShelly(800.0).start()
    grid = await FakeShelly(802.0).start()
    ck(solar.port != grid.port, "two independent fake meters")

    efi.POLL_PERIOD_SEC = 0.02
    efi.KEEPALIVE_SEC = 0.05
    touch(efi.MODBUS_FILE)
    touch(efi.INTERLEAVE_FILE)

    dev = FakeDevice("A")
    inj = new_injector(dev)
    # Two distinct endpoints. This is the check that caught the port being read from
    # the module global: both meters then pointed at the SAME socket, the second one
    # always timestamped a hair later, and it looked like a 100% interleave win.
    inj._mb = efi.ModbusMeter("127.0.0.1", "solar", port=solar.port)
    inj._mb2 = efi.ModbusMeter("127.0.0.1", "grid", port=grid.port)

    async def churn():
        # Mimic the measured behaviour: both publish at ~1 Hz (compressed here),
        # offset in time, agreeing to a couple of watts.
        for i in range(14):
            await asyncio.sleep(0.05)
            grid.value = 800.0 + 40 * i + 2.0
            await asyncio.sleep(0.05)
            solar.value = 800.0 + 40 * i

    task = asyncio.create_task(inj.run())
    await churn()
    inj.running = False
    await asyncio.wait_for(task, timeout=3.0)

    ck(inj._mb.reads > 10 and inj._mb2.reads > 10,
       "both meters were read (solar=%d grid=%d)" % (inj._mb.reads, inj._mb2.reads))
    ck(inj.sec_used > 0,
       "the second meter was actually used for some samples (%d)" % inj.sec_used)
    ck(inj.sec_used < inj._mb.reads,
       "but not for ALL of them: the source alternates as each meter publishes "
       "(%d of %d reads)" % (inj.sec_used, inj._mb.reads))
    ck(not inj.interleave_locked,
       "meters agreeing to ~2W did not trip the divergence guard")
    ck(inj.fresh_ms_n > 0, "the staleness metric was recorded (%d samples)" % inj.fresh_ms_n)
    avg = inj.fresh_ms_sum / max(1, inj.fresh_ms_n)
    ck(0 <= avg < 2000,
       "average injected-value staleness is a sane %.0fms (this is the number the "
       "interleave is meant to reduce)" % avg)
    ck(inj.transport.startswith("modbus:"),
       "transport names the meter actually used: %r" % inj.transport)

    await solar.stop()
    await grid.stop()
    clear_toggles()


# =========================================================================
# 7. safety: the new gates cannot bypass the old ones
# =========================================================================
async def test_gates_still_ordered():
    clear_toggles()
    srv = await FakeShelly(800.0).start()
    efi.MODBUS_PORT = srv.port
    touch(efi.MODBUS_FILE)
    efi.POLL_PERIOD_SEC = 0.02
    efi.KEEPALIVE_SEC = 0.02

    # A silent link must produce ZERO writes no matter how the write gate feels.
    dev = FakeDevice("A")
    inj = new_injector(dev)
    inj._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj._mb2 = efi.ModbusMeter("127.0.0.1", "grid")
    task = asyncio.create_task(inj.run())
    await asyncio.sleep(0.1)
    inj._last_frame_ts = time.time() - (efi.FRAME_MAX_AGE_SEC + 5)
    n_at_silence = len(dev.sends)
    await asyncio.sleep(0.3)
    inj.running = False
    try:
        await asyncio.wait_for(task, timeout=3.0)
    except asyncio.TimeoutError:
        task.cancel()
    ck(len(dev.sends) == n_at_silence,
       "a silent BLE link stops writes dead (%d -> %d), the write gate cannot "
       "reach past it" % (n_at_silence, len(dev.sends)))
    ck(inj.skipped_silent > 0, "and the refusals are counted (%d)" % inj.skipped_silent)

    # A wrong meter SN must also block every write.
    dev2 = FakeDevice("B")
    inj2 = new_injector(dev2)
    inj2.identity = (True, 3, "WRONGSN00000", 0, 0)
    inj2._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj2._mb2 = efi.ModbusMeter("127.0.0.1", "grid")
    await asyncio.wait_for(inj2.run(), timeout=3.0)
    ck(len(dev2.sends) == 0,
       "a mismatched meter SN aborts before any write (%d sends)" % len(dev2.sends))

    await srv.stop()
    clear_toggles()


# =========================================================================
# 8. the runtime bias override reaches the WIRE, not just the accessor
# =========================================================================
async def test_bias_override_on_the_wire():
    clear_toggles()
    efi.BIAS_FILE = os.path.join(TMP, "bias")
    srv = await FakeShelly(200.0).start()          # inside the fade window
    efi.MODBUS_PORT = srv.port
    touch(efi.MODBUS_FILE)
    efi.POLL_PERIOD_SEC = 0.02
    efi.KEEPALIVE_SEC = 0.02
    # The shipped throttle is 2s of WALL time; these checks are about the pickup
    # mechanism, so scale it down. The throttle itself is asserted separately below.
    ship_refresh, efi.BIAS_REFRESH_SEC = efi.BIAS_REFRESH_SEC, 0.05
    ck(ship_refresh >= 1.0,
       "the SHIPPED bias re-read throttle is at least 1s (%.1fs), so the executor "
       "hop cannot become a hot path at %.2fs polling"
       % (ship_refresh, efi.POLL_PERIOD_SEC))

    def expect(bias, w=200.0):
        return int(round(w + bias * (efi.BIAS_FADE_W - w) / efi.BIAS_FADE_W))

    with open(efi.BIAS_FILE, "w") as f:
        f.write("-40")
    dev = FakeDevice("A")
    inj = new_injector(dev)
    inj._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj._mb2 = efi.ModbusMeter("127.0.0.1", "grid")
    await drive(inj, 0.4)

    vals = sorted({c for _, c in dev.sends})
    ck(vals == [expect(-40)],
       "the OVERRIDE is what lands on the wire: %r (expected %d for bias -40, not "
       "%d for the built-in %+d)"
       % (vals, expect(-40), expect(efi.BIAS_W), efi.BIAS_W))
    ck(inj.bias_w == -40, "and the loop reports the bias it actually used")

    # Change it mid-flight. This is the whole point: no restart.
    dev2 = FakeDevice("B")
    inj2 = new_injector(dev2)
    inj2._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj2._mb2 = efi.ModbusMeter("127.0.0.1", "grid")
    task = asyncio.create_task(inj2.run())
    await asyncio.sleep(0.25)
    with open(efi.BIAS_FILE, "w") as f:
        f.write("-160")
    t_change = time.time()
    await asyncio.sleep(0.25)
    inj2.running = False
    await asyncio.wait_for(task, timeout=3.0)

    before = {c for t, c in dev2.sends if t <= t_change}
    # Allow one throttle period for the edit to be noticed: sends inside it
    # legitimately still carry the old value, they are not a failure to pick up.
    after = {c for t, c in dev2.sends if t > t_change + efi.BIAS_REFRESH_SEC}
    ck(before == {expect(-40)},
       "before the edit the loop injected %r" % sorted(before))
    ck(after == {expect(-160)},
       "one throttle period after the edit it injected %r without a restart "
       "(expected %d)" % (sorted(after), expect(-160)))
    ck(inj2.bias_overrides == 2,
       "two changes recorded: adopting -40 away from the built-in %+d at startup, "
       "then the mid-flight edit to -160 (%d)" % (efi.BIAS_W, inj2.bias_overrides))

    # A typo mid-flight must not change what is written.
    dev3 = FakeDevice("C")
    inj3 = new_injector(dev3)
    inj3._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj3._mb2 = efi.ModbusMeter("127.0.0.1", "grid")
    task = asyncio.create_task(inj3.run())
    await asyncio.sleep(0.2)
    with open(efi.BIAS_FILE, "w") as f:
        f.write("+180")                            # the dangerous typo
    t_typo = time.time()
    await asyncio.sleep(0.25)
    inj3.running = False
    await asyncio.wait_for(task, timeout=3.0)

    post = {c for t, c in dev3.sends if t > t_typo}
    ck(post == {expect(-160)},
       "a positive override changes NOTHING on the wire: still %r, the last good "
       "value" % sorted(post))
    ck(inj3.hass.executor_jobs > 0,
       "and every bias read went through hass.async_add_executor_job (%d), not a "
       "blocking open() in the event loop  <-- what HA's loop detector caught"
       % inj3.hass.executor_jobs)
    ck(all(c <= 200 for _, c in dev3.sends),
       "and no injected value ever exceeded the true 200W, which is the invariant "
       "that keeps a typo from commanding export")
    ck(inj3.bias_rejected >= 1, "the refusal was counted (%d)" % inj3.bias_rejected)

    # Now put the SHIPPED throttle back and prove it actually throttles: at 0.02s
    # polling an unthrottled read would hop to the executor on every cycle.
    efi.BIAS_REFRESH_SEC = ship_refresh
    with open(efi.BIAS_FILE, "w") as f:
        f.write("-40")
    dev4 = FakeDevice("D")
    inj4 = new_injector(dev4)
    inj4._mb = efi.ModbusMeter("127.0.0.1", "solar")
    inj4._mb2 = efi.ModbusMeter("127.0.0.1", "grid")
    await drive(inj4, 0.4)
    ck(inj4.hass.executor_jobs == 1,
       "with the shipped %.1fs throttle a 0.4s run reads the file ONCE (%d), not "
       "once per %.2fs poll" % (ship_refresh, inj4.hass.executor_jobs,
                                efi.POLL_PERIOD_SEC))
    ck(len(dev4.sends) > 5 and {c for _, c in dev4.sends} == {expect(-40)},
       "and the throttle does not stall the loop: %d sends, all at the override "
       "value" % len(dev4.sends))

    os.remove(efi.BIAS_FILE)
    await srv.stop()
    clear_toggles()


async def main():
    test_toggles()
    test_divergence()
    await test_rearm()
    await test_selection()
    await test_pacing()
    await test_write_gate()
    await test_live_interleave()
    await test_gates_still_ordered()
    await test_bias_override_on_the_wire()
    return report()


sys.exit(asyncio.run(main()))
