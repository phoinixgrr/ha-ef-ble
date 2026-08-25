"""Read-only: does the new quiescence gate ever open on the REAL signal?

Polls both 3EMs at POLL_PERIOD_SEC and replays the shipped guard predicate over
the samples. Answers the only question the unit tests cannot: with a gate this
strict, do we still reach verdicts, and what does a QUIET-sample difference
actually look like against the 100W limit?
"""
import asyncio, socket, struct, sys, time

BOUND, GRID, PORT = "192.168.101.158", "192.168.101.149", 502
REG, POLL = 1064, 0.25
QUIET_W, QUIET_SEC, WINDOW, LIM, RECOVER = 25.0, 1.5, 40, 100.0, 40.0
DUR = float(sys.argv[1]) if len(sys.argv) > 1 else 180.0


async def read(host, tid):
    r, w = await asyncio.open_connection(host, PORT)
    try:
        w.write(struct.pack(">HHHBBHH", tid, 0, 6, 1, 4, REG, 2))
        await w.drain()
        hdr = await asyncio.wait_for(r.readexactly(9), 2.0)
        body = await asyncio.wait_for(r.readexactly(hdr[8]), 2.0)
        lo, hi = struct.unpack(">HH", body)
        return struct.unpack("<f", struct.pack("<HH", lo, hi))[0]
    finally:
        w.close()


async def main():
    hist, diffs, verdicts, quiet_n, all_n = [], [], [], 0, 0
    quiet_d, busy_d = [], []
    old_roll, old_worst, old_trips = [], 0.0, 0   # the guard as it shipped
    t_end = time.time() + DUR
    tid = 1
    while time.time() < t_end:
        t = time.time()
        try:
            v1, v2 = await asyncio.gather(read(BOUND, tid), read(GRID, tid + 1))
        except Exception as e:
            print("read err", e)
            await asyncio.sleep(POLL)
            continue
        tid = (tid + 2) % 60000
        all_n += 1
        d = v1 - v2
        old_roll.append(d)                      # OLD: every sample, rolling, 40 wide
        del old_roll[:-WINDOW]
        if len(old_roll) == WINDOW:
            m = sum(old_roll) / WINDOW
            old_worst = max(old_worst, abs(m))
            if abs(m) > LIM:
                old_trips += 1
        hist.append((t, v1))
        hist[:] = [x for x in hist if x[0] >= t - QUIET_SEC * 2]
        win = [v for ts, v in hist if ts >= t - QUIET_SEC]
        quiet = (t - hist[0][0] >= QUIET_SEC and len(win) >= 2
                 and max(win) - min(win) <= QUIET_W)
        if quiet:
            quiet_n += 1
            quiet_d.append(d)
            diffs.append(d)
            if len(diffs) >= WINDOW:
                verdicts.append(sum(diffs) / len(diffs))
                diffs.clear()
        else:
            busy_d.append(d)
        await asyncio.sleep(max(0.0, POLL - (time.time() - t)))

    def stat(xs):
        if not xs:
            return "none"
        return "n=%d mean=%+.1fW max|d|=%.0fW" % (
            len(xs), sum(xs) / len(xs), max(abs(x) for x in xs))

    print("\n--- %.0fs, %d samples ---" % (DUR, all_n))
    print("QUIET samples : %d (%.0f%%)   %s" % (
        quiet_n, 100.0 * quiet_n / max(1, all_n), stat(quiet_d)))
    print("BUSY samples  : %d (%.0f%%)   %s" % (
        all_n - quiet_n, 100.0 * (all_n - quiet_n) / max(1, all_n), stat(busy_d)))
    print("verdicts      : %d  %s" % (
        len(verdicts), ["%+.1f" % v for v in verdicts]))
    if verdicts:
        worst = max(verdicts, key=abs)
        print("worst verdict : %+.1fW  (lock at %.0fW, recover at %.0fW) -> %s" % (
            worst, LIM, RECOVER,
            "WOULD STRIKE" if abs(worst) > LIM
            else "good" if abs(worst) <= RECOVER else "dead zone"))
    print("OLD guard     : worst rolling-%d mean over ALL samples = %.0fW, would "
          "have latched off %s" % (WINDOW, old_worst,
                                   "%d times" % old_trips if old_trips else "never"))
    if len(verdicts) >= 1:
        print("verdict rate  : one per %.0fs of wall clock" % (DUR / len(verdicts)))
    else:
        print("verdict rate  : NO verdict in %.0fs (guard would be silent)" % DUR)


asyncio.run(main())
