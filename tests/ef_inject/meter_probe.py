"""Read-only characterisation of the two Shelly Pro 3EMs over Modbus.

Answers four questions that decide the next round of work:
  1. How often does each meter's phase C value ACTUALLY change? The whole
     latency model assumes 1 Hz internal aggregation. Verify, do not assume.
  2. Are the two meters' update instants offset, and by how much? An offset is
     the entire premise of interleaving them.
  3. Do they AGREE on phase C under real load? If they do not, interleaving
     would inject a step the regulator would hunt on.
  4. What does a high-rate poll cost, and does polling faster keep the WiFi
     radio awake (i.e. does the 3ms fast path dominate at higher rates)?
"""
import asyncio, struct, sys, time

HOSTS = {"solar_158": "192.168.101.158", "grid_149": "192.168.101.149"}
REG_C = 1064
PORT = 502
UNIT = 1
HZ = 20.0
SECONDS = 40.0


class MB:
    def __init__(self, host):
        self.host = host
        self.r = self.w = None
        self.tid = 0
        self.lat = []
        self.err = 0

    async def read(self):
        if self.w is None or self.w.is_closing():
            try:
                self.r, self.w = await asyncio.wait_for(
                    asyncio.open_connection(self.host, PORT), timeout=1.0)
            except Exception:
                self.err += 1
                self.r = self.w = None
                return None
        self.tid = (self.tid + 1) & 0xFFFF
        t0 = time.time()
        try:
            self.w.write(struct.pack(">HHHBBHH", self.tid, 0, 6, UNIT, 4, REG_C, 2))
            await self.w.drain()
            head = await asyncio.wait_for(self.r.readexactly(9), timeout=1.0)
            tid, proto = struct.unpack(">HH", head[:4])
            fc, n = head[7], head[8]
            if tid != self.tid or proto or (fc & 0x80) or n != 4:
                raise ValueError("bad frame")
            body = await asyncio.wait_for(self.r.readexactly(4), timeout=1.0)
        except Exception:
            self.err += 1
            try:
                self.w.close()
            except Exception:
                pass
            self.r = self.w = None
            return None
        self.lat.append((time.time() - t0) * 1000)
        return struct.unpack(">f", body[2:4] + body[0:2])[0]

    async def close(self):
        if self.w:
            try:
                self.w.close()
                await self.w.wait_closed()
            except Exception:
                pass


def describe(name, changes):
    """changes = [(t_detected, old, new)]"""
    if len(changes) < 3:
        print("  %-10s too few changes (%d) to characterise" % (name, len(changes)))
        return None
    gaps = [b[0] - a[0] for a, b in zip(changes, changes[1:])]
    gaps_s = sorted(gaps)
    med = gaps_s[len(gaps_s) // 2]
    print("  %-10s %3d changes | gap med=%.3fs min=%.3fs max=%.3fs | implied rate %.2f Hz"
          % (name, len(changes), med, gaps_s[0], gaps_s[-1], 1.0 / med if med else 0))
    return med


async def main():
    ms = {k: MB(v) for k, v in HOSTS.items()}
    series = {k: [] for k in HOSTS}       # (t, value)
    changes = {k: [] for k in HOSTS}      # (t_detected, old, new)
    last = {k: None for k in HOSTS}
    paired = []                           # (t, v158, v149)

    print("polling %d meters at %.0f Hz for %.0fs ..." % (len(ms), HZ, SECONDS))
    t_end = time.time() + SECONDS
    period = 1.0 / HZ
    n = 0
    while time.time() < t_end:
        t_cycle = time.time()
        vals = await asyncio.gather(*(ms[k].read() for k in HOSTS))
        t = time.time()
        n += 1
        row = {}
        for k, v in zip(HOSTS, vals):
            if v is None:
                continue
            row[k] = v
            series[k].append((t, v))
            if last[k] is not None and v != last[k]:
                changes[k].append((t, last[k], v))
            last[k] = v
        if len(row) == 2:
            paired.append((t, row["solar_158"], row["grid_149"]))
        slp = period - (time.time() - t_cycle)
        if slp > 0:
            await asyncio.sleep(slp)

    for m in ms.values():
        await m.close()

    print("\n%d poll cycles, %d fully paired\n" % (n, len(paired)))

    print("Q1/Q2  value-change cadence (the real aggregation rate):")
    meds = {k: describe(k, changes[k]) for k in HOSTS}

    print("\nQ2  update-instant offset between the two meters:")
    c158 = [c[0] for c in changes["solar_158"]]
    c149 = [c[0] for c in changes["grid_149"]]
    if c158 and c149:
        offs = []
        for t in c158:
            near = min(c149, key=lambda x: abs(x - t))
            offs.append(near - t)
        offs.sort()
        med_off = offs[len(offs) // 2]
        print("  nearest-neighbour offset: med=%+.3fs min=%+.3fs max=%+.3fs" %
              (med_off, offs[0], offs[-1]))
        base = meds["solar_158"] or 1.0
        print("  as a fraction of the update period: %.2f  (0.5 = perfectly interleaved,"
              " 0 or 1 = in phase, no gain)" % (abs(med_off) / base))
    else:
        print("  not enough changes on both meters")

    print("\nQ3  do they agree on phase C right now?")
    if paired:
        d = [a - b for _, a, b in paired]
        d_s = sorted(d)
        mean_158 = sum(a for _, a, _ in paired) / len(paired)
        mean_149 = sum(b for _, _, b in paired) / len(paired)
        print("  mean  .158=%+.1fW   .149=%+.1fW   diff of means=%+.1fW" %
              (mean_158, mean_149, mean_158 - mean_149))
        print("  per-sample diff: med=%+.1fW  p05=%+.1fW  p95=%+.1fW  max|d|=%.1fW" %
              (d_s[len(d_s)//2], d_s[int(len(d_s)*0.05)],
               d_s[int(len(d_s)*0.95)], max(abs(x) for x in d)))
        rel = abs(mean_158 - mean_149) / max(1.0, abs(mean_158)) * 100
        print("  systematic offset: %.2f%% of reading" % rel)
    else:
        print("  no paired samples")

    print("\nQ4  read latency at %.0f Hz:" % HZ)
    for k, m in ms.items():
        if not m.lat:
            continue
        s = sorted(m.lat)
        fast = 100.0 * sum(1 for x in s if x < 10) / len(s)
        print("  %-10s n=%d med=%.1fms p95=%.1fms max=%.1fms  under10ms=%.0f%%  err=%d"
              % (k, len(s), s[len(s)//2], s[int(len(s)*0.95)], s[-1], fast, m.err))


asyncio.run(main())
