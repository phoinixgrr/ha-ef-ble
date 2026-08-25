"""Read-only: run the SHIPPING ModbusMeter against BOTH real meters and compare
with the HTTP RPC. No writes, no EcoFlow traffic.

Also measures what the interleave would actually have bought: for each cycle, how
much fresher the chosen meter's publication is than the bound meter's.
"""
import sys, asyncio, json, time, urllib.request
from harness import load

efi = load()

BOUND = efi.MODBUS_HOST
GRID = efi.SECOND_HOST


def http_c():
    with urllib.request.urlopen(efi.SHELLY_URL, timeout=2) as r:
        return json.load(r)["c_act_power"]


async def main():
    m1 = efi.ModbusMeter(BOUND, "bound")
    m2 = efi.ModbusMeter(GRID, "grid")
    lat = []
    gains = []
    diffs = []
    print("  #   bound      grid     http    diff   gain    ms")
    for i in range(20):
        t0 = time.time()
        v1, v2 = await asyncio.gather(m1.read(), m2.read())
        lat.append((time.time() - t0) * 1000)
        hp = http_c()
        gain = ""
        if v1 is not None and v2 is not None:
            diffs.append(v1 - v2)
            g = (m2.last_change_ts - m1.last_change_ts) * 1000
            if g > 0:
                gains.append(g)
                gain = "+%.0fms" % g
        print("  %2d %8s %8s %8s %7s %6s %5.1f"
              % (i,
                 "n/a" if v1 is None else "%.1f" % v1,
                 "n/a" if v2 is None else "%.1f" % v2,
                 "%.1f" % hp,
                 "n/a" if (v1 is None or v2 is None) else "%.1f" % (v1 - v2),
                 gain, lat[-1]))
        await asyncio.sleep(0.25)
    await m1.close()
    await m2.close()

    lat.sort()
    print("\nbound ok=%d err=%d reopens=%d | grid ok=%d err=%d reopens=%d"
          % (m1.reads, m1.err, m1.reopens, m2.reads, m2.err, m2.reopens))
    print("paired read latency p50=%.1fms p95=%.1fms max=%.1fms"
          % (lat[len(lat) // 2], lat[int(len(lat) * 0.95)], lat[-1]))
    if diffs:
        print("phase C agreement: mean=%+.1fW max|d|=%.1fW  (guard trips at a "
              "sustained %.0fW)"
              % (sum(diffs) / len(diffs), max(abs(d) for d in diffs),
                 efi.INTERLEAVE_DIVERGE_W))
    if gains:
        print("cycles where the grid meter was fresher: %d of %d, avg gain %.0fms"
              % (len(gains), len(diffs), sum(gains) / len(gains)))
    else:
        print("no cycle saw the grid meter fresher (run longer, or they are in phase)")


asyncio.run(main())
