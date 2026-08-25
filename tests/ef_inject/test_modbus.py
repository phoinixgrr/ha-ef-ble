"""Modbus wire-protocol and read-path tests for ef_inject.

A fake Modbus TCP server speaks the same dialect the Shelly Pro 3EM does,
including its float32 low-word-first layout and its FC3 exception, so the
framing, the word swap, the persistent socket, the reopen-after-failure
behaviour and the HTTP fallback are exercised against something that can
actually disagree with us.
"""
import sys, asyncio, os, tempfile
from harness import FakeShelly, ck, report, load, touch, rm

efi = load()


async def main():
    ck(efi.MODBUS_FC_READ_INPUT == 4,
       "read uses FC4 (FC3 returns exception code 2 on this meter)")

    srv = await FakeShelly().start()
    efi.MODBUS_PORT = srv.port
    m = efi.ModbusMeter("127.0.0.1", "test")

    tmp = tempfile.mkdtemp()
    efi.MODBUS_FILE = os.path.join(tmp, "ef_inject_modbus")
    efi.SHADOW_FILE = os.path.join(tmp, "ef_inject_shadow149")
    efi.INTERLEAVE_FILE = os.path.join(tmp, "ef_inject_interleave")

    # ---- happy path, both signs -----------------------------------------
    srv.value = 967.4
    v = await m.read()
    ck(v is not None and abs(v - 967.4) < 0.01, "reads +967.4W back exactly (%r)" % v)
    ck(srv.requests[-1] == (1, efi.MODBUS_UNIT, 4, efi.MODBUS_REG_C_ACT_POWER, 2),
       "request is FC4 unit=%d reg=%d count=2: %r"
       % (efi.MODBUS_UNIT, efi.MODBUS_REG_C_ACT_POWER, srv.requests[-1]))

    srv.value = -312.75
    v = await m.read()
    ck(v is not None and abs(v + 312.75) < 0.01,
       "EXPORT reads back negative, sign preserved (%r)" % v)

    srv.value = 0.0
    v = await m.read()
    ck(v == 0.0, "exactly zero survives the round trip")

    # ---- change tracking, which drives interleave + staleness metrics ----
    srv.value = 100.0
    await m.read()
    t_change = m.last_change_ts
    ck(t_change > 0, "a new value stamps last_change_ts")
    await asyncio.sleep(0.02)
    await m.read()                       # same value again
    ck(m.last_change_ts == t_change,
       "re-reading an UNCHANGED value does not restamp last_change_ts")
    srv.value = 100.0 + efi.WRITE_ON_CHANGE_W / 2.0
    await m.read()
    ck(m.last_change_ts == t_change,
       "a sub-threshold wiggle is not treated as a new publication")
    srv.value = 140.0
    await m.read()
    ck(m.last_change_ts > t_change, "a real change restamps last_change_ts")

    # ---- persistent socket ----------------------------------------------
    before = srv.connections
    for _ in range(5):
        await m.read()
    ck(srv.connections == before,
       "5 further reads reused ONE socket (connections still %d)" % srv.connections)
    ck(m.reopens == 1, "only one connect so far (reopens=%d)" % m.reopens)

    tids = [r[0] for r in srv.requests]
    ck(len(set(tids)) == len(tids), "every request carried a distinct transaction id")

    # ---- a mismatched tid is rejected and the socket dropped ------------
    srv.mode = "wrong_tid"
    err_before, ok_before = m.err, m.reads
    v = await m.read()
    ck(v is None, "reply with the wrong transaction id is REFUSED, not parsed")
    ck(m.err == err_before + 1, "mismatch counted as an error")
    ck(m.reads == ok_before, "mismatch not counted as a good read")
    ck(m._writer is None,
       "socket dropped after desync (a stale reply must not become the next answer)")

    # ---- recovery: next read reconnects ---------------------------------
    srv.mode = "ok"
    srv.value = 123.5
    reopens_before = m.reopens
    v = await m.read()
    ck(v is not None and abs(v - 123.5) < 0.01,
       "reconnects on the next cycle and reads fine")
    ck(m.reopens == reopens_before + 1, "reconnect counted in reopens")

    # ---- malformed replies are refused ----------------------------------
    srv.mode = "short_count"
    ck(await m.read() is None, "truncated data block refused rather than half-decoded")
    srv.mode = "garbage"
    ck(await m.read() is None, "modbus exception response refused")
    srv.mode = "ok"

    # ---- the server going away is survivable ----------------------------
    await srv.stop()
    await m.close()
    ck(await m.read() is None, "meter unreachable -> None, no exception escapes")
    ck(m._writer is None, "no half-open socket left behind")

    # ---- read path through the Injector ---------------------------------
    touch(efi.MODBUS_FILE)
    inj = efi.Injector(object())
    inj._mb = efi.ModbusMeter("127.0.0.1", "bound")     # nothing listening now
    inj._mb2 = efi.ModbusMeter("127.0.0.1", "grid")

    calls = []

    async def fake_http():
        calls.append(1)
        return 555.0
    inj._read_http = fake_http

    # The compliance-relevant branch: a dead Modbus path must not mean a skipped
    # cycle, because a skipped cycle lets the last injected value ride.
    v = await inj._read_local()
    ck(v == 555.0, "Modbus down -> HTTP fallback supplied the sample (%r)" % v)
    ck(len(calls) == 1, "fallback hit HTTP exactly once")
    ck(inj.transport == "http-fallback", "transport labelled honestly: %r" % inj.transport)
    ck(inj.skipped_read_err == 0, "a successful fallback is NOT counted as a skipped read")
    ck(inj.last_local is not None and inj.last_local[1] == 555.0,
       "fallback sample still lands in last_local, so the cycle can inject")
    ck(inj.polls == 1, "the poll was counted")
    ck(inj._src_change_ts == 0.0,
       "an HTTP sample claims no publication timestamp, so it cannot fake a "
       "freshness figure")

    rm(efi.MODBUS_FILE)
    calls.clear()
    v = await inj._read_local()
    ck(v == 555.0 and len(calls) == 1, "toggle removed -> HTTP used directly")
    ck(inj.transport == "http", "transport reports plain http when toggle is off")

    # ---- plausibility clamp still applies -------------------------------
    async def crazy():
        return 99999.0
    inj._read_http = crazy
    err_before = inj.skipped_read_err
    ck(await inj._read_local() is None, "implausible value rejected on the HTTP path")
    ck(inj.skipped_read_err == err_before + 1, "implausible value counted")

    srv2 = await FakeShelly(99999.0).start()
    efi.MODBUS_PORT = srv2.port
    touch(efi.MODBUS_FILE)
    inj._mb = efi.ModbusMeter("127.0.0.1", "bound")

    async def none_http():
        return None
    inj._read_http = none_http
    err_before = inj.skipped_read_err
    ck(await inj._read_local() is None,
       "implausible value rejected on the MODBUS path too")
    ck(inj.skipped_read_err == err_before + 1, "implausible Modbus value counted")
    await inj._modbus_close()
    await srv2.stop()
    rm(efi.MODBUS_FILE)

    return report()


sys.exit(asyncio.run(main()))
