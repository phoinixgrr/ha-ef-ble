#!/usr/bin/env python3
"""Deploy ef_deadman.js to the EcoFlow-bound solar meter.

Creates (or reuses) the script, uploads the code in chunks because Script.PutCode has a
per-call body limit, then verifies the stored code matches the local file byte for byte.
Does NOT start the script: starting it asserts the failsafe immediately, because no
heartbeat has been seen yet. Start it only once the HA beat side is live.

Usage:
  deploy_ef_deadman.py [--start] [--stop] [--delete] [--status]
"""
import json
import sys
import urllib.request

HOST = "192.168.101.158"
NAME = "ef_deadman"
SRC = "/Users/phoinix/Repos/ha-ef-ble/shelly/ef_deadman.js"
CHUNK = 1024


def rpc(method, params=None):
    body = json.dumps({"id": 1, "method": method, "params": params or {}}).encode()
    req = urllib.request.Request(
        "http://%s/rpc" % HOST, data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        out = json.loads(r.read().decode())
    if "error" in out:
        raise SystemExit("RPC %s failed: %s" % (method, out["error"]))
    return out.get("result", {})


def find_id():
    for s in rpc("Script.List")["scripts"]:
        if s["name"] == NAME:
            return s["id"]
    return None


def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "--deploy"
    sid = find_id()

    if arg == "--status":
        print("script id:", sid)
        if sid is not None:
            print("config:", json.dumps(rpc("Script.GetConfig", {"id": sid})))
            print("status:", json.dumps(rpc("Script.GetStatus", {"id": sid})))
        print("cloud :", json.dumps(rpc("Cloud.GetConfig")))
        print("cloudst:", json.dumps(rpc("Cloud.GetStatus")))
        return

    if arg == "--delete":
        if sid is None:
            print("nothing to delete")
            return
        try:
            rpc("Script.Stop", {"id": sid})
        except SystemExit:
            pass
        rpc("Script.Delete", {"id": sid})
        print("deleted script", sid)
        return

    if arg == "--stop":
        rpc("Script.Stop", {"id": sid})
        print("stopped", sid, json.dumps(rpc("Script.GetStatus", {"id": sid})))
        return

    if arg == "--start":
        rpc("Script.SetConfig", {"id": sid, "config": {"enable": True}})
        rpc("Script.Start", {"id": sid})
        print("started", sid, json.dumps(rpc("Script.GetStatus", {"id": sid})))
        return

    # ---- deploy ----------------------------------------------------------
    code = open(SRC).read()
    if sid is None:
        sid = rpc("Script.Create", {"name": NAME})["id"]
        print("created script id", sid)
    else:
        print("reusing script id", sid)
        try:
            rpc("Script.Stop", {"id": sid})
            print("  stopped it first")
        except SystemExit:
            pass

    # append=False on the first chunk truncates any previous code.
    first = True
    for i in range(0, len(code), CHUNK):
        rpc("Script.PutCode",
            {"id": sid, "code": code[i:i + CHUNK], "append": not first})
        first = False
    print("uploaded %d bytes in %d chunks" % (len(code), (len(code) + CHUNK - 1) // CHUNK))

    got = rpc("Script.GetCode", {"id": sid})["data"]
    if got == code:
        print("VERIFIED: stored code matches local file exactly (%d bytes)" % len(got))
    else:
        raise SystemExit("MISMATCH: stored %d bytes, local %d bytes" % (len(got), len(code)))
    print("config:", json.dumps(rpc("Script.GetConfig", {"id": sid})))
    print("NOT started. Stand up the HA heartbeat first, then --start.")


main()
