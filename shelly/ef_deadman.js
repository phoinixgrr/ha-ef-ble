// ef_deadman.js -- device-resident dead-man switch for the EcoFlow zero-feed failsafe.
//
// Runs on the EcoFlow-bound "solar" meter, Shelly Pro 3EM at 192.168.101.158.
//
// WHY THIS EXISTS
// ef_inject owns cfg_cloud_metter over BLE. Its original design assumed the EcoFlow
// cloud was the fallback: "if the local Shelly read fails or goes stale, we STOP
// injecting and let the cloud take over" (ef_inject/__init__.py:17). Disabling Shelly
// Cloud on this meter on 2026-08-27 silently voided that, because the cloud now has no
// grid reading to push. ef_inject became a single point of failure: if BLE or HA dies
// during a PV surplus, the Ultras hold their last setpoint and export is unbounded.
//
// WHY IT LIVES ON THE METER AND NOT IN HA
// The single most common way injection stops is an HA restart, and during a restart no
// HA automation runs. A watchdog inside the thing being watched cannot cover that. This
// script runs on the meter, which stays up, so it covers HA restarts, HA crashes, host
// reboots, network partitions and BLE loss with one mechanism.
//
// HOW
// automation.ef_inject_dead_man_heartbeat pokes /script/1/beat every 10 s, but only while
// ef_inject is provably injecting (it gates on sensor.ef_inject_sends_per_10s, the delta
// of the BLE write counter, because that is the only signal that catches a dead Modbus
// link: state stays "running" and BLE stays fresh while nothing is being regulated).
// If the pokes stop for timeoutSec the script re-enables Shelly Cloud, which restores
// EcoFlow's own server-side zero-feed regulation. When the pokes come back and stay
// healthy for recoverSec it disables Shelly Cloud again so ef_inject regains the field
// uncontested.
//
// The heartbeat is an HTTP endpoint, deliberately NOT a KVS key. KVS lives in the NVS
// flash partition, and a 10 s write cadence would be ~3M writes/year of avoidable flash
// wear. This endpoint is RAM only. It is also NOT an outbound poll of the HA API, because
// that would mean storing a long-lived HA token on a device whose own auth is disabled.
//
// FAIL-SAFE DIRECTION: every uncertain state resolves to cloud ENABLED. Boot, unknown
// config, a failed read, a never-seen heartbeat: all of them mean "let the cloud
// regulate". The only state that disables the cloud is a proven-healthy heartbeat.
//
// SIDE EFFECT WORTH KNOWING: this script now OWNS Cloud.enable on this meter. Enabling
// Shelly Cloud here by hand (to reach the meter from the Shelly app, say) will be
// undone within recoverSec while ef_inject is healthy. Stop the script first if you
// want the cloud left on.

// TIMING BUDGET, measured not guessed. Re-enabling Shelly Cloud reaches Shelly Cloud in
// 4 s and EcoFlow resumes writing cfg_cloud_metter 8 s after that (measured 2026-08-27).
// Worst case to a regulating cloud is timeoutSec + tickMs + 8 s = 43 s. The existing hard
// stop cuts the plug at export >200 W sustained for 60 s, so 30 s here leaves ~17 s of
// margin and the plug should not fire for an ordinary outage. Raising timeoutSec above
// ~45 s would invert that order and make every outage end with the plant switched off
// awaiting a manual restore.
let CFG = {
  timeoutSec: 30,    // no heartbeat for this long -> restore the cloud
  recoverSec: 120,   // heartbeat healthy this long -> cut the cloud again
  tickMs: 5000,      // evaluation period
};

let st = {
  beats: 0,
  lastBeat: 0,        // seeded from uptime() below, see the startup grace note
  healthySince: 0,
  want: true,         // fail safe default, overridden below when the real state is known
  applied: null,      // our belief about the live Cloud.config.enable
  flips: 0,
  lastErr: "",
};

let uptime = function () {
  let s = Shelly.getComponentStatus("sys");
  if (s === null || s === undefined) return 0;
  return s.uptime;
};

// STARTUP GRACE. Seed the last beat to now so the script allows timeoutSec for HA's
// first poke before it judges anything. Without this the first tick fires 5s after
// start, sees an infinitely old heartbeat, and asserts the failsafe before the 10s
// beat cadence has had a chance to deliver even one beat.
st.lastBeat = uptime();

// Seed `applied` AND `want` from the real config. Seeding only `applied` is not enough:
// `want` would still start true, so a script start on a healthy system would find
// applied=false, want=true and immediately enable the cloud, only putting it back
// recoverSec later. Starting from the observed state means a start while healthy is a
// no-op, which matters because SetConfig writes flash.
// Unknown config still resolves to want=true, which is the fail-safe direction.
let cc = Shelly.getComponentConfig("cloud");
if (cc !== null && cc !== undefined) {
  st.applied = cc.enable;
  st.want = cc.enable;
}
print("EFDM start, cloud.enable =", st.applied);

let applyCloud = function (on) {
  Shelly.call("Cloud.SetConfig", { config: { enable: on } }, function (r, ec, em) {
    if (ec !== 0) {
      st.lastErr = "SetConfig ec=" + JSON.stringify(ec) + " " + em;
      print("EFDM SetConfig FAILED", ec, em);
      return;   // leave st.applied unchanged so the next tick retries
    }
    st.applied = on;
    st.flips = st.flips + 1;
    if (on) print("EFDM cloud ENABLED, failsafe active, ef_inject looks dead");
    else print("EFDM cloud disabled, ef_inject healthy, field released to BLE");
  });
};

HTTPServer.registerEndpoint("beat", function (req, res) {
  st.lastBeat = uptime();
  st.beats = st.beats + 1;
  res.code = 200;
  res.body = "ok";
  res.send();
});

// Read-only introspection so the failsafe can be validated from HA or curl without
// reading the script console.
HTTPServer.registerEndpoint("dm", function (req, res) {
  let up = uptime();
  res.code = 200;
  res.body = JSON.stringify({
    up: up,
    beats: st.beats,
    age: up - st.lastBeat,
    want_cloud: st.want,
    applied: st.applied,
    flips: st.flips,
    healthy_for: st.healthySince === 0 ? 0 : up - st.healthySince,
    timeout: CFG.timeoutSec,
    recover: CFG.recoverSec,
    err: st.lastErr,
  });
  res.send();
});

Timer.set(CFG.tickMs, true, function () {
  let up = uptime();
  let age = up - st.lastBeat;

  if (age > CFG.timeoutSec) {
    // Heartbeat lost. Assert the failsafe immediately, no grace: the whole point is
    // that export during a surplus is unbounded while nothing regulates.
    st.healthySince = 0;
    st.want = true;
  } else {
    // Heartbeat present. Only release the field back to BLE after a sustained healthy
    // window, so a flapping HA cannot flap the cloud with it.
    if (st.healthySince === 0) st.healthySince = up;
    if (up - st.healthySince >= CFG.recoverSec) st.want = false;
  }

  if (st.applied !== st.want) applyCloud(st.want);
}, null);

print("EFDM armed: timeout", CFG.timeoutSec, "s recover", CFG.recoverSec, "s");
