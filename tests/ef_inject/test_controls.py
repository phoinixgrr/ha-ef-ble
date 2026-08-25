"""Tests for the entity layer: the knob helpers and the bias control.

Loaded differently from the other suites, on purpose:

  * the regulator suites load `mod_under_test.py`, a swappable COPY, because proving a
    regulator fix means running the same checks against the old and the new module;
  * this suite imports the REAL package `custom_components.ef_inject`, because the
    thing under test is `number.py` / `switch.py` / `sensor.py`, which use relative
    imports and therefore only exist inside the package.

What matters here is that a UI action cannot reach the device by any path other than
the validated one, and that the guard string shown on a dashboard cannot drift from the
one written to the log.
"""
import asyncio
import json
import os
import sys
import tempfile
import types

import harness  # installs the base homeassistant/aiohttp stubs

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PKG = os.path.join(REPO, "custom_components", "ef_inject")

# ---- the entity surface the platforms import ------------------------------
# Only what the modules actually touch. Anything missing shows up immediately as an
# ImportError rather than as a silently skipped test.
_ha = sys.modules["homeassistant"]
_const = sys.modules["homeassistant.const"]


class _StrEnum(str):
    pass


_const.EntityCategory = types.SimpleNamespace(CONFIG="config", DIAGNOSTIC="diagnostic")
_const.UnitOfPower = types.SimpleNamespace(WATT="W")
_const.UnitOfTime = types.SimpleNamespace(MILLISECONDS="ms")

_exc = sys.modules["homeassistant.exceptions"]


class ServiceValidationError(Exception):
    pass


_exc.ServiceValidationError = ServiceValidationError

_core = sys.modules["homeassistant.core"]
_core.callback = lambda f: f

_dr = sys.modules["homeassistant.helpers.device_registry"]
_dr.DeviceInfo = dict
_dr.DeviceEntryType = types.SimpleNamespace(SERVICE="service")

_disp = sys.modules["homeassistant.helpers.dispatcher"]
_disp.async_dispatcher_connect = lambda hass, sig, cb: (lambda: None)


class Entity:
    """Mirrors the parts of homeassistant.helpers.entity.Entity we rely on.

    `device_info` defaulting to `_attr_device_info` and `device_entry` defaulting to
    None are not incidental: entity_platform reads exactly these two to decide whether
    to create a device for us or accept the one we point at.
    """

    hass = None
    _attr_unique_id = None
    _attr_translation_key = None
    _attr_device_info = None
    _attr_entity_category = None
    device_entry = None

    @property
    def device_info(self):
        return self._attr_device_info

    @property
    def unique_id(self):
        return self._attr_unique_id

    @property
    def translation_key(self):
        return self._attr_translation_key

    @property
    def entity_category(self):
        return self._attr_entity_category

    def async_write_ha_state(self):
        pass

    def async_on_remove(self, func):
        pass

    async def async_added_to_hass(self):
        pass


def _install(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


_install("homeassistant.helpers.entity", Entity=Entity)
_install("homeassistant.helpers.entity_platform", AddEntitiesCallback=object)
_install("homeassistant.components", __path__=[])
_install(
    "homeassistant.components.number",
    NumberEntity=Entity,
    NumberDeviceClass=types.SimpleNamespace(POWER="power"),
    NumberMode=types.SimpleNamespace(BOX="box", SLIDER="slider"),
)
_install("homeassistant.components.switch", SwitchEntity=Entity)
_install("homeassistant.components.button", ButtonEntity=Entity)
_install(
    "homeassistant.components.sensor",
    SensorEntity=Entity,
    SensorDeviceClass=types.SimpleNamespace(POWER="power", DURATION="duration"),
    SensorStateClass=types.SimpleNamespace(
        MEASUREMENT="measurement", TOTAL_INCREASING="total_increasing"
    ),
)

sys.path.insert(0, REPO)
import custom_components.ef_inject as efi                      # noqa: E402
from custom_components.ef_inject import button as efi_button   # noqa: E402
from custom_components.ef_inject import number as efi_number   # noqa: E402
from custom_components.ef_inject import sensor as efi_sensor    # noqa: E402
from custom_components.ef_inject import switch as efi_switch    # noqa: E402

# ---- scaffolding ----------------------------------------------------------
fails = []


def ck(cond, msg):
    if cond:
        print("PASS", msg)
    else:
        fails.append(msg)
        print("FAIL", msg)


class FakeHass:
    """Just enough hass: the executor hop the knob writers depend on."""

    def __init__(self):
        self.data = {}

    async def async_add_executor_job(self, func, *args):
        return func(*args)


class FakeEntry:
    def __init__(self, title, unique_id=None, runtime_data=None):
        self.title = title
        self.unique_id = unique_id
        self.runtime_data = runtime_data


class FakeConfigEntries:
    def __init__(self, entries):
        self._entries = entries

    def async_entries(self, domain):
        return self._entries


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


TMP = tempfile.mkdtemp(prefix="ef_inject_controls_")


def tmp(name):
    return os.path.join(TMP, name)


# ---- 1. atomic write ------------------------------------------------------
p = tmp("bias")
efi.write_text_atomic(p, "-80\n")
ck(open(p).read() == "-80\n", "write_text_atomic writes the value")
ck(not os.path.exists(p + ".tmp"), "write_text_atomic leaves no .tmp behind")
efi.write_text_atomic(p, "-90\n")
ck(open(p).read() == "-90\n", "write_text_atomic overwrites in place")

# The reader must never see a half-written file. os.replace is atomic, so at every
# instant the path either holds the old bytes or the new ones, never a prefix.
saved = efi.BIAS_FILE
efi.BIAS_FILE = p
ck(efi._read_bias_file() == "-90", "the regulator's reader parses an atomically written file")
efi.BIAS_FILE = saved

# ---- 2. flag files -------------------------------------------------------
f = tmp("flag")
efi.set_flag_file(f, True)
ck(os.path.exists(f), "set_flag_file(True) creates the flag")
efi.set_flag_file(f, True)
ck(os.path.exists(f), "set_flag_file(True) is idempotent")
efi.set_flag_file(f, False)
ck(not os.path.exists(f), "set_flag_file(False) removes the flag")
efi.set_flag_file(f, False)
ck(not os.path.exists(f), "removing an absent flag is not an error")

# Every knob file constant must appear in FLAG_FILES, or a new knob silently gets no
# switch and no entry in inj.flags.
knob_consts = {
    n: getattr(efi, n)
    for n in dir(efi)
    if n.endswith("_FILE") and n != "BIAS_FILE" and isinstance(getattr(efi, n), str)
}
missing = set(knob_consts.values()) - set(efi.FLAG_FILES.values())
ck(not missing, f"FLAG_FILES covers every knob file constant (missing: {sorted(missing)})")

flags = efi.stat_flags()
ck(set(flags) == set(efi.FLAG_FILES), "stat_flags reports every knob")
ck(all(isinstance(v, bool) for v in flags.values()), "stat_flags reports booleans")

# ---- 3. locating the ef_ble entry ---------------------------------------
dev = object()
hass = FakeHass()
hass.config_entries = FakeConfigEntries(
    [FakeEntry("something else"), FakeEntry(efi.TARGET_TITLE, runtime_data=dev)]
)
ck(efi.find_ef_ble_entry(hass).runtime_data is dev, "entry found by title")

hass.config_entries = FakeConfigEntries(
    [FakeEntry("Renamed By Hand", unique_id=efi.TARGET_ADDRESS, runtime_data=dev)]
)
ck(
    efi.find_ef_ble_entry(hass).runtime_data is dev,
    "a renamed entry is still found, by BLE address",
)

hass.config_entries = FakeConfigEntries([FakeEntry("nope", unique_id="AA:BB")])
ck(efi.find_ef_ble_entry(hass) is None, "no match yields None, not a wrong device")

inj = efi.Injector(hass)
hass.config_entries = FakeConfigEntries(
    [FakeEntry("Renamed By Hand", unique_id=efi.TARGET_ADDRESS, runtime_data=dev)]
)
ck(inj._device() is dev, "_device() resolves through the same helper")
hass.config_entries = FakeConfigEntries([])
ck(inj._device() is None, "_device() yields None with no entries")

# ---- 4. the bias control -------------------------------------------------
bias_path = tmp("bias_ctl")
efi.BIAS_FILE = bias_path
efi_number.BIAS_FILE = bias_path

hass = FakeHass()
inj = efi.Injector(hass)
hass.data[efi.DOMAIN] = inj
BLE_DEV = types.SimpleNamespace(id="b12d7b4ebb606adbe034a0713fb8f15a", name="EF-60434")
RT = efi.EfInjectRuntime(address="CC:BA:97:E3:3C:52", device_entry=BLE_DEV)
ent = efi_number.EfInjectBias(inj, RT)
ent.hass = hass

ck(ent._attr_native_max_value == 0.0, "the UI cannot offer a positive bias")
ck(
    ent._attr_native_min_value == float(-efi.BIAS_MAX_ABS),
    "the UI cannot offer a bias beyond BIAS_MAX_ABS",
)
ck(ent._attr_mode == "box", "box mode, so dragging cannot fire a write per pixel")
ck(ent.native_value == float(efi.BIAS_W), "native_value reports the bias in force")
ck(
    ent.unique_id,
    "the entity has a stable unique_id",
)

# Device attachment. entity_platform only honours a pre-set `device_entry` when
# `device_info` is None (entity_platform.py:969-971); the moment device_info returns
# anything, HA creates a device of OUR OWN instead, and on 2026.8 that is a nameless
# duplicate of the Ultra because identifiers are unique per config entry. Both halves
# of that contract are pinned here, because either one silently undoes the other.
ck(
    ent.device_info is None,
    "device_info is None, so entity_platform takes device_entry as final  <-- the fix",
)
ck(
    ent.device_entry is BLE_DEV,
    "the entity points straight at ef_ble's device row, owning no device of its own",
)

inj._bias_next_check = 1e12
run(ent.async_set_native_value(-90))
ck(open(bias_path).read().strip() == "-90", "a UI set writes the bias FILE")
ck(inj.bias_w == efi.BIAS_W, "a UI set does NOT touch inj.bias_w directly")
ck(inj._bias_next_check == 0.0, "a UI set collapses the read throttle")

# The value only becomes real by going through the regulator's own validated read.
run(inj._refresh_bias())
ck(inj.bias_w == -90, "the regulator picks the new bias up on its next read")
ck(inj.bias_overrides == 1, "the override counter moved, so the log line fired")

for bad in (10, 1, efi.BIAS_MAX_ABS + 1, -(efi.BIAS_MAX_ABS + 1)):
    raised = False
    try:
        run(ent.async_set_native_value(bad))
    except ServiceValidationError:
        raised = True
    ck(raised, f"a bias of {bad:+d}W is refused at the entity, loudly")
ck(
    open(bias_path).read().strip() == "-90",
    "a refused value never reaches the file, so the previous cushion stands",
)

# ---- 5. the kill switch is presented the right way round ----------------
stop_path = tmp("stop")
efi.FLAG_FILES["stop"] = stop_path
sw = efi_switch.EfInjectRegulationSwitch(inj, RT)
sw.hass = hass
inj.flags = {"stop": False}
ck(sw.is_on is True, "regulation switch reads ON while there is no stop file")
run(sw.async_turn_off())
ck(os.path.exists(stop_path), "turning regulation OFF creates the stop file")
ck(sw.is_on is False, "and the switch immediately reads OFF")
run(sw.async_turn_on())
ck(not os.path.exists(stop_path), "turning regulation ON removes the stop file")
ck(sw.is_on is True, "and the switch immediately reads ON")
inj.flags = {}
ck(sw.is_on is None, "before the first status tick the state is unknown, not a guess")

# ---- 6. the guard string cannot drift from the log ---------------------
mb, il = tmp("modbus"), tmp("interleave")
efi.MODBUS_FILE, efi.INTERLEAVE_FILE = mb, il
efi.FLAG_FILES["modbus"], efi.FLAG_FILES["interleave"] = mb, il
guard = efi_sensor.EfInjectGuard(inj, RT)

for modbus_on, il_on, locked, label in [
    (False, False, False, "both knobs off"),
    (True, False, False, "modbus only"),
    (True, True, False, "guard armed"),
    (True, True, True, "guard latched off"),
]:
    efi.set_flag_file(mb, modbus_on)
    efi.set_flag_file(il, il_on)
    inj.interleave_locked = locked
    inj.flags = efi.stat_flags()
    ck(
        guard.native_value == inj._interleave_state(),
        f"sensor and log agree on the guard state: {label}",
    )

# ---- 7. every entity has a name, and the platforms wire up ---------------
# A translation_key with no matching entry in strings.json does not fail loudly: the
# entity just shows up as a raw object id, which is precisely the unreadable-legend
# problem this naming pass set out to fix. So walk the REAL platform setups, which also
# proves each one accepts `entry.runtime_data` and attaches to ef_ble's device.
strings = json.load(open(os.path.join(PKG, "strings.json")))
en = json.load(open(os.path.join(PKG, "translations", "en.json")))
ck(en == strings, "translations/en.json is in sync with strings.json")

collected = []
hass.data[efi.DOMAIN] = inj
for mod, domain in (
    (efi_number, "number"),
    (efi_switch, "switch"),
    (efi_button, "button"),
    (efi_sensor, "sensor"),
):
    got = []
    run(mod.async_setup_entry(hass, FakeEntry("x", runtime_data=RT), got.extend))
    ck(bool(got), f"the {domain} platform sets up from entry.runtime_data")
    collected += [(domain, e) for e in got]

ck(len(collected) == 17, f"all 17 entities are created (got {len(collected)})")
ck(
    len({e.unique_id for _, e in collected}) == len(collected),
    "every unique_id is distinct, so no entity silently displaces another",
)
ck(
    all(e.device_entry is BLE_DEV for _, e in collected),
    "every entity lands on the Ultra's device, not just the ones tested above",
)

nameless = [
    f"{d}.{e.translation_key}"
    for d, e in collected
    if not strings["entity"].get(d, {}).get(e.translation_key, {}).get("name")
]
ck(not nameless, f"every entity has a name in strings.json (missing: {nameless})")

# Which card each entity lands on, pinned exactly. This is not cosmetic: a stray
# EntityCategory.CONFIG moves a control the operator owns into a separate collapsed
# card, and the only symptom is someone failing to find it in a hurry. Anything the
# operator ACTS on carries no category; only the read-only views and the test button
# are DIAGNOSTIC.
EXPECTED_CATEGORY = {
    "regulation": None,       # controls, main card
    "bias": None,
    "bias_reset": None,
    "cushion": None,
    "modbus": None,
    "interleave": None,
    "shadow": None,
    "quiet": None,
    "ab": None,
    "bias_in_force": "diagnostic",
    "guard": "diagnostic",
    "causation": "diagnostic",
    "bias_applied": "diagnostic",
    "transport": "diagnostic",
    "freshness": "diagnostic",
    "cloud_overwrites": "diagnostic",
    "write_errors": "diagnostic",
}
misfiled = [
    f"{e.translation_key}={e.entity_category!r} want {EXPECTED_CATEGORY.get(e.translation_key)!r}"
    for _, e in collected
    if e.entity_category != EXPECTED_CATEGORY.get(e.translation_key, "MISSING")
]
ck(not misfiled, f"every entity is on its intended card (misfiled: {misfiled})")
ck(
    set(EXPECTED_CATEGORY) == {e.translation_key for _, e in collected},
    "the card map covers exactly the entities that exist, so a new one cannot slip in",
)

# ---- 8. the metric sensors report the regulator, not their own arithmetic ----
# Each is a straight read of one attribute. The point of testing something this thin is
# the two places it could still be wrong: reporting 0 for "no data yet", and reporting
# the session mean where the last sample was meant.
inj.bias_applied = -41
inj.transport = "http-fallback"
inj.echo_cloud = 7
inj.write_err = 2
inj.fresh_ms_last = None
inj.fresh_ms_sum, inj.fresh_ms_n = 9000.0, 10      # mean 900ms, deliberately different

metrics = {
    "bias_applied": efi_sensor.EfInjectBiasApplied(inj, RT),
    "transport": efi_sensor.EfInjectTransport(inj, RT),
    "freshness": efi_sensor.EfInjectFreshness(inj, RT),
    "cloud_overwrites": efi_sensor.EfInjectCloudOverwrites(inj, RT),
    "write_errors": efi_sensor.EfInjectWriteErrors(inj, RT),
}
ck(metrics["bias_applied"].native_value == -41, "the applied offset is the faded value")
ck(
    metrics["bias_applied"].native_value != inj.bias_w,
    "and is not the configured ceiling, which is the whole reason it exists",
)
ck(
    metrics["transport"].native_value == "http-fallback",
    "the transport reports the link when there is no meter suffix to strip",
)
# The suffix names which meter won the freshness race, which alternates at ~1Hz with
# the cross-check on. Reporting it as the STATE made the sensor look like a flapping
# link and cost a recorder row every 5s to say nothing changed.
inj.transport = "modbus:grid"
ck(
    metrics["transport"].native_value == "modbus",
    "the per-cycle meter suffix is stripped, so the link state is stable  <-- was a bug",
)
inj.transport = "modbus:bound"
ck(
    metrics["transport"].native_value == "modbus",
    "and the state does not move when only the winning meter changes",
)
# And the suffix is not smuggled into an attribute either: the recorder writes a row on
# any ATTRIBUTE change too, so a value that moves every tick costs the same there as it
# does in the state. Every attribute on this sensor must stand still while healthy.
attrs = metrics["transport"].extra_state_attributes
volatile = {"via", "last_meter", "second_meter_used", "sec_used"}
ck(
    not (volatile & set(attrs)),
    f"no per-tick value hides in the attributes (found: {sorted(volatile & set(attrs))})",
)
ck(
    set(attrs) == {"modbus_errors", "modbus_reopens", "second_meter_errors"},
    "the attributes are exactly the counters that move only when something breaks",
)
inj.transport = "http-fallback"
ck(
    metrics["freshness"].native_value is None,
    "freshness is unknown before the first write, NOT 0ms",
)
inj.fresh_ms_last = 163.42
ck(metrics["freshness"].native_value == 163.4, "freshness reports the LAST sample")
ck(
    metrics["freshness"].native_value != round(inj.fresh_ms_sum / inj.fresh_ms_n, 1),
    "and not the session mean, which would hide a regression in progress",
)
ck(metrics["cloud_overwrites"].native_value == 7, "cloud overwrites read echo_cloud")
ck(metrics["write_errors"].native_value == 2, "write failures read write_err alone")
inj.skipped_not_ready = inj.skipped_silent = inj.skipped_stale = 99
ck(
    metrics["write_errors"].native_value == 2,
    "and exclude the skips, which are the regulator declining to write, not failing",
)
ck(
    all(
        e._attr_state_class == "total_increasing"
        for k, e in metrics.items()
        if k in ("cloud_overwrites", "write_errors")
    ),
    "the counters are TOTAL_INCREASING, so a restart back to 0 is not a negative spike",
)
ck(
    metrics["transport"].native_value is not None
    and getattr(metrics["transport"], "_attr_device_class", None) is None,
    "the transport carries no enum device class, so a new source name is not 'invalid'",
)

print()
if fails:
    print("FAILURES:")
    for m in fails:
        print(" -", m)
    raise SystemExit(1)
print("ALL CHECKS PASSED")
