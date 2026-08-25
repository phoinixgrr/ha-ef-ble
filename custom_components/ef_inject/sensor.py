"""Read-only view of the regulator, as real entities.

`sensor.ef_inject_status` already carries every counter, but it is written with
`hass.states.async_set`, so it is not in the entity registry: it cannot be renamed,
cannot sit on a device page, and its attributes are not recorded as statistics. These
are proper entities for the numbers worth graphing and the strings worth seeing on the
card.

What is deliberately NOT here, because it already exists elsewhere and a second copy is
just recorder rows: battery power and the device's own grid figure (ef_ble publishes
both on this same device page), the real phase-C reading (the Shelly integration
publishes it), and the volume counters (`sent`, `polls`, `mb_ok`, `verdicts`), which
stay as attributes on `sensor.ef_inject_status` because they are read while debugging,
not plotted.

Cadence matters more than count. Every entity here refreshes on the 5s status tick, not
on the 2Hz regulation loop; at 2Hz a continuously moving sensor would be ~170k recorder
rows per day each. If a metric is ever added that needs sub-5s resolution, it does not
belong in the entity layer.
"""

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import EntityCategory, UnitOfPower, UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import DOMAIN, EfInjectRuntime, bias_settle_w
from .entity import EfInjectEntity


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    inj = hass.data[DOMAIN]
    runtime = entry.runtime_data
    async_add_entities(
        [
            EfInjectCushion(inj, runtime),
            EfInjectBiasSensor(inj, runtime),
            EfInjectGuard(inj, runtime),
            EfInjectBiasApplied(inj, runtime),
            EfInjectTransport(inj, runtime),
            EfInjectFreshness(inj, runtime),
            EfInjectCloudOverwrites(inj, runtime),
            EfInjectWriteErrors(inj, runtime),
        ]
    )


class EfInjectCushion(EfInjectEntity, SensorEntity):
    """Predicted resting point of REAL grid import for the bias in force.

    This is the number that actually matters to an operator: the bias is the knob, the
    cushion is what it buys. Prediction, not measurement, so it is the value to check
    the meter against rather than a second copy of the meter.
    """

    _attr_device_class = SensorDeviceClass.POWER
    _attr_native_unit_of_measurement = UnitOfPower.WATT
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 0
    _attr_icon = "mdi:transmission-tower-import"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "cushion")

    @property
    def native_value(self) -> float:
        return round(bias_settle_w(self._inj.bias_w), 1)


class EfInjectBiasSensor(EfInjectEntity, SensorEntity):
    """The bias in force. Duplicates the number's value on purpose.

    The number is a control, so its history is a record of what was TYPED; this is the
    recorded, graphable history of what the regulator ACCEPTED, which is what makes an
    export event afterwards explainable. They differ whenever a value is refused.
    """

    _attr_device_class = SensorDeviceClass.POWER
    _attr_native_unit_of_measurement = UnitOfPower.WATT
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 0
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:tune-variant"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "bias_in_force")

    @property
    def native_value(self) -> int:
        return self._inj.bias_w


class EfInjectGuard(EfInjectEntity, SensorEntity):
    """State of the two-meter divergence guard, in the words the log already uses.

    The wording is duplicated from `Injector._interleave_state()` rather than reused,
    because that method reaches for the filesystem twice and this is a property on the
    event loop. The strings are kept identical on purpose so a dashboard and a log line
    never disagree; if one changes, change both.
    """

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:shield-check"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "guard")

    @property
    def native_value(self) -> str:
        inj = self._inj
        if not (inj.flags.get("modbus") and inj.flags.get("interleave")):
            return "off(not enabled)"
        if inj.interleave_locked:
            return "OFF(locked, self-recovers at %d/%d good)" % (
                inj.sec_good_run, inj._recover_strikes_needed())
        return "ON"

    @property
    def extra_state_attributes(self) -> dict:
        inj = self._inj
        return {
            "verdicts": inj.sec_verdicts,
            "skipped_busy": inj.sec_skipped_busy,
            "bad_run": inj.sec_bad_run,
            "good_run": inj.sec_good_run,
            "good_run_needed": inj._recover_strikes_needed(),
            "locks": inj.interleave_locks,
            "recoveries": inj.interleave_recoveries,
            "rearms": inj.interleave_rearms,
        }


class EfInjectBiasApplied(EfInjectEntity, SensorEntity):
    """The offset actually on the wire, after the proximity fade.

    Not a duplicate of `bias_in_force`, which is the CEILING the operator configured.
    This is what was added to the last injected value, so it reads 0 whenever real
    import is comfortably above the fade window and only opens up as the house
    approaches export. When someone asks why import is resting where it is, this is the
    number that explains it; when someone asks what the regulator was told to aim for,
    that is the other one.
    """

    _attr_device_class = SensorDeviceClass.POWER
    _attr_native_unit_of_measurement = UnitOfPower.WATT
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 0
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:tune"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "bias_applied")

    @property
    def native_value(self) -> int:
        return self._inj.bias_applied


class EfInjectTransport(EfInjectEntity, SensorEntity):
    """Which path the meter reading came in by, verbatim from the log's `via=` field.

    The interesting value is the one nobody wants to see: `http-fallback` means the
    Modbus path failed and every injection is now riding a slower, coarser read. That
    degradation is otherwise invisible, because regulation keeps working.

    Deliberately NOT an enum device class. The strings come from the regulator
    (`modbus:` plus the source it bound to), so pinning a fixed options list here would
    make any future source name show up as an invalid state instead of as itself.
    """

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:transit-connection-variant"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "transport")

    @property
    def native_value(self) -> str | None:
        return self._inj.transport


class EfInjectFreshness(EfInjectEntity, SensorEntity):
    """Age of the meter reading at the moment it was injected.

    This is the metric the Modbus transport and the interleave exist to improve, so it
    is the one to watch after any change to either: the regulator can be perfectly
    healthy by every counter while quietly injecting values that are half a second old.
    The LAST sample, not the session mean, which flattens out and hides a regression in
    progress. `stale_avg` on the summary line remains the session mean.
    """

    _attr_device_class = SensorDeviceClass.DURATION
    _attr_native_unit_of_measurement = UnitOfTime.MILLISECONDS
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 0
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:timer-sand"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "freshness")

    @property
    def native_value(self) -> float | None:
        """None until the first write, so an empty history is never shown as 0ms."""
        ms = self._inj.fresh_ms_last
        return None if ms is None else round(ms, 1)


class EfInjectCloudOverwrites(EfInjectEntity, SensorEntity):
    """How many times the cloud has clobbered our injected value this session.

    TOTAL_INCREASING rather than TOTAL: it resets to 0 on every restart, which is
    exactly the reset semantics that state class is defined for. The absolute number is
    close to meaningless; the useful reading is its slope, which is how hard the cloud
    is currently fighting the injection.
    """

    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:cloud-alert"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "cloud_overwrites")

    @property
    def native_value(self) -> int:
        return self._inj.echo_cloud


class EfInjectWriteErrors(EfInjectEntity, SensorEntity):
    """Failed writes to the device this session.

    Counts `write_err` alone, on purpose. Folding in the skip counters
    (`not_ready`, `silent`, `stale`) would bury the signal: those are the regulator
    correctly declining to write, whereas this one is a write it tried and lost. Any
    slope at all here is worth looking at; the skips are normal and stay on the summary
    line and the status attributes.
    """

    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:alert-circle-outline"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "write_errors")

    @property
    def native_value(self) -> int:
        return self._inj.write_err
