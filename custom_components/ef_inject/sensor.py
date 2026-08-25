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

The two counters are LIFETIME totals, restored across restarts (see
`EfInjectRestoredCounter`), while the regulator's own counters and every number on the
SUMMARY log line stay per-session. That split is deliberate, not an inconsistency: the
log answers "how is this loop doing since it started", a graph answers "how often does
this happen to my house". Both readings are on the entity, the lifetime one as the state
and the session one as an attribute, so the two can always be reconciled.
"""

from dataclasses import dataclass

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import EntityCategory, UnitOfPower, UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import ExtraStoredData, RestoreEntity

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
    """Which path the meter reading came in by: `modbus`, `http-fallback` or `http`.

    The interesting value is the one nobody wants to see: `http-fallback` means the
    Modbus path failed and every injection is now riding a slower, coarser read. That
    degradation is otherwise invisible, because regulation keeps working.

    The regulator's `via=` field is `modbus:bound` / `modbus:grid`, and the suffix is
    dropped here on purpose. It names WHICH of the two meters won the freshness race on
    that cycle, which with the cross-check enabled alternates at roughly 1Hz, so
    reporting it verbatim made this sensor flip continuously and read as a flapping
    link when the link had never changed.

    It is not in the attributes either, and that is the second half of the same lesson:
    the recorder writes a states row when the ATTRIBUTES change, not just the state, so
    parking a value that moves every tick in an attribute costs exactly as many rows as
    a state would (measured: 36 attribute-only rows in 3 minutes). Only values that move
    when something has actually happened belong here. The per-cycle winner stays where
    it costs nothing: the `via=` field of the log, and the `second_meter_used` attribute
    on `sensor.ef_inject_status`, which is written every 5s regardless.

    Deliberately NOT an enum device class. The strings come from the regulator, so
    pinning a fixed options list here would make any future transport name show up as
    an invalid state rather than as itself.
    """

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:transit-connection-variant"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "transport")

    @property
    def native_value(self) -> str | None:
        via = self._inj.transport
        return None if via is None else via.split(":", 1)[0]

    @property
    def extra_state_attributes(self) -> dict:
        """Only counters that stand still while the link is healthy. See the note above:
        anything that ticks here costs a recorder row every 5s."""
        inj = self._inj
        return {
            "modbus_errors": inj._mb.err,
            "modbus_reopens": inj._mb.reopens,
            "second_meter_errors": inj._mb2.err,
        }


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


@dataclass
class _CounterSnapshot(ExtraStoredData):
    """What has to survive a restart: the total we reported AND the session counter it
    was derived from.

    Persisting the total alone is not enough. On the next start the entity cannot tell a
    restart (regulator counter back to 0, so everything it reported is history) from a
    config-entry reload (regulator still running, counter untouched, so the total it
    reported ALREADY includes the current session), and guessing wrong in the second
    direction counts the same events twice, permanently. Keeping the raw counter makes
    that decision a comparison rather than a guess.
    """

    total: int
    raw: int

    def as_dict(self) -> dict:
        return {"total": self.total, "raw": self.raw}


class EfInjectRestoredCounter(EfInjectEntity, RestoreEntity, SensorEntity):
    """A regulator session counter presented as a lifetime total.

    The regulator's counters live on the `Injector` object and start at 0 whenever a new
    one is built, which is correct for the log: the SUMMARY line means "since this loop
    started", and carrying totals across restarts would blend numbers from different code
    versions. For a graph that is the wrong reading, so the offset is restored here, in
    the entity, and the regulator is left alone. Nothing in this class is visible to the
    control loop.

    The state class stays TOTAL_INCREASING even though the value is now monotonic. That
    is the backstop: if the restore ever comes back empty (a wiped `.storage`, an entity
    renamed, a restore cache older than the retention) the state drops and HA reads it as
    a meter reset rather than as a large negative delta, so long-term statistics survive
    a failure of this class intact. That is the same mechanism that kept the `sum` column
    continuous before any of this existed.

    Two limits of the restore cache, neither worth engineering around: it is dumped every
    15 minutes and on a clean stop, so a hard kill can lose up to 15 minutes of counts,
    and it expires after 7 days, so an entity disabled for longer than that comes back at
    0. Both are undercounts of a diagnostic, never a wrong regulation decision, and the
    state class absorbs the second one.
    """

    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, inj, runtime: EfInjectRuntime, key: str) -> None:
        super().__init__(inj, runtime, key)
        self._offset = 0

    @property
    def _raw(self) -> int:
        """The regulator's own session counter. Overridden per metric."""
        raise NotImplementedError

    @property
    def native_value(self) -> int:
        return self._offset + self._raw

    @property
    def extra_state_attributes(self) -> dict:
        """Both attributes move only when the state does (or never), so neither costs a
        recorder row that the state change was not already paying for. `session` is what
        the log line shows, which is the whole point of exposing it: a discrepancy
        between the graph and the log is otherwise unexplainable."""
        return {"session": self._raw, "before_restart": self._offset}

    @property
    def extra_restore_state_data(self) -> _CounterSnapshot:
        return _CounterSnapshot(total=self.native_value, raw=self._raw)

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()

        extra = await self.async_get_last_extra_data()
        if extra is not None:
            saved = extra.as_dict()
            total, raw = self._as_count(saved.get("total")), self._as_count(saved.get("raw"))
        else:
            # Upgrade path: these entities already have a restore-cache state from before
            # this class existed, but no extra data. A missing raw is treated as 0, which
            # is right, because the only way to reach a new module version is a restart.
            last = await self.async_get_last_state()
            total, raw = self._as_count(None if last is None else last.state), 0

        if total is None:
            return          # nothing usable; start from 0 and let the state class cope
        if raw is None:
            raw = 0
        # A counter that did NOT go backwards means the same Injector is still running and
        # `total` already accounts for its current value, so only the part earned before
        # that session is carried. A counter that went backwards means a new session, so
        # the whole total is now history.
        self._offset = max(0, total - raw) if self._raw >= raw else total

    @staticmethod
    def _as_count(value) -> int | None:
        """Non-negative int or None. Guards the restore cache, which can legitimately
        hold `unknown`, `unavailable` or a value written by an older version."""
        try:
            n = int(float(value))
        except (TypeError, ValueError):
            return None
        return n if n >= 0 else None


class EfInjectCloudOverwrites(EfInjectRestoredCounter):
    """How many times the cloud has clobbered our injected value, lifetime.

    The absolute number is close to meaningless; the useful reading is its slope, which
    is how hard the cloud is currently fighting the injection. That is exactly why it is
    worth restoring: a slope is only comparable week to week if a restart does not chop
    the series into unrelated fragments.
    """

    _attr_icon = "mdi:cloud-alert"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "cloud_overwrites")

    @property
    def _raw(self) -> int:
        return self._inj.echo_cloud


class EfInjectWriteErrors(EfInjectRestoredCounter):
    """Failed writes to the device, lifetime.

    Counts `write_err` alone, on purpose. Folding in the skip counters
    (`not_ready`, `silent`, `stale`) would bury the signal: those are the regulator
    correctly declining to write, whereas this one is a write it tried and lost. Any
    slope at all here is worth looking at; the skips are normal and stay on the summary
    line and the status attributes.
    """

    _attr_icon = "mdi:alert-circle-outline"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "write_errors")

    @property
    def _raw(self) -> int:
        return self._inj.write_err
