"""The existence-flag knobs, as switches.

Each one is a file whose mere presence means "on". Toggling writes or removes the file
and lets the regulator notice, exactly as touching it from a shell does, so the
existing log lines (including the interleave "RE-ARMED by hand" message and its
escalation reset) still fire.

Note the inversion on `stop`. The file means STOP, but a switch labelled "stop" that
sits ON during normal operation reads backwards to everyone, and a kill switch that is
misread is worse than one that is hard to reach. Exposed as "Regulation" where ON
means regulating, so a mis-tap looks wrong on the card at a glance.

None of these carry `EntityCategory.CONFIG`. They did, and the result was the operator
hunting for a switch they own across two collapsed cards. CONFIG is for settings that
describe how an integration talks to a device; every switch here changes what the
regulator DOES to the house, which is a control. Diagnostics (the read-only views and
the causation test) keep DIAGNOSTIC, because those are for reading, not for acting.
"""

from homeassistant.components.switch import SwitchEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import DOMAIN, FLAG_FILES, EfInjectRuntime, set_flag_file
from .entity import EfInjectEntity


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    inj = hass.data[DOMAIN]
    runtime = entry.runtime_data
    async_add_entities(
        [
            EfInjectRegulationSwitch(inj, runtime),
            EfInjectFlagSwitch(inj, runtime, "modbus", icon="mdi:lan-connect"),
            EfInjectFlagSwitch(inj, runtime, "interleave", icon="mdi:shield-check"),
            EfInjectFlagSwitch(inj, runtime, "shadow", icon="mdi:eye-outline"),
            # Diagnostics. Off by default in the registry: useful when investigating,
            # noise on a dashboard the rest of the time.
            EfInjectFlagSwitch(
                inj, runtime, "quiet", icon="mdi:volume-off", enabled=False
            ),
            EfInjectFlagSwitch(inj, runtime, "ab", icon="mdi:ab-testing", enabled=False),
        ]
    )


class EfInjectFlagSwitch(EfInjectEntity, SwitchEntity):
    def __init__(
        self, inj, runtime: EfInjectRuntime, key: str, icon: str, enabled: bool = True
    ) -> None:
        super().__init__(inj, runtime, key)
        self._attr_icon = icon
        self._attr_entity_registry_enabled_default = enabled
        self._path = FLAG_FILES[key]

    @property
    def is_on(self) -> bool | None:
        return self._flag(self._key)

    async def async_turn_on(self, **kwargs) -> None:
        await self._write(True)

    async def async_turn_off(self, **kwargs) -> None:
        await self._write(False)

    async def _write(self, present: bool) -> None:
        await self.hass.async_add_executor_job(set_flag_file, self._path, present)
        # Optimistic, then corrected by the next 5s tick from the real stat. The knob
        # should not appear to do nothing for five seconds.
        self._inj.flags[self._key] = present
        self.async_write_ha_state()


class EfInjectRegulationSwitch(EfInjectFlagSwitch):
    """Inverted view of the stop file. ON means regulating."""

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "stop", icon="mdi:shield-sync")
        # Own unique_id and name, because the entity is the logical inverse of the
        # file it drives and calling it "stop" would invite exactly the misreading
        # this class exists to avoid.
        self._attr_unique_id = f"{DOMAIN}_regulation"
        self._attr_translation_key = "regulation"

    @property
    def is_on(self) -> bool | None:
        stopped = self._flag("stop")
        return None if stopped is None else not stopped

    async def async_turn_on(self, **kwargs) -> None:
        await self._write(False)      # regulating = no stop file

    async def async_turn_off(self, **kwargs) -> None:
        await self._write(True)
