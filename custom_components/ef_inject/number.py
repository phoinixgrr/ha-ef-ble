"""The bias control.

This is the only entity in the integration that can change regulation, so it is the
one to read carefully.

Design, and why:

- **Bounds come from the safety constant.** min is `-BIAS_MAX_ABS`, max is `0`. The UI
  therefore cannot even offer an unsafe value.
- **`bias_is_safe` is called anyway**, the same function the regulator calls, not a
  copy of its logic. A bound is a convenience; the invariant is that a positive bias
  can never reach the device, because a positive bias tells the inverter it is
  importing when it is not, commands ramp-UP, and manufactures the very export this
  integration exists to prevent.
- **Rejection is loud.** `ServiceValidationError` surfaces in the UI immediately
  rather than letting the value silently snap back on the next tick.
- **The write goes to the FILE.** Never to `inj.bias_w`. Setting the attribute would
  bypass `_refresh_bias`, desync its `_bias_raw` content cache, skip the
  `EFINJECT BIAS -72W -> -80W` log line and skip the counters. Writing the file means
  the UI is just a faster way to type `echo -80 > /config/ef_inject_bias`.
- **`_bias_next_check = 0` afterwards** so the regulator picks it up on the next poll
  (~0.25s) instead of up to `BIAS_REFRESH_SEC` later.
- **BOX, not SLIDER.** A dragged slider fires every intermediate value, each one a
  file write plus a regulation change plus a log line.
"""

from homeassistant.components.number import (
    NumberDeviceClass,
    NumberEntity,
    NumberMode,
)
from homeassistant.const import UnitOfPower
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import (
    BIAS_FILE,
    BIAS_MAX_ABS,
    DOMAIN,
    EfInjectRuntime,
    bias_is_safe,
    write_text_atomic,
)
from .entity import EfInjectEntity


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    async_add_entities([EfInjectBias(hass.data[DOMAIN], entry.runtime_data)])


class EfInjectBias(EfInjectEntity, NumberEntity):
    _attr_device_class = NumberDeviceClass.POWER
    _attr_native_unit_of_measurement = UnitOfPower.WATT
    _attr_native_min_value = float(-BIAS_MAX_ABS)
    _attr_native_max_value = 0.0
    _attr_native_step = 1.0
    _attr_mode = NumberMode.BOX
    # Deliberately NOT EntityCategory.CONFIG. Category CONFIG banishes an entity to the
    # collapsed Configuration list; this is the knob the operator actually turns, so it
    # sits in the main controls beside the Ultra's own power settings.
    _attr_icon = "mdi:tune-variant"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "bias")

    @property
    def native_value(self) -> float:
        """The bias actually in force, which is what the regulator will use.

        Read from the Injector rather than from the file, so a value the regulator
        REFUSED never shows up here as though it had taken effect.
        """
        return float(self._inj.bias_w)

    async def async_set_native_value(self, value: float) -> None:
        bias = int(round(value))
        if not bias_is_safe(bias):
            raise ServiceValidationError(
                f"Refusing bias {bias:+d}W: it must be <= 0 and |bias| <= "
                f"{BIAS_MAX_ABS}. A positive bias would command ramp-up and "
                f"manufacture real export."
            )
        await self.hass.async_add_executor_job(
            write_text_atomic, BIAS_FILE, f"{bias}\n"
        )
        # Collapse the read throttle so the change lands on the next poll and the
        # regulator's own log line appears immediately.
        self._inj._bias_next_check = 0.0
