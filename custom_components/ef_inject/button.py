"""One-shot actions.

`bias_reset` exists because a number cannot express "absent", and absent is what makes
the regulator fall back to the built-in default. Deleting the override file is the only
way to say "stop overriding" rather than "override with the same value".
"""

from homeassistant.components.button import ButtonEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import BIAS_FILE, DOMAIN, FLAG_FILES, EfInjectRuntime, set_flag_file
from .entity import EfInjectEntity


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    inj = hass.data[DOMAIN]
    runtime = entry.runtime_data
    async_add_entities(
        [
            EfInjectBiasReset(inj, runtime),
            EfInjectCausation(inj, runtime),
        ]
    )


class EfInjectBiasReset(EfInjectEntity, ButtonEntity):
    # No category, so it lands next to the offset it resets. See EfInjectBias.
    _attr_icon = "mdi:restore"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "bias_reset")

    async def async_press(self) -> None:
        # set_flag_file(present=False) is just "remove, tolerate absent", which is
        # exactly the semantics wanted here even though bias is not a flag file.
        # The regulator logs the revert itself, naming the default it fell back to.
        await self.hass.async_add_executor_job(set_flag_file, BIAS_FILE, False)
        self._inj._bias_next_check = 0.0


class EfInjectCausation(EfInjectEntity, ButtonEntity):
    """Arm one causation run. The loop consumes and deletes the file itself."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_entity_registry_enabled_default = False
    _attr_icon = "mdi:test-tube"

    def __init__(self, inj, runtime: EfInjectRuntime) -> None:
        super().__init__(inj, runtime, "causation")

    async def async_press(self) -> None:
        await self.hass.async_add_executor_job(
            set_flag_file, FLAG_FILES["causation"], True
        )
        self._inj.flags["causation"] = True
