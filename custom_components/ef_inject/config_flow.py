"""Config flow for ef_inject.

There is nothing to configure. The flow exists for one reason: HA refuses to attach
an entity to a device unless the platform was loaded from a config entry
(`entity_platform.py`, `if self.config_entry:` ... `else: device = None`). Without an
entry the knobs would still work but would land on no device page, so the Ultra's card
could not carry them.

Single instance, keyed on the domain, so the import kick in async_setup can fire on
every start without accumulating entries.
"""

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult

from . import DOMAIN

TITLE = "EcoFlow meter injection"


class EfInjectConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def _single(self) -> ConfigFlowResult:
        await self.async_set_unique_id(DOMAIN)
        self._abort_if_unique_id_configured()
        return self.async_create_entry(title=TITLE, data={})

    async def async_step_import(self, import_data=None) -> ConfigFlowResult:
        """Kicked from async_setup on every start."""
        return await self._single()

    async def async_step_user(self, user_input=None) -> ConfigFlowResult:
        """So the entry can be re-added by hand if it is ever deleted."""
        return await self._single()
