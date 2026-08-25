"""Shared base for the ef_inject knob entities.

Two rules the subclasses inherit and must not work around:

1. **No blocking I/O in a property.** Entity properties run on the event loop and are
   read often. Knob state comes from `inj.flags`, refreshed once per 5s status tick in
   the executor, never from `os.path.exists` inline.
2. **The file is the source of truth, not the entity.** Writers touch the file and let
   the regulator pick the value up through its existing validated read path, so the
   UI and a shell `echo` produce the same log line, the same counters and the same
   sticky-safe refusal. Nothing here sets `inj.bias_w` directly.
"""

from homeassistant.core import callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import Entity

from . import DOMAIN, EfInjectRuntime, SIGNAL_UPDATE


class EfInjectEntity(Entity):
    """Base entity, sitting on ef_ble's device page for the regulated Ultra."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    # NOT set, deliberately: `device_info` stays None so that entity_platform takes the
    # `device_entry` assigned below as final. Declaring ef_ble's identifiers in a
    # DeviceInfo would NOT join their device on HA 2026.8, because identifiers are
    # unique per config entry; it forges a nameless second device instead (observed on
    # 2026.8.3). Attaching at the entity level is the supported way to put controls from
    # one integration on another integration's device, and is what the template helpers
    # do. The cost is that we own no device row, which is also the point: nothing of
    # ours can clobber the fields ef_ble owns on a device we are only a guest on.
    _attr_device_info = None

    def __init__(self, inj, runtime: EfInjectRuntime, key: str) -> None:
        self._inj = inj
        self._runtime = runtime
        self._key = key
        self._attr_unique_id = f"{DOMAIN}_{key}"
        self._attr_translation_key = key
        self.device_entry = runtime.device_entry

    @property
    def available(self) -> bool:
        """Deliberately NOT tied to the BLE link.

        A dropped connection does not make the bias meaningless: the file still holds
        the cushion the regulator will use the moment the link returns. Greying the
        knob out exactly when someone is investigating a disconnect is the wrong
        behaviour, so these entities are available whenever the regulator object is.
        """
        return self._inj is not None

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(
            async_dispatcher_connect(self.hass, SIGNAL_UPDATE, self._handle_tick)
        )

    @callback
    def _handle_tick(self) -> None:
        self.async_write_ha_state()

    # -- knob helpers -------------------------------------------------------
    def _flag(self, name: str) -> bool | None:
        """Last known state of a knob file. None until the first status tick."""
        return self._inj.flags.get(name)
