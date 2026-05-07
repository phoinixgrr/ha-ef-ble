import asyncio
import time

from ..connection import ConnectionState
from ..devicebase import DeviceBase
from ..entity import controls
from ..packet import Packet
from ..pb import bk_series_pb2
from ..props import ProtobufProps, pb_field, proto_attr_mapper
from ..props.enums import IntFieldValue
from ..props.transforms import pround

pb = proto_attr_mapper(bk_series_pb2.DisplayPropertyUpload)

_TARGET_POWER_REFRESH_SEC = 30


class GridStatus(IntFieldValue):
    UNKNOWN = -1

    NOT_VALID = 0
    GRID_IN = 1
    GRID_OFFLINE = 2
    FEED_GRID = 3


class Device(DeviceBase, ProtobufProps):
    """STREAM Microinverter"""

    SN_PREFIX = (b"BK01", b"BK02", b"N011")
    NAME_PREFIX = "EF-BK"

    pv_power_1 = pb_field(pb.pow_get_pv, pround(2))
    pv_voltage_1 = pb_field(pb.plug_in_info_pv_vol, pround(1))
    pv_current_1 = pb_field(pb.plug_in_info_pv_amp, pround(2))

    pv_power_2 = pb_field(pb.pow_get_pv2, pround(2))
    pv_voltage_2 = pb_field(pb.plug_in_info_pv2_vol, pround(1))
    pv_current_2 = pb_field(pb.plug_in_info_pv2_amp, pround(2))

    grid_power = pb_field(pb.grid_connection_power)
    grid_voltage = pb_field(pb.grid_connection_vol, pround(2))
    grid_current = pb_field(pb.grid_connection_amp, pround(2))
    grid_frequency = pb_field(pb.grid_connection_freq, pround(2))
    grid_connection_status = pb_field(pb.grid_connection_sta, GridStatus.from_value)

    wifi_rssi = pb_field(pb.module_wifi_rssi, pround(0))
    feed_grid_mode_power_limit = pb_field(pb.feed_grid_mode_pow_limit)
    feed_grid_mode_power_max = pb_field(pb.feed_grid_mode_pow_max)

    # Telemetry additions ported from ecoflow-stream-ble-hack
    bms_mos_temperature_max = pb_field(pb.bms_max_mos_temp)
    bms_mos_temperature_min = pb_field(pb.bms_min_mos_temp)
    feed_grid_safety_power_max = pb_field(pb.feed_grid_safety_pow_max)
    debug_mode_enabled = pb_field(pb.debug_mode_enable)
    inverter_target_power = pb_field(pb.inv_target_pwr)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._target_power_value: int | None = None
        self._target_power_maintain_task: asyncio.Task | None = None
        self.on_disconnect(self._cancel_target_power_loop)
        self.on_connection_state_change(self._on_state_change_restart_loop)

    @classmethod
    def check(cls, sn):
        return sn[:4] in cls.SN_PREFIX

    async def packet_parse(self, data: bytes):
        return Packet.from_bytes(data, xor_payload=True)

    async def data_parse(self, packet: Packet):
        processed = False
        self.reset_updated()

        if packet.src == 0x02 and packet.cmd_set == 0xFE and packet.cmd_id == 0x15:
            self.update_from_bytes(bk_series_pb2.DisplayPropertyUpload, packet.payload)
            processed = True

        for field_name in self.updated_fields:
            self.update_callback(field_name)
            self.update_state(field_name, getattr(self, field_name))

        return processed

    async def _send_config_packet(self, message: bk_series_pb2.ConfigWrite):
        message.cfg_utc_time = round(time.time())
        payload = message.SerializeToString()
        packet = Packet(0x20, 0x02, 0xFE, 0x11, payload, 0x01, 0x01, 0x13)
        await self._conn.sendPacket(packet)

    async def _write_inverter_target_power(self, power: int):
        await self._send_config_packet(
            bk_series_pb2.ConfigWrite(cfg_inv_target_pwr=float(power))
        )

    async def _target_power_loop(self):
        try:
            while (
                self._target_power_value is not None
                and self._target_power_value > 0
                and self.is_connected
            ):
                try:
                    await self._write_inverter_target_power(self._target_power_value)
                    self._logger.debug(
                        "Target power refresh: %sW", self._target_power_value
                    )
                except Exception as exc:
                    self._logger.warning("Target power refresh failed: %s", exc)
                await asyncio.sleep(_TARGET_POWER_REFRESH_SEC)
        except asyncio.CancelledError:
            pass

    def _cancel_target_power_loop(self, *_args, **_kwargs):
        if self._target_power_maintain_task is not None:
            self._target_power_maintain_task.cancel()
            self._target_power_maintain_task = None

    def _on_state_change_restart_loop(self, state: ConnectionState):
        if (
            state == ConnectionState.AUTHENTICATED
            and self._target_power_value
            and self._target_power_value > 0
            and (
                self._target_power_maintain_task is None
                or self._target_power_maintain_task.done()
            )
        ):
            self._target_power_maintain_task = asyncio.create_task(
                self._target_power_loop()
            )

    @controls.power(inverter_target_power, min=0, max=2100)
    async def set_inverter_target_power(self, power: float):
        value = int(power)
        if value < 0:
            return False

        self._target_power_value = value
        try:
            await self._write_inverter_target_power(value)
        except Exception as exc:
            self._logger.warning("Failed initial target power write: %s", exc)
            return False

        if value > 0:
            if (
                self._target_power_maintain_task is None
                or self._target_power_maintain_task.done()
            ):
                self._target_power_maintain_task = asyncio.create_task(
                    self._target_power_loop()
                )
        else:
            self._cancel_target_power_loop()
        return True

    async def set_feed_grid_mode_pow_limit(self, power: int):
        if (
            power < 0
            or self.feed_grid_mode_power_max is None
            or power > self.feed_grid_mode_power_max
        ):
            return False

        await self._send_config_packet(
            bk_series_pb2.ConfigWrite(cfg_feed_grid_mode_pow_limit=power)
        )
        return True
