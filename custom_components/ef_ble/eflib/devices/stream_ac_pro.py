from ..entity import controls
from ..packet import Packet
from ..pb import bk_series_pb2
from ..props import ProtobufProps, pb_field
from . import stream_ac

pb = stream_ac.pb


class Device(stream_ac.Device, ProtobufProps):
    """STREAM AC PRO"""

    SN_PREFIX = (b"BK31",)

    ac_power_1 = pb_field(pb.pow_get_schuko1)
    ac_power_2 = pb_field(pb.pow_get_schuko2)

    ac_1 = pb_field(pb.relay2_onoff)
    ac_2 = pb_field(pb.relay3_onoff)

    async def data_parse(self, packet: Packet):
        if (
            packet.src == 0x35
            and packet.cmd_set == 0x01
            and packet.cmd_id == Packet.NET_BLE_COMMAND_CMD_SET_RET_TIME
            and len(packet.payload) == 0
        ):
            self._time_commands.async_send_all()
            return True

        return await super().data_parse(packet)

    @controls.outlet(ac_1)
    async def enable_ac_1(self, enable: bool):
        await self._send_config_packet(
            bk_series_pb2.ConfigWrite(cfg_relay2_onoff=enable)
        )

    @controls.outlet(ac_2)
    async def enable_ac_2(self, enable: bool):
        await self._send_config_packet(
            bk_series_pb2.ConfigWrite(cfg_relay3_onoff=enable)
        )
