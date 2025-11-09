import logging
import json
from typing import Any, cast, override

from homeassistant.components.binary_sensor import BinarySensorDeviceClass # pyright: ignore[reportMissingImports]
from homeassistant.helpers.entity import EntityCategory # pyright: ignore[reportMissingImports]

from custom_components.ecoflow_cloud.api import EcoflowApiClient
from custom_components.ecoflow_cloud.api.message import JSONDict
from custom_components.ecoflow_cloud.devices import const, BaseDevice
from custom_components.ecoflow_cloud.devices.const import ATTR_DESIGN_CAPACITY, ATTR_FULL_CAPACITY, ATTR_REMAIN_CAPACITY, BATTERY_CHARGING_STATE, \
    MAIN_DESIGN_CAPACITY, MAIN_FULL_CAPACITY, MAIN_REMAIN_CAPACITY
from custom_components.ecoflow_cloud.entities import BaseSensorEntity, BaseNumberEntity, BaseSwitchEntity, BaseSelectEntity
from custom_components.ecoflow_cloud.number import ChargingPowerEntity, MaxBatteryLevelEntity, MinBatteryLevelEntity, BatteryBackupLevel
from custom_components.ecoflow_cloud.select import DictSelectEntity, TimeoutDictSelectEntity
from custom_components.ecoflow_cloud.sensor import LevelSensorEntity, RemainSensorEntity, TempSensorEntity, \
    CyclesSensorEntity, InWattsSensorEntity, OutWattsSensorEntity, VoltSensorEntity, InMilliampSensorEntity, \
    InVoltSensorEntity, MilliVoltSensorEntity, InMilliVoltSensorEntity, \
    OutMilliVoltSensorEntity, ChargingStateSensorEntity, CapacitySensorEntity, StatusSensorEntity, \
    QuotaStatusSensorEntity, OutVoltSensorEntity
from custom_components.ecoflow_cloud.switch import BeeperEntity, EnabledEntity
from homeassistant.util import dt

from .proto.support.const import Command, CommandFuncAndId, get_expected_payload_type, AddressId
from .proto.support.message import ProtoMessage

_LOGGER = logging.getLogger(__name__)


class River3ChargingStateSensorEntity(BaseSensorEntity):
    """ChargingStateSensorEntity for River3 with inverted values.
    
    River3 uses: 0=idle, 1=discharging, 2=charging
    Standard expects: 0=unused, 1=charging, 2=discharging
    """
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:battery-charging"
    _attr_device_class = BinarySensorDeviceClass.BATTERY_CHARGING

    def _update_value(self, val: Any) -> bool:
        if val == 0:
            return super()._update_value("unused")
        elif val == 1:
            return super()._update_value("discharging")  # River3: 1 = discharging
        elif val == 2:
            return super()._update_value("charging")  # River3: 2 = charging
        else:
            return False


class OutWattsAbsSensorEntity(OutWattsSensorEntity):
    """OutWattsSensorEntity that returns absolute values (converts negative to positive)"""
    def _update_value(self, val: Any) -> bool:
        return super()._update_value(abs(int(val)))


def _json_to_proto_command(json_cmd: dict[str, Any], device_sn: str) -> ProtoMessage:
    """Convert JSON command to ProtoMessage for River3.
    
    River3 uses protobuf for commands, but the command structure uses JSON-like
    params. We encode the JSON params as bytes in the protobuf payload.
    """
    from .proto.ecopacket_pb2 import SendHeaderMsg
    
    # Create a SendHeaderMsg with the JSON params encoded as bytes
    packet = SendHeaderMsg()
    message = packet.msg.add()
    
    # Set command function and ID (254_17 for River3 set commands)
    message.cmd_func = 254
    message.cmd_id = 17
    
    # Encode JSON params as bytes
    json_bytes = json.dumps(json_cmd).encode('utf-8')
    message.pdata = json_bytes
    message.data_len = len(json_bytes)
    
    # Set source and destination
    message.src = AddressId.APP.value
    message.dest = AddressId.IOT2.value  # dest:2 from markdown
    message.device_sn = device_sn
    
    # Create a wrapper ProtoMessage that bypasses normal payload verification
    # by overriding to_proto_message to return our custom packet
    class River3ProtoMessage(ProtoMessage):
        def __init__(self, packet: SendHeaderMsg):
            # Don't call super().__init__() with command/payload since we're bypassing that
            from ...api.message import Message
            Message.__init__(self)
            self._packet = packet
        
        def to_proto_message(self):
            return self._packet
        
        def _verify_command_and_payload(self):
            pass  # Skip verification for custom commands
        
        def private_api_to_mqtt_payload(self):
            return self._packet.SerializeToString()
    
    return River3ProtoMessage(packet)


class River3(BaseDevice):

    @staticmethod
    def default_charging_power_step() -> int:
        return 50

    @override
    def _prepare_data(self, raw_data: bytes) -> dict[str, Any]:
        res: dict[str, Any] = {"params": {}}
        from google.protobuf.json_format import MessageToDict
        from .proto.support import flatten_dict

        from .proto.ecopacket_pb2 import SendHeaderMsg
        from .proto.support.const import Command, CommandFuncAndId

        try:
            packet = SendHeaderMsg()
            _ = packet.ParseFromString(raw_data)
            for message in packet.msg:
                _LOGGER.debug(
                    'cmd_func %u, cmd_id %u, payload "%s"',
                    message.cmd_func,
                    message.cmd_id,
                    message.pdata.hex(),
                )

                if (
                    message.HasField("device_sn")
                    and message.device_sn != self.device_data.sn
                ):
                    _LOGGER.info(
                        "Ignoring EcoPacket for SN %s on topic for SN %s",
                        message.device_sn,
                        self.device_data.sn,
                    )

                command_desc = CommandFuncAndId(
                    func=message.cmd_func, id=message.cmd_id
                )

                try:
                    command = Command(command_desc)
                except ValueError:
                    # Log cmd_func 254 messages even if not recognized, as they might be DisplayPropertyUpload/RuntimePropertyUpload
                    if command_desc.func == 254:
                        _LOGGER.info(
                            "River3 received cmd_func 254, cmd_id %u (not recognized as Command enum)",
                            command_desc.id,
                        )
                    else:
                        _LOGGER.info(
                            "Unsupported EcoPacket cmd_func %u, cmd_id %u",
                            command_desc.func,
                            command_desc.id,
                        )
                    continue

                params = cast(JSONDict, res.setdefault("params", {}))
                if command in {Command.PRIVATE_API_SMART_METER_DISPLAY_PROPERTY_UPLOAD, Command.PRIVATE_API_SMART_METER_RUNTIME_PROPERTY_UPLOAD}:
                    payload = get_expected_payload_type(command)()
                    try:
                        if message.enc_type == 1:
                            message.pdata = bytes([byte ^ (message.seq % 256) for byte in message.pdata])

                        _ = payload.ParseFromString(message.pdata)
                        flattened = cast(
                            JSONDict,
                            flatten_dict(MessageToDict(payload, preserving_proto_field_name=False)),
                        )
                        
                        # Log which command type and some key fields for debugging
                        key_fields = [k for k in flattened.keys() if 'Ac' in k or 'powGet' in k]
                        _LOGGER.info(
                            "River3 parsed %s (cmd_func %u, cmd_id %u) - found %d fields, AC/power fields: %s",
                            command.name,
                            command_desc.func,
                            command_desc.id,
                            len(flattened),
                            key_fields[:10] if key_fields else "none"
                        )
                        
                        params.update(
                            (f"{command.func}_{command.id}.{key}", value)
                            for key, value in flattened.items()
                        )
                    except Exception as e:
                        _LOGGER.error(f"Error parsing protobuf payload for {command.name}: {e}", exc_info=True)
                        
                res["timestamp"] = dt.utcnow()
        except Exception as error:
            _LOGGER.error(error)
            _LOGGER.info(raw_data.hex())
        return res

    def sensors(self, client: EcoflowApiClient) -> list[BaseSensorEntity]:
        return [
            # DisplayPropertyUpload fields (254_21)
            LevelSensorEntity(client, self, "254_21.bmsBattSoc", const.MAIN_BATTERY_LEVEL)
                .attr("254_21.bmsDesignCap", ATTR_DESIGN_CAPACITY, 0)
                .attr("254_22.bmsFullCap", ATTR_FULL_CAPACITY, 0)
                .attr("254_22.bmsRemainCap", ATTR_REMAIN_CAPACITY, 0),
            CapacitySensorEntity(client, self, "254_21.bmsDesignCap", MAIN_DESIGN_CAPACITY, False),
            CapacitySensorEntity(client, self, "254_22.bmsFullCap", MAIN_FULL_CAPACITY, False),
            CapacitySensorEntity(client, self, "254_22.bmsRemainCap", MAIN_REMAIN_CAPACITY, False),

            LevelSensorEntity(client, self, "254_21.bmsBattSoh", const.SOH),

            LevelSensorEntity(client, self, "254_21.cmsBattSoc", const.COMBINED_BATTERY_LEVEL),

            River3ChargingStateSensorEntity(client, self, "254_21.bmsChgDsgState", BATTERY_CHARGING_STATE),

            InWattsSensorEntity(client, self, "254_21.powInSumW", const.TOTAL_IN_POWER).with_energy(),
            OutWattsSensorEntity(client, self, "254_21.powOutSumW", const.TOTAL_OUT_POWER).with_energy(),

            InMilliampSensorEntity(client, self, "254_22.plugInInfoPvAmp", const.SOLAR_IN_CURRENT),
            InVoltSensorEntity(client, self, "254_22.plugInInfoPvVol", const.SOLAR_IN_VOLTAGE),

            InWattsSensorEntity(client, self, "254_21.powGetAcIn", const.AC_IN_POWER),
            OutWattsAbsSensorEntity(client, self, "254_21.powGetAcOut", const.AC_OUT_POWER),

            InVoltSensorEntity(client, self, "254_22.plugInInfoAcInVol", const.AC_IN_VOLT),
            OutVoltSensorEntity(client, self, "254_22.plugInInfoAcOutVol", const.AC_OUT_VOLT),

            InWattsSensorEntity(client, self, "254_21.powGetPv", const.SOLAR_IN_POWER),

            OutWattsSensorEntity(client, self, "254_21.powGet_12v", const.DC_OUT_POWER),
            OutWattsAbsSensorEntity(client, self, "254_21.powGetTypec1", const.TYPEC_1_OUT_POWER),
            OutWattsAbsSensorEntity(client, self, "254_21.powGetQcusb1", const.USB_QC_1_OUT_POWER),
            OutWattsAbsSensorEntity(client, self, "254_21.powGetQcusb2", const.USB_QC_2_OUT_POWER),

            RemainSensorEntity(client, self, "254_21.bmsChgRemTime", const.CHARGE_REMAINING_TIME),
            RemainSensorEntity(client, self, "254_21.bmsDsgRemTime", const.DISCHARGE_REMAINING_TIME),
            RemainSensorEntity(client, self, "254_21.cmsChgRemTime", const.REMAINING_TIME),

            TempSensorEntity(client, self, "254_22.tempPcsDc", "PCS DC Temperature"),
            TempSensorEntity(client, self, "254_22.tempPcsAc", "PCS AC Temperature"),
            # Cycles from BMSHeartBeatReport - may need different handling
            CyclesSensorEntity(client, self, "254_21.cycles", const.CYCLES),

            TempSensorEntity(client, self, "254_21.bmsMinCellTemp", const.BATTERY_TEMP)
                .attr("254_21.bmsMaxCellTemp", const.ATTR_MAX_CELL_TEMP, 0),
            TempSensorEntity(client, self, "254_21.bmsMaxCellTemp", const.MAX_CELL_TEMP, False),

            VoltSensorEntity(client, self, "254_22.bmsBattVol", const.BATTERY_VOLT, False)
                .attr("254_22.bmsMinCellVol", const.ATTR_MIN_CELL_VOLT, 0)
                .attr("254_22.bmsMaxCellVol", const.ATTR_MAX_CELL_VOLT, 0),
            MilliVoltSensorEntity(client, self, "254_22.bmsMinCellVol", const.MIN_CELL_VOLT, False),
            MilliVoltSensorEntity(client, self, "254_22.bmsMaxCellVol", const.MAX_CELL_VOLT, False),

            self._status_sensor(client),

        ]

    def numbers(self, client: EcoflowApiClient) -> list[BaseNumberEntity]:
        return [
            MaxBatteryLevelEntity(client, self, "254_21.cmsMaxChgSoc", const.MAX_CHARGE_LEVEL, 50, 100,
                                  lambda value: {"moduleType": 2, "operateType": "upsConfig",
                                                 "params": {"maxChgSoc": int(value)}}),

            MinBatteryLevelEntity(client, self, "254_21.cmsMinDsgSoc", const.MIN_DISCHARGE_LEVEL, 0, 30,
                                  lambda value: {"moduleType": 2, "operateType": "dsgCfg",
                                                 "params": {"minDsgSoc": int(value)}}),

            ChargingPowerEntity(client, self, "254_21.plugInInfoAcInChgPowMax", const.AC_CHARGING_POWER, 50, 305,
                                lambda value: {"moduleType": 5, "operateType": "acChgCfg",
                                               "params": {"chgWatts": int(value), "chgPauseFlag": 255}}),

            BatteryBackupLevel(client, self, "254_21.energyBackupStartSoc", const.BACKUP_RESERVE_LEVEL, 5, 100,
                               "254_21.cmsMinDsgSoc", "254_21.cmsMaxChgSoc",
                               lambda value: {"moduleType": 1, "operateType": "watthConfig",
                                              "params": {"isConfig": 1,
                                                         "energyBackupStartSoc": int(value),
                                                         "minDsgSoc": 0,
                                                         "minChgSoc": 0}}),
        ]

    def switches(self, client: EcoflowApiClient) -> list[BaseSwitchEntity]:
        device = self
        
        return [
            BeeperEntity(client, self, "254_21.enBeep", const.BEEPER,
                         lambda value: _json_to_proto_command(
                             {"moduleType": 5, "operateType": "quietMode",
                              "params": {"enabled": value}}, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.cfgAcOutOpen", const.AC_ENABLED,
                          lambda value, params: _json_to_proto_command(
                              {"moduleType": 5, "operateType": "acOutCfg",
                               "params": {"enabled": value, "out_voltage": -1, "out_freq": 255,
                                          "xboost": params.get("254_21.xboostEn", 255) if params else 255}}, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.xboostEn", const.XBOOST_ENABLED,
                          lambda value, params: _json_to_proto_command(
                              {"moduleType": 5, "operateType": "acOutCfg",
                               "params": {"enabled": params.get("254_21.cfgAcOutOpen", 255) if params else 255, "out_voltage": -1, "out_freq": 255,
                                          "xboost": value}}, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.cfgDc12vOutOpen", const.DC_ENABLED,
                          lambda value: _json_to_proto_command(
                              {"moduleType": 5, "operateType": "mpptCar",
                               "params": {"enabled": value}}, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.energyBackupEn", const.BP_ENABLED,
                          lambda value, params: _json_to_proto_command(
                              {"moduleType": 1, "operateType": "watthConfig",
                               "params": {"isConfig": value,
                                          "energyBackupStartSoc": value * 50,
                                          "minDsgSoc": 0,
                                          "minChgSoc": 0}}, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.outputPowerOffMemory", const.AC_ALWAYS_ENABLED,
                          lambda value, params: _json_to_proto_command(
                              {"moduleType": 1, "operateType": "acAutoOutConfig",
                               "params": {"acAutoOutConfig": value,
                                          "minAcOutSoc": int(params.get("254_21.cmsMinDsgSoc", 0)) + 5}}, device.device_data.sn)),
        ]

    def selects(self, client: EcoflowApiClient) -> list[BaseSelectEntity]:
        # Note: DC charge current options may need adjustment - document shows 4-8A range
        dc_charge_current_options = {
            "4A": 4,
            "6A": 6,
            "8A": 8
        }
        
        return [
            DictSelectEntity(client, self, "254_21.plugInInfoPvDcAmpMax", const.DC_CHARGE_CURRENT, dc_charge_current_options,
                             lambda value: {"moduleType": 5, "operateType": "dcChgCfg",
                                            "params": {"dcChgCfg": value}}),

            DictSelectEntity(client, self, "254_21.pvChgType", const.DC_MODE, const.DC_MODE_OPTIONS,
                             lambda value: {"moduleType": 5, "operateType": "chaType",
                                            "params": {"chaType": value}}),

            TimeoutDictSelectEntity(client, self, "254_21.screenOffTime", const.SCREEN_TIMEOUT, const.SCREEN_TIMEOUT_OPTIONS,
                                    lambda value: {"moduleType": 5, "operateType": "lcdCfg",
                                                   "params": {"brighLevel": 255, "delayOff": value}}),

            TimeoutDictSelectEntity(client, self, "254_21.devStandbyTime", const.UNIT_TIMEOUT, const.UNIT_TIMEOUT_OPTIONS,
                                    lambda value: {"moduleType": 5, "operateType": "standby",
                                                   "params": {"standbyMins": value}}),

            TimeoutDictSelectEntity(client, self, "254_21.acStandbyTime", const.AC_TIMEOUT, const.AC_TIMEOUT_OPTIONS,
                                    lambda value: {"moduleType": 5, "operateType": "acStandby",
                                                   "params": {"standbyMins": value}})
        ]

    def _status_sensor(self, client: EcoflowApiClient) -> StatusSensorEntity:
        return QuotaStatusSensorEntity(client, self)
