import logging
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
    InWattsSensorEntity, OutWattsSensorEntity, VoltSensorEntity, InMilliampSensorEntity, \
    InVoltSensorEntity, MilliVoltSensorEntity, CapacitySensorEntity, StatusSensorEntity, \
    QuotaStatusSensorEntity, OutVoltSensorEntity
from custom_components.ecoflow_cloud.switch import BeeperEntity, EnabledEntity
from homeassistant.util import dt

from .proto.support.const import Command, CommandFuncAndId, get_expected_payload_type
from .proto.support.message import ProtoMessage

_LOGGER = logging.getLogger(__name__)


def _decode_varint(data: bytes, start_idx: int) -> tuple[int, int]:
    result = 0
    shift = 0
    idx = start_idx
    
    while idx < len(data):
        byte = data[idx]
        result |= (byte & 0x7F) << shift
        idx += 1
        if (byte & 0x80) == 0:
            break
        shift += 7
    
    return result, idx


class River3ChargingStateSensorEntity(BaseSensorEntity):
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:battery-charging"
    _attr_device_class = BinarySensorDeviceClass.BATTERY_CHARGING

    def _update_value(self, val: Any) -> bool:
        if val == 0:
            return super()._update_value("unused")
        elif val == 1:
            return super()._update_value("discharging")
        elif val == 2:
            return super()._update_value("charging")
        else:
            return False


class OutWattsAbsSensorEntity(OutWattsSensorEntity):
    def _update_value(self, val: Any) -> bool:
        return super()._update_value(abs(int(val)))


def _create_river3_proto_command(field_name: str, value: int, device_sn: str, data_len: int = 3):
    from .proto.ecopacket_pb2 import SendHeaderMsg
    from .proto.support.message import ProtoMessage
    import time
    
    packet = SendHeaderMsg()
    message = packet.msg.add()
    
    message.src = 32
    message.dest = 2
    message.d_src = 1
    message.d_dest = 1
    message.cmd_func = 254
    message.cmd_id = 17
    message.need_ack = 1
    message.seq = int(time.time() * 1000) % 2147483647
    message.product_id = 1
    message.version = 19
    message.payload_ver = 1
    message.device_sn = device_sn
    message.data_len = data_len
    
    field_numbers = {
        'en_beep': 9,
        'cfg_dc12v_out_open': 18,
        'xboost_en': 25,
        'cms_max_chg_soc': 33,
        'cms_min_dsg_soc': 34,
        'plug_in_info_ac_in_chg_pow_max': 54,
        'cfg_ac_out_open': 76,
        'plug_in_info_pv_dc_amp_max': 87,
        'pv_chg_type': 90,
        'output_power_off_memory': 141,
    }
    
    if field_name not in field_numbers:
        _LOGGER.error(f"Unknown set_dp3 field: {field_name}")
    
    pdata = bytearray()
    
    if field_name in field_numbers:
        field_num = field_numbers[field_name]
        field_key = (field_num << 3) | 0
        
        while field_key > 0x7F:
            pdata.append((field_key & 0x7F) | 0x80)
            field_key >>= 7
        pdata.append(field_key & 0x7F)
        
        val = int(value)
        if val < 0:
            val = val & 0xFFFFFFFF
        while val > 0x7F:
            pdata.append((val & 0x7F) | 0x80)
            val >>= 7
        pdata.append(val & 0x7F)
    
    message.pdata = bytes(pdata)
    
    class River3CommandMessage(ProtoMessage):
        def __init__(self, packet: SendHeaderMsg):
            super().__init__(command=None, payload=None)
            self._packet = packet
        
        def private_api_to_mqtt_payload(self):
            return self._packet.SerializeToString()
    
    return River3CommandMessage(packet)


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
                if (
                    message.HasField("device_sn")
                    and message.device_sn != self.device_data.sn
                ):
                    continue

                command_desc = CommandFuncAndId(
                    func=message.cmd_func, id=message.cmd_id
                )

                try:
                    command = Command(command_desc)
                except ValueError:
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
                        
                        params.update(
                            (f"{command.func}_{command.id}.{key}", value)
                            for key, value in flattened.items()
                        )
                    except Exception as e:
                        _LOGGER.error(f"Error parsing protobuf payload for {command.name}: {e}", exc_info=True)
                
                elif command_desc.func == 254 and command_desc.id in (17, 18):
                    try:
                        if message.enc_type == 1:
                            message.pdata = bytes([byte ^ (message.seq % 256) for byte in message.pdata])
                        
                        pdata = message.pdata
                        idx = 0
                        parsed_fields = {}
                        
                        while idx < len(pdata):
                            field_key, idx = _decode_varint(pdata, idx)
                            field_num = field_key >> 3
                            wire_type = field_key & 0x7
                            
                            if wire_type == 0:
                                value, idx = _decode_varint(pdata, idx)
                                parsed_fields[field_num] = value
                            elif wire_type == 2:
                                length, idx = _decode_varint(pdata, idx)
                                idx += length
                            else:
                                break
                        
                        field_name_map = {
                            9: 'enBeep',
                            18: 'cfgDc12vOutOpen',
                            25: 'xboostEn',
                            33: 'cmsMaxChgSoc',
                            34: 'cmsMinDsgSoc',
                            54: 'plugInInfoAcInChgPowMax',
                            74: 'dcOutOpen',
                            76: 'cfgAcOutOpen',
                            87: 'plugInInfoPvDcAmpMax',
                            90: 'pvChgType',
                            141: 'outputPowerOffMemory',
                        }
                        
                        if command_desc.id == 18:
                            config_ok = parsed_fields.get(2, 0)
                            if not config_ok:
                                continue
                        
                        for field_num, value in parsed_fields.items():
                            if field_num in field_name_map:
                                state_name = field_name_map[field_num]
                                params[f"254_21.{state_name}"] = value
                    except Exception as e:
                        _LOGGER.error(f"Error parsing set_dp3 payload: {e}", exc_info=True)
                        
                res["timestamp"] = dt.utcnow()
        except Exception as error:
            _LOGGER.error(error)
        return res

    def sensors(self, client: EcoflowApiClient) -> list[BaseSensorEntity]:
        return [
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
                               "254_21.cmsMinDsgSoc", "254_21.cmsMaxChgSoc", 5,
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
                         lambda value: _create_river3_proto_command(
                             "en_beep", 1 if value else 0, device.device_data.sn, data_len=2)),

            EnabledEntity(client, self, "254_21.cfgAcOutOpen", const.AC_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "cfg_ac_out_open", 1 if value else 0, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.xboostEn", const.XBOOST_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "xboost_en", 1 if value else 0, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.dcOutOpen", const.DC_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "cfg_dc12v_out_open", 1 if value else 0, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.outputPowerOffMemory", const.AC_ALWAYS_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "output_power_off_memory", 1 if value else 0, device.device_data.sn)),

            EnabledEntity(client, self, "254_21.energyBackupEn", const.BP_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "energy_backup_en", 1 if value else 0, device.device_data.sn, data_len=7)),
        ]

    def selects(self, client: EcoflowApiClient) -> list[BaseSelectEntity]:
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
