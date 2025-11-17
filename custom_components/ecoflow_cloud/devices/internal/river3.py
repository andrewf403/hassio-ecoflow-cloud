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


def _decode_varint(data: bytes, start_idx: int) -> tuple[int, int]:
    """Decode a protobuf varint from bytes.
    
    Args:
        data: The bytes to decode from
        start_idx: Starting index in the data
        
    Returns:
        Tuple of (decoded_value, next_index)
    """
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


def _create_river3_proto_command(field_name: str, value: int, device_sn: str, data_len: int = 3):
    """Create a River3 protobuf command for set_dp3 message structure.
    
    River3 commands must be sent as protobuf (SendHeaderMsg), not JSON.
    Based on the JavaScript implementation at lines 3245-3360 and protobuf at 3807-3892.
    
    The structure is:
    - SendHeaderMsg with header fields (src, dest, cmd_func=254, cmd_id=17, etc.)
    - pdata contains the set_dp3 protobuf message with the specific field set
    
    Args:
        field_name: The protobuf field name in snake_case (e.g., 'cfg_dc12v_out_open', 'xboost_en')
        value: The integer value (0 or 1 for switches)
        device_sn: The device serial number
        data_len: The data length for the command
    """
    from .proto.ecopacket_pb2 import SendHeaderMsg
    from .proto.support.message import ProtoMessage
    import time
    import struct
    
    # Create SendHeaderMsg packet matching JS structure (lines 3245-3263)
    packet = SendHeaderMsg()
    message = packet.msg.add()
    
    # Set all required header fields from JS implementation
    message.src = 32  # APP
    message.dest = 2  # IOT2
    message.d_src = 1
    message.d_dest = 1
    message.cmd_func = 254
    message.cmd_id = 17
    message.need_ack = 1
    # seq is int32, so we need to keep it within range (max 2^31-1 = 2147483647)
    # Use modulo to wrap the timestamp and keep lower 31 bits
    message.seq = int(time.time() * 1000) % 2147483647
    message.product_id = 1
    message.version = 19
    message.payload_ver = 1
    message.device_sn = device_sn
    message.data_len = data_len
    
    # Encode the pdata as protobuf set_dp3 message
    # Field mapping (from river3.js protoSource lines 3807-3833):
    # - cfg_dc12v_out_open = field 18
    # - cfg_ac_out_open = field 76
    # - xboost_en = field 25
    # - en_beep = field 9
    # - output_power_off_memory = field 141
    # All are int32/uint32 types
    
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
        # Note: cfg_energy_backup is field 43 but is a nested message, handled separately
    }
    
    if field_name not in field_numbers:
        _LOGGER.error(f"Unknown set_dp3 field: {field_name}")
        # Try to continue anyway in case there are other valid fields
    
    # Manually encode the protobuf message for set_dp3
    # Protobuf encoding: field_number << 3 | wire_type
    # wire_type 0 = varint (for int32/uint32)
    pdata = bytearray()
    
    if field_name in field_numbers:
        field_num = field_numbers[field_name]
        # Encode field key: (field_number << 3) | wire_type
        # wire_type = 0 for varint
        field_key = (field_num << 3) | 0
        
        # Encode the field key as varint
        while field_key > 0x7F:
            pdata.append((field_key & 0x7F) | 0x80)
            field_key >>= 7
        pdata.append(field_key & 0x7F)
        
        # Encode the value as varint
        val = int(value)
        if val < 0:
            # Negative int32 uses 10 bytes in protobuf
            val = val & 0xFFFFFFFF  # Convert to unsigned representation
        while val > 0x7F:
            pdata.append((val & 0x7F) | 0x80)
            val >>= 7
        pdata.append(val & 0x7F)
    
    message.pdata = bytes(pdata)
    
    _LOGGER.info(
        f"River3 command: {field_name}={value}, field_num={field_numbers.get(field_name, '?')}, "
        f"pdata_hex={pdata.hex()}, data_len={data_len}"
    )
    
    # Return the packet serialized as bytes via ProtoMessage wrapper
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
                
                # Handle set_dp3 (cmd_func=254, cmd_id=17) and setReply_dp3 (cmd_func=254, cmd_id=18)
                # These contain switch states for AC/DC outputs that aren't in DisplayPropertyUpload
                elif command_desc.func == 254 and command_desc.id in (17, 18):
                    _LOGGER.info(
                        f"River3 received set_dp3 message: cmd_id={command_desc.id}, "
                        f"pdata_len={len(message.pdata)}, pdata_hex={message.pdata.hex()}"
                    )
                    try:
                        if message.enc_type == 1:
                            message.pdata = bytes([byte ^ (message.seq % 256) for byte in message.pdata])
                            _LOGGER.debug(f"River3 decrypted pdata: {message.pdata.hex()}")
                        
                        # Parse the pdata as a generic protobuf message to extract fields
                        # We'll manually decode the protobuf varint fields
                        pdata = message.pdata
                        idx = 0
                        parsed_fields = {}
                        
                        while idx < len(pdata):
                            # Read field key (field_number << 3 | wire_type)
                            field_key, idx = _decode_varint(pdata, idx)
                            field_num = field_key >> 3
                            wire_type = field_key & 0x7
                            
                            if wire_type == 0:  # varint
                                value, idx = _decode_varint(pdata, idx)
                                parsed_fields[field_num] = value
                            elif wire_type == 2:  # length-delimited (nested message or string)
                                length, idx = _decode_varint(pdata, idx)
                                # For now, just skip these bytes (could be cfg_energy_backup)
                                idx += length
                            else:
                                _LOGGER.warning(f"Unknown wire_type {wire_type} for field {field_num}")
                                break
                        
                        # Map field numbers to state names (reverse of the field_numbers dict)
                        # Note: These are set_dp3 field numbers, not DisplayPropertyUpload
                        field_name_map = {
                            9: 'enBeep',
                            18: 'cfgDc12vOutOpen',  # set_dp3 DC output command (not the same as dcOutOpen in DisplayPropertyUpload)
                            25: 'xboostEn',
                            33: 'cmsMaxChgSoc',
                            34: 'cmsMinDsgSoc',
                            54: 'plugInInfoAcInChgPowMax',
                            74: 'dcOutOpen',  # DisplayPropertyUpload also has this field (DC output status)
                            76: 'cfgAcOutOpen',  # set_dp3 AC output command (only in set_dp3, not DisplayPropertyUpload)
                            87: 'plugInInfoPvDcAmpMax',
                            90: 'pvChgType',
                            141: 'outputPowerOffMemory',
                        }
                        
                        # For cmd_id=18 (setReply_dp3), check if configOk field (field 2) is true
                        if command_desc.id == 18:
                            config_ok = parsed_fields.get(2, 0)
                            if not config_ok:
                                _LOGGER.warning(f"River3 set_dp3 command failed (configOk=false)")
                                continue
                        
                        # Store parsed fields in params under the "254_21" prefix (to match DisplayPropertyUpload)
                        # This allows switches to read their state from set_dp3 responses
                        for field_num, value in parsed_fields.items():
                            if field_num in field_name_map:
                                state_name = field_name_map[field_num]
                                params[f"254_21.{state_name}"] = value
                                _LOGGER.info(
                                    f"River3 set_dp3 (cmd_id {command_desc.id}): field_{field_num} ({state_name}) = {value}"
                                )
                            else:
                                _LOGGER.debug(
                                    f"River3 set_dp3 (cmd_id {command_desc.id}): unknown field_{field_num} = {value}"
                                )
                    except Exception as e:
                        _LOGGER.error(f"Error parsing set_dp3 payload: {e}", exc_info=True)
                        
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
            # Not sure if it works correctly, didn't test it yet
            # InVoltSensorEntity(client, self, "254_22.plugInInfoPvVol", const.SOLAR_IN_VOLTAGE),

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
            # CyclesSensorEntity(client, self, "254_21.cycles", const.CYCLES),

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
        # River3 commands MUST be sent as protobuf, not JSON
        # Using _create_river3_proto_command to generate proper SendHeaderMsg packets
        # Note: Field names must be in snake_case as per protobuf definition
        return [
            # Beeper - data_len=2 per JS
            BeeperEntity(client, self, "254_21.enBeep", const.BEEPER,
                         lambda value: _create_river3_proto_command(
                             "en_beep", 1 if value else 0, device.device_data.sn, data_len=2)),

            # AC Output switch - NOTE: Status comes from cfgAcOutOpen in set_dp3/setReply_dp3 messages
            # Unlike DC (which has dcOutOpen in DisplayPropertyUpload), AC status only comes from set_dp3
            # This means status updates only when commands are sent/acknowledged, not in regular status messages
            EnabledEntity(client, self, "254_21.cfgAcOutOpen", const.AC_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "cfg_ac_out_open", 1 if value else 0, device.device_data.sn)),

            # X-Boost switch
            EnabledEntity(client, self, "254_21.xboostEn", const.XBOOST_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "xboost_en", 1 if value else 0, device.device_data.sn)),

            # DC 12V Output switch - NOTE: Status comes from dcOutOpen (field in DisplayPropertyUpload)
            # but command uses cfg_dc12v_out_open (field 18 in set_dp3)
            EnabledEntity(client, self, "254_21.dcOutOpen", const.DC_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "cfg_dc12v_out_open", 1 if value else 0, device.device_data.sn)),

            # AC Always On (output power off memory)
            EnabledEntity(client, self, "254_21.outputPowerOffMemory", const.AC_ALWAYS_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "output_power_off_memory", 1 if value else 0, device.device_data.sn)),

            # Backup Reserve
            EnabledEntity(client, self, "254_21.energyBackupEn", const.BP_ENABLED,
                          lambda value: _create_river3_proto_command(
                              "energy_backup_en", 1 if value else 0, device.device_data.sn, data_len=7)),
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
