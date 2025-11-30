import logging
from typing import Any, override

from homeassistant.components.binary_sensor import BinarySensorDeviceClass  # pyright: ignore[reportMissingImports]
from homeassistant.helpers.entity import EntityCategory  # pyright: ignore[reportMissingImports]

from custom_components.ecoflow_cloud.api import EcoflowApiClient
from custom_components.ecoflow_cloud.devices import BaseDevice, const
from custom_components.ecoflow_cloud.devices.internal.proto import ef_river3_pb2 as pb2
from custom_components.ecoflow_cloud.entities import (
    BaseNumberEntity,
    BaseSelectEntity,
    BaseSensorEntity,
    BaseSwitchEntity,
)
from custom_components.ecoflow_cloud.number import (
    BatteryBackupLevel,
    ChargingPowerEntity,
    MaxBatteryLevelEntity,
    MinBatteryLevelEntity,
)
from custom_components.ecoflow_cloud.select import DictSelectEntity, TimeoutDictSelectEntity
from custom_components.ecoflow_cloud.sensor import (
    CapacitySensorEntity,
    CyclesSensorEntity,
    InEnergySensorEntity,
    InEnergySolarSensorEntity,
    InMilliampSensorEntity,
    InVoltSensorEntity,
    InWattsSensorEntity,
    LevelSensorEntity,
    MilliVoltSensorEntity,
    OutEnergySensorEntity,
    OutWattsSensorEntity,
    QuotaStatusSensorEntity,
    RemainSensorEntity,
    TempSensorEntity,
    VoltSensorEntity,
)
from custom_components.ecoflow_cloud.switch import BeeperEntity, EnabledEntity

_LOGGER = logging.getLogger(__name__)


def _encode_varint(val: int) -> bytes:
    """Encode an integer as a protobuf varint."""
    result = bytearray()
    if val < 0:
        val = val & 0xFFFFFFFF
    while val > 0x7F:
        result.append((val & 0x7F) | 0x80)
        val >>= 7
    result.append(val & 0x7F)
    return bytes(result)


def _create_river3_proto_command(field_name: str, value: int, device_sn: str, data_len: int | None = None):
    """Create a protobuf command for River 3.
    
    River 3 uses direct protobuf commands (not JSON) for all controllable entities.
    This function builds the proper SendHeaderMsg with the field encoded in pdata.
    
    Based on ioBroker JS implementation: all controls use cmdFunc=254, cmdId=17.
    """
    from .proto.ecopacket_pb2 import SendHeaderMsg
    from .proto.support.message import ProtoMessage
    import time
    
    # Field numbers from the proto definition (River3SetCommand) - from JS set_dp3
    # Maps field_name -> (field_number, default_data_len)
    field_config = {
        # Switches
        'en_beep': (9, 2),
        'ac_standby_time': (10, 3),  # Can be 2 if value <= 128
        'dc_standby_time': (11, 3),
        'screen_off_time': (12, 3),  # Can be 2 if value <= 128
        'dev_standby_time': (13, 3), # Can be 2 if value <= 128
        'lcd_light': (14, 2),
        'cfg_dc12v_out_open': (18, 3),
        'xboost_en': (25, 3),
        'cms_max_chg_soc': (33, 3),
        'cms_min_dsg_soc': (34, 3),
        'plug_in_info_ac_in_chg_pow_max': (54, 4),  # Can be 3 if value <= 128
        'cfg_ac_out_open': (76, 3),
        'plug_in_info_pv_dc_amp_max': (87, 3),
        'pv_chg_type': (90, 3),
        'output_power_off_memory': (141, 3),
    }
    
    if field_name not in field_config:
        _LOGGER.error(f"Unknown River3 set field: {field_name}")
        return None
    
    field_num, default_data_len = field_config[field_name]
    
    # Dynamic data_len calculation based on JS implementation
    val = int(value)
    if data_len is None:
        # Follow JS logic for variable-length fields
        if field_name == 'plug_in_info_ac_in_chg_pow_max':
            data_len = 4 if val > 128 else 3
        elif field_name in ('ac_standby_time', 'dev_standby_time', 'screen_off_time'):
            data_len = 3 if val > 128 else 2
        else:
            data_len = default_data_len
    
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
    
    # Build pdata: field key (varint) + value (varint)
    pdata = bytearray()
    
    field_key = (field_num << 3) | 0  # wire type 0 = varint
    pdata.extend(_encode_varint(field_key))
    pdata.extend(_encode_varint(val))
    
    message.pdata = bytes(pdata)
    
    class River3CommandMessage(ProtoMessage):
        def __init__(self, packet: SendHeaderMsg):
            super().__init__(command=None, payload=None)
            self._packet = packet
        
        def private_api_to_mqtt_payload(self):
            return self._packet.SerializeToString()
    
    return River3CommandMessage(packet)


def _create_river3_energy_backup_command(
    energy_backup_en: int | None, 
    energy_backup_start_soc: int,
    device_sn: str
):
    """Create a protobuf command for River 3 energy backup settings.
    
    Energy backup uses a nested cfgEnergyBackup message (field 43) containing:
    - energy_backup_en (field 1): enable/disable backup
    - energy_backup_start_soc (field 2): SOC threshold
    
    Based on JS implementation:
    - When enabling: send both fields, data_len=7
    - When disabling: send only energy_backup_start_soc (no enable field), data_len=5
    """
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
    
    # Build nested cfgEnergyBackup message (field 43, wire type 2 = length-delimited)
    inner_pdata = bytearray()
    
    if energy_backup_en is not None and energy_backup_en == 1:
        # Enable: include both fields
        # energy_backup_en = 1 (field 1)
        inner_pdata.extend(_encode_varint((1 << 3) | 0))  # field 1, wire type 0
        inner_pdata.extend(_encode_varint(1))
        # energy_backup_start_soc (field 2)
        inner_pdata.extend(_encode_varint((2 << 3) | 0))  # field 2, wire type 0
        inner_pdata.extend(_encode_varint(int(energy_backup_start_soc)))
        message.data_len = 7
    else:
        # Disable: only send energy_backup_start_soc (no enable field means disabled)
        # energy_backup_start_soc (field 2)
        inner_pdata.extend(_encode_varint((2 << 3) | 0))  # field 2, wire type 0
        inner_pdata.extend(_encode_varint(int(energy_backup_start_soc)))
        message.data_len = 5
    
    # Wrap in cfgEnergyBackup (field 43, wire type 2)
    pdata = bytearray()
    pdata.extend(_encode_varint((43 << 3) | 2))  # field 43, wire type 2 (length-delimited)
    pdata.extend(_encode_varint(len(inner_pdata)))  # length prefix
    pdata.extend(inner_pdata)
    
    message.pdata = bytes(pdata)
    
    class River3CommandMessage(ProtoMessage):
        def __init__(self, packet: SendHeaderMsg):
            super().__init__(command=None, payload=None)
            self._packet = packet
        
        def private_api_to_mqtt_payload(self):
            return self._packet.SerializeToString()
    
    return River3CommandMessage(packet)


# Message type mapping for BMS heartbeat related reports
# These (cmdFunc, cmdId) pairs are known to map to BMSHeartBeatReport
BMS_HEARTBEAT_COMMANDS: set[tuple[int, int]] = {
    (3, 1),
    (3, 2),
    (3, 30),
    (3, 50),
    (32, 1),
    (32, 3),
    (32, 50),
    (32, 51),
    (32, 52),
    (254, 24),
    (254, 25),
    (254, 26),
    (254, 27),
    (254, 28),
    (254, 29),
    (254, 30),
}


class River3ChargingStateSensorEntity(BaseSensorEntity):
    """Sensor for battery charging state."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:battery-charging"
    _attr_device_class = BinarySensorDeviceClass.BATTERY_CHARGING

    def _update_value(self, val: Any) -> bool:
        if val == 0:
            return super()._update_value("idle")
        elif val == 1:
            return super()._update_value("discharging")
        elif val == 2:
            return super()._update_value("charging")
        else:
            return False


class OutWattsAbsSensorEntity(OutWattsSensorEntity):
    """Output power sensor that uses absolute value."""

    def _update_value(self, val: Any) -> bool:
        return super()._update_value(abs(int(val)))


class River3(BaseDevice):
    """EcoFlow River 3 device implementation using protobuf decoding.

    River 3 is a portable power station from the same generation as Delta Pro 3,
    sharing similar protobuf message structures for data communication.

    Message Types:
    - cmdFunc=254, cmdId=21: DisplayPropertyUpload (main status/settings)
    - cmdFunc=254, cmdId=22: RuntimePropertyUpload (runtime sensor data)
    - cmdFunc=254, cmdId=17: Set commands
    - cmdFunc=254, cmdId=18: Set reply confirmation
    """

    @staticmethod
    def default_charging_power_step() -> int:
        return 50

    @override
    def sensors(self, client: EcoflowApiClient) -> list[BaseSensorEntity]:
        return [
            # Main Battery System
            LevelSensorEntity(client, self, "bms_batt_soc", const.MAIN_BATTERY_LEVEL)
            .attr("bms_design_cap", const.ATTR_DESIGN_CAPACITY, 0)
            .attr("bms_full_cap", const.ATTR_FULL_CAPACITY, 0)
            .attr("bms_remain_cap", const.ATTR_REMAIN_CAPACITY, 0),
            CapacitySensorEntity(client, self, "bms_design_cap", const.MAIN_DESIGN_CAPACITY, False),
            CapacitySensorEntity(client, self, "bms_full_cap", const.MAIN_FULL_CAPACITY, False),
            CapacitySensorEntity(client, self, "bms_remain_cap", const.MAIN_REMAIN_CAPACITY, False),
            LevelSensorEntity(client, self, "bms_batt_soh", const.SOH),
            # Combined Battery Level (for expansion batteries)
            LevelSensorEntity(client, self, "cms_batt_soc", const.COMBINED_BATTERY_LEVEL),
            # Charging State
            River3ChargingStateSensorEntity(client, self, "bms_chg_dsg_state", const.BATTERY_CHARGING_STATE),
            # Power Input/Output
            InWattsSensorEntity(client, self, "pow_in_sum_w", const.TOTAL_IN_POWER).with_energy(),
            OutWattsSensorEntity(client, self, "pow_out_sum_w", const.TOTAL_OUT_POWER).with_energy(),
            # Solar Input - River 3 has single PV input
            InWattsSensorEntity(client, self, "pow_get_pv", const.SOLAR_IN_POWER),
            InMilliampSensorEntity(client, self, "plug_in_info_pv_amp", const.SOLAR_IN_CURRENT),
            # AC Input/Output
            InWattsSensorEntity(client, self, "pow_get_ac_in", const.AC_IN_POWER),
            OutWattsAbsSensorEntity(client, self, "pow_get_ac_out", const.AC_OUT_POWER),
            InVoltSensorEntity(client, self, "plug_in_info_ac_in_vol", const.AC_IN_VOLT),
            # Note: AC output voltage sensor may not be available in proto
            # DC Output
            OutWattsSensorEntity(client, self, "pow_get_12v", const.DC_OUT_POWER),
            # USB Output
            OutWattsAbsSensorEntity(client, self, "pow_get_typec1", const.TYPEC_1_OUT_POWER),
            OutWattsAbsSensorEntity(client, self, "pow_get_qcusb1", const.USB_QC_1_OUT_POWER),
            OutWattsAbsSensorEntity(client, self, "pow_get_qcusb2", const.USB_QC_2_OUT_POWER),
            # Remaining Time
            RemainSensorEntity(client, self, "bms_chg_rem_time", const.CHARGE_REMAINING_TIME),
            RemainSensorEntity(client, self, "bms_dsg_rem_time", const.DISCHARGE_REMAINING_TIME),
            RemainSensorEntity(client, self, "cms_chg_rem_time", const.REMAINING_TIME),
            # Temperature
            TempSensorEntity(client, self, "temp_pcs_dc", "PCS DC Temperature"),
            TempSensorEntity(client, self, "temp_pcs_ac", "PCS AC Temperature"),
            TempSensorEntity(client, self, "bms_min_cell_temp", const.BATTERY_TEMP)
            .attr("bms_max_cell_temp", const.ATTR_MAX_CELL_TEMP, 0),
            TempSensorEntity(client, self, "bms_max_cell_temp", const.MAX_CELL_TEMP, False),
            # Battery Voltage
            VoltSensorEntity(client, self, "bms_batt_vol", const.BATTERY_VOLT, False)
            .attr("bms_min_cell_vol", const.ATTR_MIN_CELL_VOLT, 0)
            .attr("bms_max_cell_vol", const.ATTR_MAX_CELL_VOLT, 0),
            MilliVoltSensorEntity(client, self, "bms_min_cell_vol", const.MIN_CELL_VOLT, False),
            MilliVoltSensorEntity(client, self, "bms_max_cell_vol", const.MAX_CELL_VOLT, False),
            # Battery Cycles (from BMSHeartBeatReport)
            CyclesSensorEntity(client, self, "cycles", const.CYCLES),
            # Statistics Energy (from display_statistics_sum - Wh)
            # These are cumulative energy counters for specific ports/inputs
            OutEnergySensorEntity(client, self, "ac_out_energy", "AC Output Energy"),
            InEnergySensorEntity(client, self, "ac_in_energy", "AC Input Energy"),
            InEnergySolarSensorEntity(client, self, "pv_in_energy", const.SOLAR_IN_ENERGY),
            OutEnergySensorEntity(client, self, "dc12v_out_energy", "DC 12V Output Energy", False),
            OutEnergySensorEntity(client, self, "typec_out_energy", "Type-C Output Energy", False),
            OutEnergySensorEntity(client, self, "usba_out_energy", "USB-A Output Energy", False),
            # Status
            QuotaStatusSensorEntity(client, self),
        ]

    @override
    def numbers(self, client: EcoflowApiClient) -> list[BaseNumberEntity]:
        device = self
        return [
            # Max charge SOC - protobuf field cms_max_chg_soc (field 33)
            MaxBatteryLevelEntity(
                client,
                self,
                "cms_max_chg_soc",
                const.MAX_CHARGE_LEVEL,
                50,
                100,
                lambda value: _create_river3_proto_command(
                    "cms_max_chg_soc", int(value), device.device_data.sn
                ),
            ),
            # Min discharge SOC - protobuf field cms_min_dsg_soc (field 34)
            MinBatteryLevelEntity(
                client,
                self,
                "cms_min_dsg_soc",
                const.MIN_DISCHARGE_LEVEL,
                0,
                30,
                lambda value: _create_river3_proto_command(
                    "cms_min_dsg_soc", int(value), device.device_data.sn
                ),
            ),
            # AC charging power - protobuf field plug_in_info_ac_in_chg_pow_max (field 54)
            ChargingPowerEntity(
                client,
                self,
                "plug_in_info_ac_in_chg_pow_max",
                const.AC_CHARGING_POWER,
                50,
                305,
                lambda value: _create_river3_proto_command(
                    "plug_in_info_ac_in_chg_pow_max", int(value), device.device_data.sn
                ),
            ),
            # Battery backup level - uses nested cfgEnergyBackup (field 43)
            BatteryBackupLevel(
                client,
                self,
                "energy_backup_start_soc",
                const.BACKUP_RESERVE_LEVEL,
                5,
                100,
                "cms_min_dsg_soc",
                "cms_max_chg_soc",
                5,
                lambda value: _create_river3_energy_backup_command(
                    1, int(value), device.device_data.sn
                ),
            ),
        ]

    @override
    def switches(self, client: EcoflowApiClient) -> list[BaseSwitchEntity]:
        device = self
        return [
            # Beeper control - using protobuf field en_beep (field 9)
            BeeperEntity(
                client,
                self,
                "en_beep",
                const.BEEPER,
                lambda value: _create_river3_proto_command(
                    "en_beep", 1 if value else 0, device.device_data.sn, data_len=2
                ),
            ),
            # AC Output - using protobuf field cfg_ac_out_open (field 76)
            EnabledEntity(
                client,
                self,
                "cfg_ac_out_open",
                const.AC_ENABLED,
                lambda value, params=None: _create_river3_proto_command(
                    "cfg_ac_out_open", 1 if value else 0, device.device_data.sn
                ),
            ),
            # X-Boost - using protobuf field xboost_en (field 25)
            EnabledEntity(
                client,
                self,
                "xboost_en",
                const.XBOOST_ENABLED,
                lambda value, params=None: _create_river3_proto_command(
                    "xboost_en", 1 if value else 0, device.device_data.sn
                ),
            ),
            # DC 12V Output - using protobuf field cfg_dc12v_out_open (field 18)
            EnabledEntity(
                client,
                self,
                "dc_out_open",
                const.DC_ENABLED,
                lambda value, params=None: _create_river3_proto_command(
                    "cfg_dc12v_out_open", 1 if value else 0, device.device_data.sn
                ),
            ),
            # AC Always On - using protobuf field output_power_off_memory (field 147)
            EnabledEntity(
                client,
                self,
                "output_power_off_memory",
                const.AC_ALWAYS_ENABLED,
                lambda value, params=None: _create_river3_proto_command(
                    "output_power_off_memory", 1 if value else 0, device.device_data.sn
                ),
            ),
            # Backup Reserve - uses nested cfgEnergyBackup (field 43)
            # When enabling, needs to include energy_backup_start_soc from current state
            EnabledEntity(
                client,
                self,
                "energy_backup_en",
                const.BP_ENABLED,
                lambda value, params=None: _create_river3_energy_backup_command(
                    1 if value else None,  # None means disable (don't send enable field)
                    params.get("energy_backup_start_soc", 5) if params else 5,
                    device.device_data.sn
                ),
            ),
        ]

    @override
    def selects(self, client: EcoflowApiClient) -> list[BaseSelectEntity]:
        device = self
        dc_charge_current_options = {"4A": 4, "6A": 6, "8A": 8}

        return [
            # DC charge current - protobuf field plug_in_info_pv_dc_amp_max (field 87)
            DictSelectEntity(
                client,
                self,
                "plug_in_info_pv_dc_amp_max",
                const.DC_CHARGE_CURRENT,
                dc_charge_current_options,
                lambda value: _create_river3_proto_command(
                    "plug_in_info_pv_dc_amp_max", int(value), device.device_data.sn
                ),
            ),
            # DC charging mode - protobuf field pv_chg_type (field 90)
            DictSelectEntity(
                client,
                self,
                "pv_chg_type",
                const.DC_MODE,
                const.DC_MODE_OPTIONS,
                lambda value: _create_river3_proto_command(
                    "pv_chg_type", int(value), device.device_data.sn
                ),
            ),
            # Screen timeout - protobuf field screen_off_time (field 12)
            TimeoutDictSelectEntity(
                client,
                self,
                "screen_off_time",
                const.SCREEN_TIMEOUT,
                const.SCREEN_TIMEOUT_OPTIONS,
                lambda value: _create_river3_proto_command(
                    "screen_off_time", int(value), device.device_data.sn
                ),
            ),
            # Unit timeout - protobuf field dev_standby_time (field 13)
            TimeoutDictSelectEntity(
                client,
                self,
                "dev_standby_time",
                const.UNIT_TIMEOUT,
                const.UNIT_TIMEOUT_OPTIONS,
                lambda value: _create_river3_proto_command(
                    "dev_standby_time", int(value), device.device_data.sn
                ),
            ),
            # AC timeout - protobuf field ac_standby_time (field 10)
            TimeoutDictSelectEntity(
                client,
                self,
                "ac_standby_time",
                const.AC_TIMEOUT,
                const.AC_TIMEOUT_OPTIONS,
                lambda value: _create_river3_proto_command(
                    "ac_standby_time", int(value), device.device_data.sn
                ),
            ),
        ]

    @override
    def _prepare_data(self, raw_data: bytes) -> dict[str, Any]:
        """Prepare River 3 data by decoding protobuf and flattening fields.

        Uses the same message structure as Delta Pro 3:
        - HeaderMessage wrapper for all messages
        - DisplayPropertyUpload for status/settings (cmdFunc=254, cmdId=21)
        - RuntimePropertyUpload for runtime data (cmdFunc=254, cmdId=22)
        """
        _LOGGER.debug(f"[River3] _prepare_data called with {len(raw_data)} bytes")

        flat_dict: dict[str, Any] | None = None
        decoded_data: dict[str, Any] | None = None
        try:
            _LOGGER.debug(f"Processing {len(raw_data)} bytes of raw data")

            # 1. Decode HeaderMessage
            header_info = self._decode_header_message(raw_data)
            if not header_info:
                _LOGGER.debug("HeaderMessage decoding failed, trying JSON fallback")
                return super()._prepare_data(raw_data)

            # 2. Extract payload data
            pdata = self._extract_payload_data(header_info.get("header_obj"))
            if not pdata:
                _LOGGER.debug("No payload data found in header")
                return {}

            # 3. XOR decode (if needed)
            decoded_pdata = self._perform_xor_decode(pdata, header_info)

            # 4. Protobuf message decode
            decoded_data = self._decode_message_by_type(decoded_pdata, header_info)
            if not decoded_data:
                # Empty result is normal for some message types (set commands, unknown types)
                # Only log at debug level since we already log specifics in _decode_message_by_type
                cmd_func = header_info.get("cmdFunc", 0)
                cmd_id = header_info.get("cmdId", 0)
                _LOGGER.debug(f"No data extracted from message cmdFunc={cmd_func}, cmdId={cmd_id}")
                return {}

            # 5. Flatten all fields for params
            flat_dict = self._flatten_dict(decoded_data)
            _LOGGER.debug(f"Flat dict for params (all fields): {flat_dict}")  # noqa: G004
        except Exception as e:
            _LOGGER.debug(f"[River3] Data processing failed: {e}")
            # Fallback to quiet JSON processing
            return self._quiet_json_parse(raw_data)

        # Home Assistant expects a dict with 'params' on success
        _LOGGER.debug(f"[River3] Successfully processed protobuf data, returning {len(flat_dict or {})} fields")
        return {
            "params": flat_dict or {},
            "all_fields": decoded_data or {},
        }

    def _decode_header_message(self, raw_data: bytes) -> dict[str, Any] | None:
        """Decode HeaderMessage and extract header info."""
        try:
            # Try Base64 decode first
            import base64

            try:
                decoded_payload = base64.b64decode(raw_data, validate=True)
                _LOGGER.debug("Base64 decode successful")
                raw_data = decoded_payload
            except Exception:
                _LOGGER.debug("Data is not Base64 encoded, using as-is")

            # Try to decode as HeaderMessage
            try:
                header_msg = pb2.River3HeaderMessage()
                header_msg.ParseFromString(raw_data)
            except AttributeError as e:
                _LOGGER.debug(f"River3HeaderMessage class not found in pb2 module: {e}")
                return None
            except Exception as e:
                _LOGGER.debug(f"Failed to parse River3HeaderMessage: {e}")
                return None

            if not header_msg.header:
                _LOGGER.debug("No headers found in HeaderMessage")
                return None

            # Use the first header (usually single)
            header = header_msg.header[0]
            header_info = {
                "src": getattr(header, "src", 0),
                "dest": getattr(header, "dest", 0),
                "dSrc": getattr(header, "d_src", 0),
                "dDest": getattr(header, "d_dest", 0),
                "encType": getattr(header, "enc_type", 0),
                "checkType": getattr(header, "check_type", 0),
                "cmdFunc": getattr(header, "cmd_func", 0),
                "cmdId": getattr(header, "cmd_id", 0),
                "dataLen": getattr(header, "data_len", 0),
                "needAck": getattr(header, "need_ack", 0),
                "seq": getattr(header, "seq", 0),
                "productId": getattr(header, "product_id", 0),
                "version": getattr(header, "version", 0),
                "payloadVer": getattr(header, "payload_ver", 0),
                "header_obj": header,
            }

            _LOGGER.debug(f"Header decoded: cmdFunc={header_info['cmdFunc']}, cmdId={header_info['cmdId']}")
            return header_info

        except Exception as e:
            _LOGGER.debug(f"HeaderMessage decode failed: {e}")
            return None

    def _extract_payload_data(self, header_obj: Any) -> bytes | None:
        """Extract payload bytes from header."""
        try:
            pdata = getattr(header_obj, "pdata", b"")
            if pdata:
                _LOGGER.debug(f"Extracted {len(pdata)} bytes of payload data")
                return pdata
            else:
                _LOGGER.debug("No pdata found in header")
                return None
        except Exception as e:
            _LOGGER.debug(f"Payload extraction error: {e}")
            return None

    def _perform_xor_decode(self, pdata: bytes, header_info: dict[str, Any]) -> bytes:
        """Perform XOR decoding if required by header info."""
        enc_type = header_info.get("encType", 0)
        src = header_info.get("src", 0)
        seq = header_info.get("seq", 0)

        # XOR decode condition: enc_type == 1 and src != 32
        if enc_type == 1 and src != 32:
            return self._xor_decode_pdata(pdata, seq)
        else:
            return pdata

    def _xor_decode_pdata(self, pdata: bytes, seq: int) -> bytes:
        """Apply XOR over payload with sequence value."""
        if not pdata:
            return b""

        decoded_payload = bytearray()
        for byte_val in pdata:
            decoded_payload.append((byte_val ^ seq) & 0xFF)

        return bytes(decoded_payload)

    def _decode_message_by_type(self, pdata: bytes, header_info: dict[str, Any]) -> dict[str, Any]:
        """Decode protobuf message based on cmdFunc/cmdId.

        River 3 uses the same message types as Delta Pro 3:
        - cmdFunc=254, cmdId=21: DisplayPropertyUpload
        - cmdFunc=254, cmdId=22: RuntimePropertyUpload
        - cmdFunc=254, cmdId=17: Set command
        - cmdFunc=254, cmdId=18: Set reply
        """
        cmd_func = header_info.get("cmdFunc", 0)
        cmd_id = header_info.get("cmdId", 0)

        try:
            _LOGGER.debug(f"Decoding message: cmdFunc={cmd_func}, cmdId={cmd_id}, size={len(pdata)} bytes")

            if cmd_func == 254 and cmd_id == 21:
                # DisplayPropertyUpload - main status and settings
                msg = pb2.River3DisplayPropertyUpload()
                msg.ParseFromString(pdata)
                result = self._protobuf_to_dict(msg)
                # Extract statistics (energy data) from display_statistics_sum
                result = self._extract_statistics(result)
                return result

            elif cmd_func == 254 and cmd_id == 22:
                # RuntimePropertyUpload - runtime sensor data
                msg = pb2.River3RuntimePropertyUpload()
                msg.ParseFromString(pdata)
                return self._protobuf_to_dict(msg)

            elif cmd_func == 254 and cmd_id == 17:
                # Set command (from app/HA to device)
                try:
                    msg = pb2.River3SetCommand()
                    msg.ParseFromString(pdata)
                    return self._protobuf_to_dict(msg)
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as set_dp3: {e}")
                    return {}

            elif cmd_func == 254 and cmd_id == 18:
                # Set reply (confirmation from device)
                try:
                    msg = pb2.River3SetReply()
                    msg.ParseFromString(pdata)
                    result = self._protobuf_to_dict(msg)
                    # Only process if config was successful
                    if result.get("config_ok", False):
                        return result
                    else:
                        _LOGGER.debug(f"Set reply indicates config not OK: {result}")
                        return {}
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as setReply_dp3: {e}")
                    return {}

            elif cmd_func == 32 and cmd_id == 2:
                # cmdFunc32_cmdId2_Report (CMS = Combined Management System)
                try:
                    msg = pb2.River3CMSHeartBeatReport()
                    msg.ParseFromString(pdata)
                    return self._protobuf_to_dict(msg)
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as cmdFunc32_cmdId2_Report: {e}")
                    return {}

            elif self._is_bms_heartbeat(cmd_func, cmd_id):
                # BMSHeartBeatReport - Battery heartbeat with cycles and energy data
                try:
                    msg = pb2.River3BMSHeartBeatReport()
                    msg.ParseFromString(pdata)
                    _LOGGER.debug(f"Successfully decoded BMSHeartBeatReport: cmdFunc={cmd_func}, cmdId={cmd_id}")
                    return self._protobuf_to_dict(msg)
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as BMSHeartBeatReport (cmdFunc={cmd_func}, cmdId={cmd_id}): {e}")
                    return {}

            # Unknown message type - try BMSHeartBeatReport as fallback
            _LOGGER.debug(f"Unknown message type: cmdFunc={cmd_func}, cmdId={cmd_id}, size={len(pdata)} bytes")

            # Try to decode as BMSHeartBeatReport since that's a common case
            try:
                msg = pb2.River3BMSHeartBeatReport()
                msg.ParseFromString(pdata)
                result = self._protobuf_to_dict(msg)
                # Check if we got meaningful data (cycles or energy fields)
                if "cycles" in result or "accu_chg_energy" in result or "accu_dsg_energy" in result:
                    _LOGGER.info(
                        f"Found BMSHeartBeatReport at unexpected cmdFunc={cmd_func}, cmdId={cmd_id}. "
                        f"Consider updating BMS_HEARTBEAT_COMMANDS mapping."
                    )
                    return result
            except Exception as e:
                _LOGGER.debug(f"Failed fallback BMSHeartBeatReport decode: {e}")

            return {}

        except Exception as e:
            _LOGGER.debug(f"Message decode error for cmdFunc={cmd_func}, cmdId={cmd_id}: {e}")
            return {}

    def _is_bms_heartbeat(self, cmd_func: int, cmd_id: int) -> bool:
        """Return True if the pair maps to a BMSHeartBeatReport message."""
        return (cmd_func, cmd_id) in BMS_HEARTBEAT_COMMANDS

    def _flatten_dict(self, d: dict, parent_key: str = "", sep: str = "_") -> dict:
        """Flatten nested dict with underscore separator."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _protobuf_to_dict(self, protobuf_obj: Any) -> dict[str, Any]:
        """Convert protobuf message to dictionary."""
        try:
            from google.protobuf.json_format import MessageToDict

            result = MessageToDict(protobuf_obj, preserving_proto_field_name=True)
            _LOGGER.debug(f"MessageToDict result fields: {len(result)}")
            return result
        except ImportError:
            result = self._manual_protobuf_to_dict(protobuf_obj)
            _LOGGER.debug(f"Manual conversion result fields: {len(result)}")
            return result

    def _extract_statistics(self, data: dict[str, Any]) -> dict[str, Any]:
        """Extract statistics from display_statistics_sum into flat fields.
        
        The statistics are sent as a list of {statistics_object: enum, statistics_content: value}.
        Field names are derived directly from the proto enum (e.g., STATISTICS_OBJECT_AC_OUT_ENERGY -> ac_out_energy).
        
        Energy values are in Wh.
        """
        stats_sum = data.get("display_statistics_sum", {})
        list_info = stats_sum.get("list_info", [])
        
        if not list_info:
            return data
        
        for item in list_info:
            # Handle both snake_case (from proto) and camelCase (from MessageToDict)
            stat_obj = item.get("statistics_object") or item.get("statisticsObject")
            stat_content = item.get("statistics_content") or item.get("statisticsContent")
            
            if stat_obj is not None and stat_content is not None:
                # Derive field name from enum string: STATISTICS_OBJECT_AC_OUT_ENERGY -> ac_out_energy
                if isinstance(stat_obj, str) and stat_obj.startswith("STATISTICS_OBJECT_"):
                    field_name = stat_obj.replace("STATISTICS_OBJECT_", "").lower()
                    data[field_name] = stat_content
                    _LOGGER.debug(f"Extracted statistic: {field_name} = {stat_content}")
                elif isinstance(stat_obj, int):
                    # If it's an integer, we need to look up the enum name from the proto
                    try:
                        enum_name = pb2.River3StatisticsObject.Name(stat_obj)
                        if enum_name.startswith("STATISTICS_OBJECT_"):
                            field_name = enum_name.replace("STATISTICS_OBJECT_", "").lower()
                            data[field_name] = stat_content
                            _LOGGER.debug(f"Extracted statistic: {field_name} = {stat_content}")
                    except ValueError:
                        _LOGGER.debug(f"Unknown statistics_object value: {stat_obj}")
        
        return data

    def _manual_protobuf_to_dict(self, protobuf_obj: Any) -> dict[str, Any]:
        """Convert protobuf object to dict manually (fallback)."""
        result = {}
        for field, value in protobuf_obj.ListFields():
            if field.label == field.LABEL_REPEATED:
                result[field.name] = list(value)
            elif hasattr(value, "ListFields"):  # nested message
                result[field.name] = self._manual_protobuf_to_dict(value)
            else:
                result[field.name] = value
        return result

    @override
    def update_data(self, raw_data, data_type: str) -> bool:
        """Decode protobuf for data_topic; silently handle other topics."""
        if data_type == self.device_info.data_topic:
            # Device status updates come as protobuf
            raw = self._prepare_data(raw_data)
            self.data.update_data(raw)
        elif data_type == self.device_info.set_topic:
            # Commands we send - silently ignore echoes
            pass
        elif data_type == self.device_info.set_reply_topic:
            # Device replies to commands - try protobuf
            # Also update entity data since AC switch status comes from setReply
            raw = self._prepare_set_reply_data(raw_data)
            if raw:
                self.data.update_data(raw)  # Update entities with reply data
            self.data.add_set_reply_message(raw)
        elif data_type == self.device_info.get_topic:
            # Get commands we send - silently ignore
            pass
        elif data_type == self.device_info.get_reply_topic:
            # Get replies - try protobuf, also update entity data
            raw = self._prepare_set_reply_data(raw_data)
            if raw:
                self.data.update_data(raw)  # Update entities with reply data
            self.data.add_get_reply_message(raw)
        else:
            return False
        return True

    def _prepare_set_reply_data(self, raw_data: bytes) -> dict[str, Any]:
        """Parse set/get reply data - try protobuf, fall back to quiet JSON."""
        try:
            # Try to decode as protobuf HeaderMessage first
            import base64
            try:
                decoded_payload = base64.b64decode(raw_data, validate=True)
                raw_data = decoded_payload
            except Exception:
                pass

            header_msg = pb2.River3HeaderMessage()
            header_msg.ParseFromString(raw_data)

            if header_msg.header:
                header = header_msg.header[0]
                pdata = getattr(header, "pdata", b"")
                if pdata:
                    # Try to decode as SetReply
                    try:
                        enc_type = getattr(header, "enc_type", 0)
                        src = getattr(header, "src", 0)
                        seq = getattr(header, "seq", 0)
                        if enc_type == 1 and src != 32:
                            pdata = self._xor_decode_pdata(pdata, seq)

                        reply_msg = pb2.River3SetReply()
                        reply_msg.ParseFromString(pdata)
                        result = self._protobuf_to_dict(reply_msg)
                        if result.get("config_ok"):
                            _LOGGER.debug(f"Set reply successful: {result}")
                        return {"params": self._flatten_dict(result)}
                    except Exception as e:
                        _LOGGER.debug(f"Failed to parse as River3SetReply: {e}")
        except Exception as e:
            _LOGGER.debug(f"Protobuf parse failed for set_reply: {e}")

        # Fall back to quiet JSON parsing (no error logs)
        return self._quiet_json_parse(raw_data)

    def _quiet_json_parse(self, raw_data: bytes) -> dict[str, Any]:
        """Parse JSON data without logging errors."""
        import json
        try:
            payload = raw_data.decode("utf-8", errors="ignore")
            return json.loads(payload)
        except Exception:
            return {}
