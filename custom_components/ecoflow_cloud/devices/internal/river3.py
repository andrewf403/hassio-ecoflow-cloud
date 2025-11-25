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
    InMilliampSensorEntity,
    InVoltSensorEntity,
    InWattsSensorEntity,
    LevelSensorEntity,
    MilliVoltSensorEntity,
    OutWattsSensorEntity,
    QuotaStatusSensorEntity,
    RemainSensorEntity,
    TempSensorEntity,
    VoltSensorEntity,
)
from custom_components.ecoflow_cloud.switch import BeeperEntity, EnabledEntity

_LOGGER = logging.getLogger(__name__)

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
            OutWattsAbsSensorEntity(client, self, "pow_get_ac", const.AC_OUT_POWER),
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
            # Status
            QuotaStatusSensorEntity(client, self),
        ]

    @override
    def numbers(self, client: EcoflowApiClient) -> list[BaseNumberEntity]:
        return [
            MaxBatteryLevelEntity(
                client,
                self,
                "cms_max_chg_soc",
                const.MAX_CHARGE_LEVEL,
                50,
                100,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 33, "cmsMaxChgSoc": int(value)},
                },
            ),
            MinBatteryLevelEntity(
                client,
                self,
                "cms_min_dsg_soc",
                const.MIN_DISCHARGE_LEVEL,
                0,
                30,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 34, "cmsMinDsgSoc": int(value)},
                },
            ),
            ChargingPowerEntity(
                client,
                self,
                "plug_in_info_ac_in_chg_pow_max",
                const.AC_CHARGING_POWER,
                50,
                305,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 54, "plugInInfoAcInChgPowMax": int(value)},
                },
            ),
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
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {
                        "id": 43,
                        "cfgEnergyBackup": {
                            "energyBackupEn": 1,
                            "energyBackupStartSoc": int(value),
                        },
                    },
                },
            ),
        ]

    @override
    def switches(self, client: EcoflowApiClient) -> list[BaseSwitchEntity]:
        return [
            # Beeper control - en_beep field 9 in SetCommand, field 195 in Display
            BeeperEntity(
                client,
                self,
                "en_beep",
                const.BEEPER,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 9, "enBeep": value},
                },
            ),
            # AC Output - cfg_ac_out_open field 76 in SetCommand
            EnabledEntity(
                client,
                self,
                "cfg_ac_out_open",
                const.AC_ENABLED,
                lambda value, params=None: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 76, "cfgAcOutOpen": value},
                },
            ),
            # X-Boost - xboost_en field 25 in SetCommand
            EnabledEntity(
                client,
                self,
                "xboost_en",
                const.XBOOST_ENABLED,
                lambda value, params=None: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 25, "xboostEn": value},
                },
            ),
            # DC 12V Output - cfg_dc12v_out_open field 18 in SetCommand
            EnabledEntity(
                client,
                self,
                "dc_out_open",
                const.DC_ENABLED,
                lambda value, params=None: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 18, "cfgDc12vOutOpen": value},
                },
            ),
            # AC Always On - output_power_off_memory field 147 in SetCommand
            EnabledEntity(
                client,
                self,
                "output_power_off_memory",
                const.AC_ALWAYS_ENABLED,
                lambda value, params=None: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 147, "outputPowerOffMemory": value},
                },
            ),
            # Backup Reserve - cfg_energy_backup field 43 in SetCommand
            EnabledEntity(
                client,
                self,
                "energy_backup_en",
                const.BP_ENABLED,
                lambda value, params=None: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 43, "cfgEnergyBackup": {"energyBackupEn": 1 if value else 0}},
                },
            ),
        ]

    @override
    def selects(self, client: EcoflowApiClient) -> list[BaseSelectEntity]:
        dc_charge_current_options = {"4A": 4, "6A": 6, "8A": 8}

        return [
            DictSelectEntity(
                client,
                self,
                "plug_in_info_pv_dc_amp_max",
                const.DC_CHARGE_CURRENT,
                dc_charge_current_options,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 87, "plugInInfoPvDcAmpMax": value},
                },
            ),
            DictSelectEntity(
                client,
                self,
                "pv_chg_type",
                const.DC_MODE,
                const.DC_MODE_OPTIONS,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 90, "pvChgType": value},
                },
            ),
            TimeoutDictSelectEntity(
                client,
                self,
                "screen_off_time",
                const.SCREEN_TIMEOUT,
                const.SCREEN_TIMEOUT_OPTIONS,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 12, "screenOffTime": value},
                },
            ),
            TimeoutDictSelectEntity(
                client,
                self,
                "dev_standby_time",
                const.UNIT_TIMEOUT,
                const.UNIT_TIMEOUT_OPTIONS,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 13, "devStandbyTime": value},
                },
            ),
            TimeoutDictSelectEntity(
                client,
                self,
                "ac_standby_time",
                const.AC_TIMEOUT,
                const.AC_TIMEOUT_OPTIONS,
                lambda value: {
                    "moduleType": 0,
                    "operateType": "TCP",
                    "params": {"id": 10, "acStandbyTime": value},
                },
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
            _LOGGER.error(f"[River3] Data processing failed: {e}", exc_info=True)
            _LOGGER.debug("[River3] Attempting JSON fallback after protobuf failure")
            # Fallback to parent's JSON processing for compatibility
            try:
                return super()._prepare_data(raw_data)
            except Exception as e2:
                _LOGGER.error(f"[River3] JSON fallback also failed: {e2}")
                return {}

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

            # Try to decode as River3HeaderMessage
            try:
                header_msg = pb2.River3HeaderMessage()
                header_msg.ParseFromString(raw_data)
            except AttributeError as e:
                _LOGGER.error(f"River3HeaderMessage class not found in pb2 module: {e}")
                _LOGGER.debug(f"Available classes in pb2: {[attr for attr in dir(pb2) if not attr.startswith('_')]}")
                return None
            except Exception as e:
                _LOGGER.error(f"Failed to parse River3HeaderMessage: {e}")
                _LOGGER.debug(f"Raw data length: {len(raw_data)}, first 20 bytes: {raw_data[:20].hex()}")
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
                # River3DisplayPropertyUpload - main status and settings
                msg = pb2.River3DisplayPropertyUpload()
                msg.ParseFromString(pdata)
                return self._protobuf_to_dict(msg)

            elif cmd_func == 254 and cmd_id == 22:
                # River3RuntimePropertyUpload - runtime sensor data
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
                    _LOGGER.debug(f"Failed to decode as River3SetCommand: {e}")
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
                    _LOGGER.debug(f"Failed to decode as River3SetReply: {e}")
                    return {}

            elif cmd_func == 32 and cmd_id == 2:
                # River3CMSHeartBeatReport (CMS = Combined Management System)
                try:
                    msg = pb2.River3CMSHeartBeatReport()
                    msg.ParseFromString(pdata)
                    return self._protobuf_to_dict(msg)
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as River3CMSHeartBeatReport: {e}")
                    return {}

            elif self._is_bms_heartbeat(cmd_func, cmd_id):
                # River3BMSHeartBeatReport - Battery heartbeat with cycles and energy data
                try:
                    msg = pb2.River3BMSHeartBeatReport()
                    msg.ParseFromString(pdata)
                    _LOGGER.debug(f"Successfully decoded River3BMSHeartBeatReport: cmdFunc={cmd_func}, cmdId={cmd_id}")
                    return self._protobuf_to_dict(msg)
                except Exception as e:
                    _LOGGER.debug(f"Failed to decode as River3BMSHeartBeatReport (cmdFunc={cmd_func}, cmdId={cmd_id}): {e}")
                    return {}

            # Unknown message type - try River3BMSHeartBeatReport as fallback
            _LOGGER.debug(f"Unknown message type: cmdFunc={cmd_func}, cmdId={cmd_id}, size={len(pdata)} bytes")

            # Try to decode as River3BMSHeartBeatReport since that's a common case
            try:
                msg = pb2.River3BMSHeartBeatReport()
                msg.ParseFromString(pdata)
                result = self._protobuf_to_dict(msg)
                # Check if we got meaningful data (cycles or energy fields)
                if "cycles" in result or "accu_chg_energy" in result or "accu_dsg_energy" in result:
                    _LOGGER.info(
                        f"Found River3BMSHeartBeatReport at unexpected cmdFunc={cmd_func}, cmdId={cmd_id}. "
                        f"Consider updating BMS_HEARTBEAT_COMMANDS mapping."
                    )
                    return result
            except Exception as e:
                _LOGGER.debug(f"Failed fallback River3BMSHeartBeatReport decode: {e}")

            return {}

        except Exception as e:
            _LOGGER.error(f"Message decode error for cmdFunc={cmd_func}, cmdId={cmd_id}: {e}")
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
        """Decode protobuf for all topics since River 3 uses protobuf throughout."""
        if data_type == self.device_info.data_topic:
            raw = self._prepare_data(raw_data)
            self.data.update_data(raw)
        elif data_type == self.device_info.set_topic:
            # Set commands we send - use protobuf parsing
            raw = self._prepare_data(raw_data)
            self.data.add_set_message(raw)
        elif data_type == self.device_info.set_reply_topic:
            # Device replies with protobuf
            raw = self._prepare_data(raw_data)
            self.data.add_set_reply_message(raw)
        elif data_type == self.device_info.get_topic:
            # Get commands we send - use protobuf parsing
            raw = self._prepare_data(raw_data)
            self.data.add_get_message(raw)
        elif data_type == self.device_info.get_reply_topic:
            # Device replies with protobuf
            raw = self._prepare_data(raw_data)
            self.data.add_get_reply_message(raw)
        else:
            return False
        return True
