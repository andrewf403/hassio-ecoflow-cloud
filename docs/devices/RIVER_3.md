## RIVER_3

*Sensors*
- Main Battery Level (`bms_batt_soc`)
- Main Design Capacity (`bms_design_cap`)   _(disabled)_
- Main Full Capacity (`bms_full_cap`)   _(disabled)_
- Main Remain Capacity (`bms_remain_cap`)   _(disabled)_
- State of Health (`bms_batt_soh`)
- Battery Level (`cms_batt_soc`)
- Battery Charging State (`bms_chg_dsg_state`)
- Total In Power (`pow_in_sum_w`) (energy:  _[Device Name]_ Total In  Energy)
- Total Out Power (`pow_out_sum_w`) (energy:  _[Device Name]_ Total Out  Energy)
- Solar In Power (`pow_get_pv`)
- Solar In Current (`plug_in_info_pv_amp`)
- AC In Power (`pow_get_ac_in`)
- AC Out Power (`pow_get_ac_out`)
- AC In Volts (`plug_in_info_ac_in_vol`)
- DC Out Power (`pow_get_12v`)
- Type-C (1) Out Power (`pow_get_typec1`)
- USB QC (1) Out Power (`pow_get_qcusb1`)
- USB QC (2) Out Power (`pow_get_qcusb2`)
- Charge Remaining Time (`bms_chg_rem_time`)
- Discharge Remaining Time (`bms_dsg_rem_time`)
- Remaining Time (`cms_chg_rem_time`)
- PCS DC Temperature (`temp_pcs_dc`)
- PCS AC Temperature (`temp_pcs_ac`)
- Battery Temperature (`bms_min_cell_temp`)
- Max Cell Temperature (`bms_max_cell_temp`)   _(disabled)_
- Battery Volts (`bms_batt_vol`)   _(disabled)_
- Min Cell Volts (`bms_min_cell_vol`)   _(disabled)_
- Max Cell Volts (`bms_max_cell_vol`)   _(disabled)_
- Cycles (`cycles`)
- AC Output Energy (`ac_out_energy`)
- AC Input Energy (`ac_in_energy`)
- Solar In Energy (`pv_in_energy`)
- DC 12V Output Energy (`dc12v_out_energy`)   _(disabled)_
- Type-C Output Energy (`typec_out_energy`)   _(disabled)_
- USB-A Output Energy (`usba_out_energy`)   _(disabled)_
- Status

*Switches*
- Beeper (`en_beep` -> `{"en_beep": "VALUE"}`)
- AC Enabled (`cfg_ac_out_open` -> `{"cfg_ac_out_open": "VALUE"}`)
- X-Boost Enabled (`xboost_en` -> `{"xboost_en": "VALUE"}`)
- DC (12V) Enabled (`cfg_dc12v_out_open` -> `{"cfg_dc12v_out_open": "VALUE"}`)
- AC Always On (`output_power_off_memory` -> `{"output_power_off_memory": "VALUE"}`)
- Backup Reserve Enabled (`energy_backup_en` -> `{"energy_backup_en": "VALUE", "energy_backup_start_soc": "VALUE"}`)

*Sliders (numbers)*
- Max Charge Level (`cms_max_chg_soc` -> `{"cms_max_chg_soc": "VALUE"}` [50 - 100])
- Min Discharge Level (`cms_min_dsg_soc` -> `{"cms_min_dsg_soc": "VALUE"}` [0 - 30])
- AC Charging Power (`plug_in_info_ac_in_chg_pow_max` -> `{"plug_in_info_ac_in_chg_pow_max": "VALUE"}` [50 - 305])
- Backup Reserve Level (`energy_backup_start_soc` -> `{"energy_backup_en": 1, "energy_backup_start_soc": "VALUE"}` [5 - 100])

*Selects*
- DC (12V) Charge Current (`plug_in_info_pv_dc_amp_max` -> `{"plug_in_info_pv_dc_amp_max": "VALUE"}` [4A (4), 6A (6), 8A (8)])
- DC Mode (`pv_chg_type` -> `{"pv_chg_type": "VALUE"}` [Auto (0), Solar Recharging (1), Car Recharging (2)])
- Screen Timeout (`screen_off_time` -> `{"screen_off_time": "VALUE"}` [Never (0), 10 sec (10), 30 sec (30), 1 min (60), 5 min (300), 30 min (1800)])
- Unit Timeout (`dev_standby_time` -> `{"dev_standby_time": "VALUE"}` [Never (0), 30 min (30), 1 hr (60), 2 hr (120), 4 hr (240), 6 hr (360), 12 hr (720), 24 hr (1440)])
- AC Timeout (`ac_standby_time` -> `{"ac_standby_time": "VALUE"}` [Never (0), 30 min (30), 1 hr (60), 2 hr (120), 4 hr (240), 6 hr (360), 12 hr (720), 24 hr (1440)])

