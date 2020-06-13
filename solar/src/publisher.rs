use crate::ToMainLoop;
use anyhow::Result;
use morningstar::prostar_mppt::{Settings, Stats};
use netidx::{
    self,
    chars::Chars,
    path::Path,
    publisher::{BindCfg, Publisher, Val, Value},
    resolver::Auth,
};
use solar_client::Config;
use uom::si::{
    electric_charge::ampere_hour,
    electric_current::ampere,
    electric_potential::volt,
    electrical_resistance::ohm,
    energy::kilowatt_hour,
    power::watt,
    thermodynamic_temperature::degree_celsius,
    time::{day, hour, minute, second},
};
use std::time::Duration;

#[derive(Clone)]
struct PublishedStats {
    timestamp: Val,
    software_version: Val,
    battery_voltage_settings_multiplier: Val,
    supply_3v3: Val,
    supply_12v: Val,
    supply_5v: Val,
    gate_drive_voltage: Val,
    battery_terminal_voltage: Val,
    array_voltage: Val,
    load_voltage: Val,
    charge_current: Val,
    array_current: Val,
    load_current: Val,
    battery_current_net: Val,
    battery_sense_voltage: Val,
    meterbus_voltage: Val,
    heatsink_temperature: Val,
    battery_temperature: Val,
    ambient_temperature: Val,
    rts_temperature: Val,
    u_inductor_temperature: Val,
    v_inductor_temperature: Val,
    w_inductor_temperature: Val,
    charge_state: Val,
    array_faults: Val,
    battery_voltage_slow: Val,
    target_voltage: Val,
    ah_charge_resettable: Val,
    ah_charge_total: Val,
    kwh_charge_resettable: Val,
    kwh_charge_total: Val,
    load_state: Val,
    load_faults: Val,
    lvd_setpoint: Val,
    ah_load_resettable: Val,
    ah_load_total: Val,
    hourmeter: Val,
    alarms: Val,
    array_power: Val,
    array_vmp: Val,
    array_max_power_sweep: Val,
    array_voc: Val,
    battery_v_min_daily: Val,
    battery_v_max_daily: Val,
    ah_charge_daily: Val,
    ah_load_daily: Val,
    array_faults_daily: Val,
    load_faults_daily: Val,
    alarms_daily: Val,
    array_voltage_max_daily: Val,
    array_voltage_fixed: Val,
    array_voc_percent_fixed: Val,
}

impl PublishedStats {
    fn new(publisher: &Publisher, base: &Path) -> Result<Self> {
        Ok(PublishedStats {
            timestamp: publisher.publish(base.append("timestamp"), Value::Null)?,
            software_version: publisher
                .publish(base.append("software_version"), Value::Null)?,
            battery_voltage_settings_multiplier: publisher.publish(
                base.append("battery_voltage_settings_multiplier"),
                Value::Null,
            )?,
            supply_3v3: publisher.publish(base.append("supply_3v3"), Value::Null)?,
            supply_12v: publisher.publish(base.append("supply_12v"), Value::Null)?,
            supply_5v: publisher.publish(base.append("supply_5v"), Value::Null)?,
            gate_drive_voltage: publisher
                .publish(base.append("gate_drive_voltage"), Value::Null)?,
            battery_terminal_voltage: publisher
                .publish(base.append("battery_terminal_voltage"), Value::Null)?,
            array_voltage: publisher
                .publish(base.append("array_voltage"), Value::Null)?,
            load_voltage: publisher.publish(base.append("load_voltage"), Value::Null)?,
            charge_current: publisher
                .publish(base.append("charge_current"), Value::Null)?,
            array_current: publisher
                .publish(base.append("array_current"), Value::Null)?,
            load_current: publisher.publish(base.append("load_current"), Value::Null)?,
            battery_current_net: publisher
                .publish(base.append("battery_current_net"), Value::Null)?,
            battery_sense_voltage: publisher
                .publish(base.append("battery_sense_voltage"), Value::Null)?,
            meterbus_voltage: publisher
                .publish(base.append("meterbus_voltage"), Value::Null)?,
            heatsink_temperature: publisher
                .publish(base.append("heatsink_temperature"), Value::Null)?,
            battery_temperature: publisher
                .publish(base.append("battery_temperature"), Value::Null)?,
            ambient_temperature: publisher
                .publish(base.append("ambient_temperature"), Value::Null)?,
            rts_temperature: publisher
                .publish(base.append("rts_temperature"), Value::Null)?,
            u_inductor_temperature: publisher
                .publish(base.append("u_inductor_temperature"), Value::Null)?,
            v_inductor_temperature: publisher
                .publish(base.append("v_inductor_temperature"), Value::Null)?,
            w_inductor_temperature: publisher
                .publish(base.append("w_inductor_temperature"), Value::Null)?,
            charge_state: publisher.publish(base.append("charge_state"), Value::Null)?,
            array_faults: publisher.publish(base.append("array_faults"), Value::Null)?,
            battery_voltage_slow: publisher
                .publish(base.append("battery_voltage_slow"), Value::Null)?,
            target_voltage: publisher
                .publish(base.append("target_voltage"), Value::Null)?,
            ah_charge_resettable: publisher
                .publish(base.append("ah_charge_resettable"), Value::Null)?,
            ah_charge_total: publisher
                .publish(base.append("ah_charge_total"), Value::Null)?,
            kwh_charge_resettable: publisher
                .publish(base.append("kwh_charge_resettable"), Value::Null)?,
            kwh_charge_total: publisher
                .publish(base.append("kwh_charge_total"), Value::Null)?,
            load_state: publisher.publish(base.append("load_state"), Value::Null)?,
            load_faults: publisher.publish(base.append("load_faults"), Value::Null)?,
            lvd_setpoint: publisher.publish(base.append("lvd_setpoint"), Value::Null)?,
            ah_load_resettable: publisher
                .publish(base.append("ah_load_resettable"), Value::Null)?,
            ah_load_total: publisher
                .publish(base.append("ah_load_total"), Value::Null)?,
            hourmeter: publisher.publish(base.append("hourmeter"), Value::Null)?,
            alarms: publisher.publish(base.append("alarms"), Value::Null)?,
            array_power: publisher.publish(base.append("array_power"), Value::Null)?,
            array_vmp: publisher.publish(base.append("array_vmp"), Value::Null)?,
            array_max_power_sweep: publisher
                .publish(base.append("array_max_power_sweep"), Value::Null)?,
            array_voc: publisher.publish(base.append("array_voc"), Value::Null)?,
            battery_v_min_daily: publisher
                .publish(base.append("battery_v_min_daily"), Value::Null)?,
            battery_v_max_daily: publisher
                .publish(base.append("battery_v_max_daily"), Value::Null)?,
            ah_charge_daily: publisher
                .publish(base.append("ah_charge_daily"), Value::Null)?,
            ah_load_daily: publisher
                .publish(base.append("ah_load_daily"), Value::Null)?,
            array_faults_daily: publisher
                .publish(base.append("array_faults_daily"), Value::Null)?,
            load_faults_daily: publisher
                .publish(base.append("load_faults_daily"), Value::Null)?,
            alarms_daily: publisher.publish(base.append("alarms_daily"), Value::Null)?,
            array_voltage_max_daily: publisher
                .publish(base.append("array_voltage_max_daily"), Value::Null)?,
            array_voltage_fixed: publisher
                .publish(base.append("array_voltage_fixed"), Value::Null)?,
            array_voc_percent_fixed: publisher
                .publish(base.append("array_voc_percent_fixed"), Value::Null)?,
        })
    }

    fn update(&self, st: &Stats) {
        self.timestamp.update(Value::String(Chars::from(st.timestamp.to_string())));
        self.software_version.update(Value::V32(st.software_version as u32));
        self.battery_voltage_settings_multiplier
            .update(Value::V32(st.battery_voltage_settings_multiplier as u32));
        self.supply_3v3.update(Value::F32(st.supply_3v3.get::<volt>()));
        self.supply_12v.update(Value::F32(st.supply_12v.get::<volt>()));
        self.supply_5v.update(Value::F32(st.supply_5v.get::<volt>()));
        self.gate_drive_voltage.update(Value::F32(st.gate_drive_voltage.get::<volt>()));
        self.battery_terminal_voltage
            .update(Value::F32(st.battery_terminal_voltage.get::<volt>()));
        self.array_voltage.update(Value::F32(st.array_voltage.get::<volt>()));
        self.load_voltage.update(Value::F32(st.load_voltage.get::<volt>()));
        self.charge_current.update(Value::F32(st.charge_current.get::<ampere>()));
        self.array_current.update(Value::F32(st.array_current.get::<ampere>()));
        self.load_current.update(Value::F32(st.load_current.get::<ampere>()));
        self.battery_current_net
            .update(Value::F32(st.battery_current_net.get::<ampere>()));
        self.battery_sense_voltage
            .update(Value::F32(st.battery_sense_voltage.get::<volt>()));
        self.meterbus_voltage.update(Value::F32(st.meterbus_voltage.get::<volt>()));
        self.heatsink_temperature
            .update(Value::F32(st.heatsink_temperature.get::<degree_celsius>()));
        self.battery_temperature
            .update(Value::F32(st.battery_temperature.get::<degree_celsius>()));
        self.ambient_temperature
            .update(Value::F32(st.ambient_temperature.get::<degree_celsius>()));
        self.rts_temperature.update(
            st.rts_temperature
                .map(|t| Value::F32(t.get::<degree_celsius>()))
                .unwrap_or(Value::Null),
        );
        self.u_inductor_temperature
            .update(Value::F32(st.u_inductor_temperature.get::<degree_celsius>()));
        self.v_inductor_temperature
            .update(Value::F32(st.v_inductor_temperature.get::<degree_celsius>()));
        self.w_inductor_temperature
            .update(Value::F32(st.w_inductor_temperature.get::<degree_celsius>()));
        self.charge_state
            .update(Value::String(Chars::from(format!("{:?}", st.charge_state))));
        self.array_faults.update(Value::V32(st.array_faults.bits() as u32));
        self.battery_voltage_slow
            .update(Value::F32(st.battery_voltage_slow.get::<volt>()));
        self.target_voltage.update(Value::F32(st.target_voltage.get::<volt>()));
        self.ah_charge_resettable
            .update(Value::F32(st.ah_charge_resettable.get::<ampere_hour>()));
        self.ah_charge_total.update(Value::F32(st.ah_charge_total.get::<ampere_hour>()));
        self.kwh_charge_resettable
            .update(Value::F32(st.kwh_charge_resettable.get::<kilowatt_hour>()));
        self.kwh_charge_total
            .update(Value::F32(st.kwh_charge_total.get::<kilowatt_hour>()));
        self.load_state
            .update(Value::String(Chars::from(format!("{:?}", st.load_state))));
        self.load_faults.update(Value::V32(st.load_faults.bits() as u32));
        self.lvd_setpoint.update(Value::F32(st.lvd_setpoint.get::<volt>()));
        self.ah_load_resettable
            .update(Value::F32(st.ah_load_resettable.get::<ampere_hour>()));
        self.ah_load_total.update(Value::F32(st.ah_load_total.get::<ampere_hour>()));
        self.hourmeter.update(Value::F32(st.hourmeter.get::<hour>()));
        self.alarms.update(Value::U32(st.alarms.bits()));
        self.array_power.update(Value::F32(st.array_power.get::<watt>()));
        self.array_vmp.update(Value::F32(st.array_vmp.get::<volt>()));
        self.array_max_power_sweep
            .update(Value::F32(st.array_max_power_sweep.get::<watt>()));
        self.array_voc.update(Value::F32(st.array_voc.get::<volt>()));
        self.battery_v_min_daily.update(Value::F32(st.battery_v_min_daily.get::<volt>()));
        self.battery_v_max_daily.update(Value::F32(st.battery_v_max_daily.get::<volt>()));
        self.ah_charge_daily.update(Value::F32(st.ah_charge_daily.get::<ampere_hour>()));
        self.ah_load_daily.update(Value::F32(st.ah_load_daily.get::<ampere_hour>()));
        self.array_faults_daily.update(Value::V32(st.array_faults_daily.bits() as u32));
        self.load_faults_daily.update(Value::V32(st.load_faults_daily.bits() as u32));
        self.alarms_daily.update(Value::U32(st.alarms_daily.bits()));
        self.array_voltage_max_daily
            .update(Value::F32(st.array_voltage_max_daily.get::<volt>()));
        self.array_voltage_fixed.update(Value::F32(st.array_voltage_fixed.get::<volt>()));
        self.array_voc_percent_fixed.update(Value::F32(st.array_voc_percent_fixed));
    }
}

#[derive(Clone)]
struct PublishedSettings {
    regulation_voltage: Val,
    float_voltage: Val,
    time_before_float: Val,
    time_before_float_low_battery: Val,
    float_low_battery_voltage_trigger: Val,
    float_cancel_voltage: Val,
    exit_float_time: Val,
    equalize_voltage: Val,
    days_between_equalize_cycles: Val,
    equalize_time_limit_above_regulation_voltage: Val,
    equalize_time_limit_at_regulation_voltage: Val,
    alarm_on_setting_change: Val,
    reference_charge_voltage_limit: Val,
    battery_charge_current_limit: Val,
    temperature_compensation_coefficent: Val,
    high_voltage_disconnect: Val,
    high_voltage_reconnect: Val,
    maximum_charge_voltage_reference: Val,
    max_battery_temp_compensation_limit: Val,
    min_battery_temp_compensation_limit: Val,
    load_low_voltage_disconnect: Val,
    load_low_voltage_reconnect: Val,
    load_high_voltage_disconnect: Val,
    load_high_voltage_reconnect: Val,
    lvd_load_current_compensation: Val,
    lvd_warning_timeout: Val,
    led_green_to_green_and_yellow_limit: Val,
    led_green_and_yellow_to_yellow_limit: Val,
    led_yellow_to_yellow_and_red_limit: Val,
    led_yellow_and_red_to_red_flashing_limit: Val,
    modbus_id: Val,
    meterbus_id: Val,
    mppt_fixed_vmp: Val,
    mppt_fixed_vmp_percent: Val,
    charge_current_limit: Val,
}

impl PublishedSettings {
    fn new(publisher: &Publisher, base: &Path) -> Result<Self> {
        Ok(PublishedSettings {
            regulation_voltage: publisher
                .publish(base.append("regulation_voltage"), Value::Null)?,
            float_voltage: publisher
                .publish(base.append("float_voltage"), Value::Null)?,
            time_before_float: publisher
                .publish(base.append("time_before_float"), Value::Null)?,
            time_before_float_low_battery: publisher
                .publish(base.append("time_before_float_low_battery"), Value::Null)?,
            float_low_battery_voltage_trigger: publisher
                .publish(base.append("float_low_battery_voltage_trigger"), Value::Null)?,
            float_cancel_voltage: publisher
                .publish(base.append("float_cancel_voltage"), Value::Null)?,
            exit_float_time: publisher
                .publish(base.append("exit_float_time"), Value::Null)?,
            equalize_voltage: publisher
                .publish(base.append("equalize_voltage"), Value::Null)?,
            days_between_equalize_cycles: publisher
                .publish(base.append("days_between_equalize_cycles"), Value::Null)?,
            equalize_time_limit_above_regulation_voltage: publisher.publish(
                base.append("equalize_time_limit_above_regulation_voltage"),
                Value::Null,
            )?,
            equalize_time_limit_at_regulation_voltage: publisher.publish(
                base.append("equalize_time_limit_at_regulation_voltage"),
                Value::Null,
            )?,
            alarm_on_setting_change: publisher
                .publish(base.append("alarm_on_setting_change"), Value::Null)?,
            reference_charge_voltage_limit: publisher
                .publish(base.append("reference_charge_voltage_limit"), Value::Null)?,
            battery_charge_current_limit: publisher
                .publish(base.append("battery_charge_current_limit"), Value::Null)?,
            temperature_compensation_coefficent: publisher.publish(
                base.append("temperature_compensation_coefficent"),
                Value::Null,
            )?,
            high_voltage_disconnect: publisher
                .publish(base.append("high_voltage_disconnect"), Value::Null)?,
            high_voltage_reconnect: publisher
                .publish(base.append("high_voltage_reconnect"), Value::Null)?,
            maximum_charge_voltage_reference: publisher
                .publish(base.append("maximum_charge_voltage_reference"), Value::Null)?,
            max_battery_temp_compensation_limit: publisher.publish(
                base.append("max_battery_temp_compensation_limit"),
                Value::Null,
            )?,
            min_battery_temp_compensation_limit: publisher.publish(
                base.append("min_battery_temp_compensation_limit"),
                Value::Null,
            )?,
            load_low_voltage_disconnect: publisher
                .publish(base.append("load_low_voltage_disconnect"), Value::Null)?,
            load_low_voltage_reconnect: publisher
                .publish(base.append("load_low_voltage_reconnect"), Value::Null)?,
            load_high_voltage_disconnect: publisher
                .publish(base.append("load_high_voltage_disconnect"), Value::Null)?,
            load_high_voltage_reconnect: publisher
                .publish(base.append("load_high_voltage_reconnect"), Value::Null)?,
            lvd_load_current_compensation: publisher
                .publish(base.append("lvd_load_current_compensation"), Value::Null)?,
            lvd_warning_timeout: publisher
                .publish(base.append("lvd_warning_timeout"), Value::Null)?,
            led_green_to_green_and_yellow_limit: publisher.publish(
                base.append("led_green_to_green_and_yellow_limit"),
                Value::Null,
            )?,
            led_green_and_yellow_to_yellow_limit: publisher.publish(
                base.append("led_green_and_yellow_to_yellow_limit"),
                Value::Null,
            )?,
            led_yellow_to_yellow_and_red_limit: publisher.publish(
                base.append("led_yellow_to_yellow_and_red_limit"),
                Value::Null,
            )?,
            led_yellow_and_red_to_red_flashing_limit: publisher.publish(
                base.append("led_yellow_and_red_to_red_flashing_limit"),
                Value::Null,
            )?,
            modbus_id: publisher.publish(base.append("modbus_id"), Value::Null)?,
            meterbus_id: publisher.publish(base.append("meterbus_id"), Value::Null)?,
            mppt_fixed_vmp: publisher
                .publish(base.append("mppt_fixed_vmp"), Value::Null)?,
            mppt_fixed_vmp_percent: publisher
                .publish(base.append("mppt_fixed_vmp_percent"), Value::Null)?,
            charge_current_limit: publisher
                .publish(base.append("charge_current_limit"), Value::Null)?,
        })
    }

    fn update(&self, set: &Settings) {
        self.regulation_voltage.update(Value::F32(set.regulation_voltage.get::<volt>()));
        self.float_voltage.update(Value::F32(set.float_voltage.get::<volt>()));
        self.time_before_float.update(Value::F32(set.time_before_float.get::<second>()));
        self.time_before_float_low_battery
            .update(Value::F32(set.time_before_float_low_battery.get::<second>()));
        self.float_low_battery_voltage_trigger
            .update(Value::F32(set.float_low_battery_voltage_trigger.get::<volt>()));
        self.float_cancel_voltage
            .update(Value::F32(set.float_cancel_voltage.get::<volt>()));
        self.exit_float_time.update(Value::F32(set.exit_float_time.get::<minute>()));
        self.equalize_voltage.update(Value::F32(set.equalize_voltage.get::<volt>()));
        self.days_between_equalize_cycles
            .update(Value::F32(set.days_between_equalize_cycles.get::<day>()));
        self.equalize_time_limit_above_regulation_voltage.update(Value::F32(
            set.equalize_time_limit_above_regulation_voltage.get::<minute>(),
        ));
        self.equalize_time_limit_at_regulation_voltage.update(Value::F32(
            set.equalize_time_limit_at_regulation_voltage.get::<minute>(),
        ));
        self.alarm_on_setting_change.update(match set.alarm_on_setting_change {
            true => Value::True,
            false => Value::False,
        });
        self.reference_charge_voltage_limit
            .update(Value::F32(set.reference_charge_voltage_limit.get::<volt>()));
        self.battery_charge_current_limit
            .update(Value::F32(set.battery_charge_current_limit.get::<ampere>()));
        self.temperature_compensation_coefficent
            .update(Value::F32(set.temperature_compensation_coefficent.get::<volt>()));
        self.high_voltage_disconnect
            .update(Value::F32(set.high_voltage_disconnect.get::<volt>()));
        self.high_voltage_reconnect
            .update(Value::F32(set.high_voltage_reconnect.get::<volt>()));
        self.maximum_charge_voltage_reference
            .update(Value::F32(set.maximum_charge_voltage_reference.get::<volt>()));
        self.max_battery_temp_compensation_limit.update(Value::F32(
            set.max_battery_temp_compensation_limit.get::<degree_celsius>(),
        ));
        self.min_battery_temp_compensation_limit.update(Value::F32(
            set.min_battery_temp_compensation_limit.get::<degree_celsius>(),
        ));
        self.load_low_voltage_disconnect
            .update(Value::F32(set.load_low_voltage_disconnect.get::<volt>()));
        self.load_low_voltage_reconnect
            .update(Value::F32(set.load_low_voltage_reconnect.get::<volt>()));
        self.load_high_voltage_disconnect
            .update(Value::F32(set.load_high_voltage_disconnect.get::<volt>()));
        self.load_high_voltage_reconnect
            .update(Value::F32(set.load_high_voltage_reconnect.get::<volt>()));
        self.lvd_load_current_compensation
            .update(Value::F32(set.lvd_load_current_compensation.get::<ohm>()));
        self.lvd_warning_timeout
            .update(Value::F32(set.lvd_warning_timeout.get::<second>()));
        self.led_green_to_green_and_yellow_limit
            .update(Value::F32(set.led_green_to_green_and_yellow_limit.get::<volt>()));
        self.led_green_and_yellow_to_yellow_limit
            .update(Value::F32(set.led_green_and_yellow_to_yellow_limit.get::<volt>()));
        self.led_yellow_to_yellow_and_red_limit
            .update(Value::F32(set.led_yellow_to_yellow_and_red_limit.get::<volt>()));
        self.led_yellow_and_red_to_red_flashing_limit.update(Value::F32(
            set.led_yellow_and_red_to_red_flashing_limit.get::<volt>(),
        ));
        self.modbus_id.update(Value::U32(set.modbus_id as u32));
        self.meterbus_id.update(Value::U32(set.meterbus_id as u32));
        self.mppt_fixed_vmp.update(Value::F32(set.mppt_fixed_vmp.get::<volt>()));
        self.mppt_fixed_vmp_percent.update(Value::F32(set.mppt_fixed_vmp_percent));
        self.charge_current_limit
            .update(Value::F32(set.charge_current_limit.get::<ampere>()));
    }
}

struct Netidx {
    publisher: Publisher,
    stats: PublishedStats,
    settings: PublishedSettings,
}

impl Netidx {
    async fn new(cfg: &Config) -> Result<Self> {
        let resolver = netidx::config::Config::load_default()?;
        let bindcfg = cfg.netidx_bind.parse::<BindCfg>()?;
        let base = Path::from(cfg.netidx_base.clone());
        let auth = cfg
            .netidx_spn
            .clone()
            .map(|s| Auth::Krb5 { spn: Some(s), upn: None })
            .unwrap_or(Auth::Anonymous);
        let publisher = Publisher::new(resolver, auth, bindcfg).await?;
        let stats = PublishedStats::new(&publisher, &base.append("stats"))?;
        let settings = PublishedSettings::new(&publisher, &base.append("settings"))?;
        Ok(Netidx { publisher, stats, settings })
    }

    fn update_stats(&self, st: &Stats) {
        self.stats.update(st);
    }

    fn update_settings(&self, set: &Settings) {
        self.settings.update(set);
    }

    async fn flush(&self, timeout: Duration) -> Result<()> {
        Ok(self.publisher.flush(Some(timeout)).await?)
    }
}
