use crate::ToMainLoop;
use anyhow::Result;
use futures::{channel::mpsc as fmpsc, prelude::*, select_biased};
use log::{info, warn};
use morningstar::prostar_mppt::{ChargeState, LoadState, Settings, Stats};
use netidx::{
    self,
    chars::Chars,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, DesiredAuth, Publisher, UpdateBatch, Val, Value, WriteRequest},
};
use parking_lot::Mutex;
use solar_client::{Config, FromClient, ToClient};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::mpsc::{self, Sender},
    task,
};
use uom::si::{
    electric_charge::ampere_hour,
    electric_current::ampere,
    electric_potential::volt,
    electrical_resistance::ohm,
    energy::kilowatt_hour,
    f32::*,
    power::watt,
    thermodynamic_temperature::degree_celsius,
    time::{day, hour, minute, second},
};

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

    fn update(&self, batch: &mut UpdateBatch, st: &Stats) {
        use chrono::prelude::*;
        self.timestamp
            .update_changed(batch, Value::DateTime(DateTime::<Utc>::from(st.timestamp)));
        self.software_version
            .update_changed(batch, Value::V32(st.software_version as u32));
        self.battery_voltage_settings_multiplier
            .update(batch, Value::V32(st.battery_voltage_settings_multiplier as u32));
        self.supply_3v3.update_changed(batch, Value::F32(st.supply_3v3.get::<volt>()));
        self.supply_12v.update_changed(batch, Value::F32(st.supply_12v.get::<volt>()));
        self.supply_5v.update_changed(batch, Value::F32(st.supply_5v.get::<volt>()));
        self.gate_drive_voltage
            .update_changed(batch, Value::F32(st.gate_drive_voltage.get::<volt>()));
        self.battery_terminal_voltage
            .update_changed(batch, Value::F32(st.battery_terminal_voltage.get::<volt>()));
        self.array_voltage
            .update_changed(batch, Value::F32(st.array_voltage.get::<volt>()));
        self.load_voltage
            .update_changed(batch, Value::F32(st.load_voltage.get::<volt>()));
        self.charge_current
            .update_changed(batch, Value::F32(st.charge_current.get::<ampere>()));
        self.array_current
            .update_changed(batch, Value::F32(st.array_current.get::<ampere>()));
        self.load_current
            .update_changed(batch, Value::F32(st.load_current.get::<ampere>()));
        self.battery_current_net
            .update_changed(batch, Value::F32(st.battery_current_net.get::<ampere>()));
        self.battery_sense_voltage
            .update_changed(batch, Value::F32(st.battery_sense_voltage.get::<volt>()));
        self.meterbus_voltage
            .update_changed(batch, Value::F32(st.meterbus_voltage.get::<volt>()));
        self.heatsink_temperature.update_changed(
            batch,
            Value::F32(st.heatsink_temperature.get::<degree_celsius>()),
        );
        self.battery_temperature.update_changed(
            batch,
            Value::F32(st.battery_temperature.get::<degree_celsius>()),
        );
        self.ambient_temperature.update_changed(
            batch,
            Value::F32(st.ambient_temperature.get::<degree_celsius>()),
        );
        self.rts_temperature.update_changed(
            batch,
            st.rts_temperature
                .map(|t| Value::F32(t.get::<degree_celsius>()))
                .unwrap_or(Value::Null),
        );
        self.u_inductor_temperature.update_changed(
            batch,
            Value::F32(st.u_inductor_temperature.get::<degree_celsius>()),
        );
        self.v_inductor_temperature.update_changed(
            batch,
            Value::F32(st.v_inductor_temperature.get::<degree_celsius>()),
        );
        self.w_inductor_temperature.update_changed(
            batch,
            Value::F32(st.w_inductor_temperature.get::<degree_celsius>()),
        );
        self.charge_state.update_changed(
            batch,
            Value::String(Chars::from(format!("{:?}", st.charge_state))),
        );
        self.array_faults
            .update_changed(batch, Value::V32(st.array_faults.bits() as u32));
        self.battery_voltage_slow
            .update_changed(batch, Value::F32(st.battery_voltage_slow.get::<volt>()));
        self.target_voltage
            .update_changed(batch, Value::F32(st.target_voltage.get::<volt>()));
        self.ah_charge_resettable.update_changed(
            batch,
            Value::F32(st.ah_charge_resettable.get::<ampere_hour>()),
        );
        self.ah_charge_total
            .update_changed(batch, Value::F32(st.ah_charge_total.get::<ampere_hour>()));
        self.kwh_charge_resettable.update_changed(
            batch,
            Value::F32(st.kwh_charge_resettable.get::<kilowatt_hour>()),
        );
        self.kwh_charge_total.update_changed(
            batch,
            Value::F32(st.kwh_charge_total.get::<kilowatt_hour>()),
        );
        self.load_state.update_changed(
            batch,
            Value::String(Chars::from(format!("{:?}", st.load_state))),
        );
        self.load_faults.update_changed(batch, Value::V32(st.load_faults.bits() as u32));
        self.lvd_setpoint
            .update_changed(batch, Value::F32(st.lvd_setpoint.get::<volt>()));
        self.ah_load_resettable.update_changed(
            batch,
            Value::F32(st.ah_load_resettable.get::<ampere_hour>()),
        );
        self.ah_load_total
            .update_changed(batch, Value::F32(st.ah_load_total.get::<ampere_hour>()));
        self.hourmeter.update_changed(batch, Value::F32(st.hourmeter.get::<hour>()));
        self.alarms.update_changed(batch, Value::U32(st.alarms.bits()));
        self.array_power.update_changed(batch, Value::F32(st.array_power.get::<watt>()));
        self.array_vmp.update_changed(batch, Value::F32(st.array_vmp.get::<volt>()));
        self.array_max_power_sweep
            .update_changed(batch, Value::F32(st.array_max_power_sweep.get::<watt>()));
        self.array_voc.update_changed(batch, Value::F32(st.array_voc.get::<volt>()));
        self.battery_v_min_daily
            .update_changed(batch, Value::F32(st.battery_v_min_daily.get::<volt>()));
        self.battery_v_max_daily
            .update_changed(batch, Value::F32(st.battery_v_max_daily.get::<volt>()));
        self.ah_charge_daily
            .update_changed(batch, Value::F32(st.ah_charge_daily.get::<ampere_hour>()));
        self.ah_load_daily
            .update_changed(batch, Value::F32(st.ah_load_daily.get::<ampere_hour>()));
        self.array_faults_daily
            .update_changed(batch, Value::V32(st.array_faults_daily.bits() as u32));
        self.load_faults_daily
            .update_changed(batch, Value::V32(st.load_faults_daily.bits() as u32));
        self.alarms_daily.update_changed(batch, Value::U32(st.alarms_daily.bits()));
        self.array_voltage_max_daily
            .update_changed(batch, Value::F32(st.array_voltage_max_daily.get::<volt>()));
        self.array_voltage_fixed
            .update_changed(batch, Value::F32(st.array_voltage_fixed.get::<volt>()));
        self.array_voc_percent_fixed
            .update_changed(batch, Value::F32(st.array_voc_percent_fixed));
    }
}

macro_rules! f32 {
    ($r:expr) => {
        match $r.value {
            Value::F32(v) => v,
            v => {
                let m = format!("{:?} not accepted, expected F32", v);
                warn!("{}", &m);
                if let Some(reply) = $r.send_result {
                    reply.send(Value::Error(Chars::from(m)));
                }
                continue;
            }
        }
    };
}

macro_rules! bool {
    ($r:expr) => {
        match $r.value {
            Value::True => true,
            Value::False => false,
            v => {
                let m = format!("{:?} not accepted, expected bool", v);
                warn!("{}", &m);
                if let Some(reply) = $r.send_result {
                    reply.send(Value::Error(Chars::from(m)));
                }
                return None;
            }
        }
    };
}

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

    fn update(&self, batch: &mut UpdateBatch, set: &Settings) {
        self.regulation_voltage
            .update_changed(batch, Value::F32(set.regulation_voltage.get::<volt>()));
        self.float_voltage
            .update_changed(batch, Value::F32(set.float_voltage.get::<volt>()));
        self.time_before_float
            .update_changed(batch, Value::F32(set.time_before_float.get::<second>()));
        self.time_before_float_low_battery.update_changed(
            batch,
            Value::F32(set.time_before_float_low_battery.get::<second>()),
        );
        self.float_low_battery_voltage_trigger.update_changed(
            batch,
            Value::F32(set.float_low_battery_voltage_trigger.get::<volt>()),
        );
        self.float_cancel_voltage
            .update_changed(batch, Value::F32(set.float_cancel_voltage.get::<volt>()));
        self.exit_float_time
            .update_changed(batch, Value::F32(set.exit_float_time.get::<minute>()));
        self.equalize_voltage
            .update_changed(batch, Value::F32(set.equalize_voltage.get::<volt>()));
        self.days_between_equalize_cycles.update_changed(
            batch,
            Value::F32(set.days_between_equalize_cycles.get::<day>()),
        );
        self.equalize_time_limit_above_regulation_voltage.update_changed(
            batch,
            Value::F32(set.equalize_time_limit_above_regulation_voltage.get::<minute>()),
        );
        self.equalize_time_limit_at_regulation_voltage.update_changed(
            batch,
            Value::F32(set.equalize_time_limit_at_regulation_voltage.get::<minute>()),
        );
        self.alarm_on_setting_change.update_changed(
            batch,
            match set.alarm_on_setting_change {
                true => Value::True,
                false => Value::False,
            },
        );
        self.reference_charge_voltage_limit.update_changed(
            batch,
            Value::F32(set.reference_charge_voltage_limit.get::<volt>()),
        );
        self.battery_charge_current_limit.update_changed(
            batch,
            Value::F32(set.battery_charge_current_limit.get::<ampere>()),
        );
        self.temperature_compensation_coefficent.update_changed(
            batch,
            Value::F32(set.temperature_compensation_coefficent.get::<volt>()),
        );
        self.high_voltage_disconnect
            .update_changed(batch, Value::F32(set.high_voltage_disconnect.get::<volt>()));
        self.high_voltage_reconnect
            .update_changed(batch, Value::F32(set.high_voltage_reconnect.get::<volt>()));
        self.maximum_charge_voltage_reference.update_changed(
            batch,
            Value::F32(set.maximum_charge_voltage_reference.get::<volt>()),
        );
        self.max_battery_temp_compensation_limit.update_changed(
            batch,
            Value::F32(set.max_battery_temp_compensation_limit.get::<degree_celsius>()),
        );
        self.min_battery_temp_compensation_limit.update_changed(
            batch,
            Value::F32(set.min_battery_temp_compensation_limit.get::<degree_celsius>()),
        );
        self.load_low_voltage_disconnect.update_changed(
            batch,
            Value::F32(set.load_low_voltage_disconnect.get::<volt>()),
        );
        self.load_low_voltage_reconnect.update_changed(
            batch,
            Value::F32(set.load_low_voltage_reconnect.get::<volt>()),
        );
        self.load_high_voltage_disconnect.update_changed(
            batch,
            Value::F32(set.load_high_voltage_disconnect.get::<volt>()),
        );
        self.load_high_voltage_reconnect.update_changed(
            batch,
            Value::F32(set.load_high_voltage_reconnect.get::<volt>()),
        );
        self.lvd_load_current_compensation.update_changed(
            batch,
            Value::F32(set.lvd_load_current_compensation.get::<ohm>()),
        );
        self.lvd_warning_timeout
            .update_changed(batch, Value::F32(set.lvd_warning_timeout.get::<second>()));
        self.led_green_to_green_and_yellow_limit.update_changed(
            batch,
            Value::F32(set.led_green_to_green_and_yellow_limit.get::<volt>()),
        );
        self.led_green_and_yellow_to_yellow_limit.update_changed(
            batch,
            Value::F32(set.led_green_and_yellow_to_yellow_limit.get::<volt>()),
        );
        self.led_yellow_to_yellow_and_red_limit.update_changed(
            batch,
            Value::F32(set.led_yellow_to_yellow_and_red_limit.get::<volt>()),
        );
        self.led_yellow_and_red_to_red_flashing_limit.update_changed(
            batch,
            Value::F32(set.led_yellow_and_red_to_red_flashing_limit.get::<volt>()),
        );
        self.modbus_id.update_changed(batch, Value::U32(set.modbus_id as u32));
        self.meterbus_id.update_changed(batch, Value::U32(set.meterbus_id as u32));
        self.mppt_fixed_vmp
            .update_changed(batch, Value::F32(set.mppt_fixed_vmp.get::<volt>()));
        self.mppt_fixed_vmp_percent
            .update_changed(batch, Value::F32(set.mppt_fixed_vmp_percent));
        self.charge_current_limit
            .update_changed(batch, Value::F32(set.charge_current_limit.get::<ampere>()));
    }

    fn register_writable(
        &self,
        publisher: &Publisher,
        channel: fmpsc::Sender<Pooled<Vec<WriteRequest>>>,
    ) {
        publisher.writes(self.regulation_voltage.id(), channel.clone());
        publisher.writes(self.float_voltage.id(), channel.clone());
        publisher.writes(self.time_before_float.id(), channel.clone());
        publisher.writes(self.time_before_float_low_battery.id(), channel.clone());
        publisher.writes(self.float_low_battery_voltage_trigger.id(), channel.clone());
        publisher.writes(self.float_cancel_voltage.id(), channel.clone());
        publisher.writes(self.exit_float_time.id(), channel.clone());
        publisher.writes(self.equalize_voltage.id(), channel.clone());
        publisher.writes(self.days_between_equalize_cycles.id(), channel.clone());
        publisher.writes(self.equalize_time_limit_above_regulation_voltage.id(), channel.clone());
        publisher.writes(self.equalize_time_limit_at_regulation_voltage.id(), channel.clone());
        publisher.writes(self.alarm_on_setting_change.id(), channel.clone());
        publisher.writes(self.reference_charge_voltage_limit.id(), channel.clone());
        publisher.writes(self.battery_charge_current_limit.id(), channel.clone());
        publisher.writes(self.temperature_compensation_coefficent.id(), channel.clone());
        publisher.writes(self.high_voltage_disconnect.id(), channel.clone());
        publisher.writes(self.high_voltage_reconnect.id(), channel.clone());
        publisher.writes(self.maximum_charge_voltage_reference.id(), channel.clone());
        publisher.writes(self.max_battery_temp_compensation_limit.id(), channel.clone());
        publisher.writes(self.min_battery_temp_compensation_limit.id(), channel.clone());
        publisher.writes(self.load_low_voltage_disconnect.id(), channel.clone());
        publisher.writes(self.load_low_voltage_reconnect.id(), channel.clone());
        publisher.writes(self.load_high_voltage_disconnect.id(), channel.clone());
        publisher.writes(self.load_high_voltage_reconnect.id(), channel.clone());
        publisher.writes(self.lvd_load_current_compensation.id(), channel.clone());
        publisher.writes(self.lvd_warning_timeout.id(), channel.clone());
        publisher.writes(self.led_green_to_green_and_yellow_limit.id(), channel.clone());
        publisher.writes(self.led_green_and_yellow_to_yellow_limit.id(), channel.clone());
        publisher.writes(self.led_yellow_to_yellow_and_red_limit.id(), channel.clone());
        publisher.writes(self.led_yellow_and_red_to_red_flashing_limit.id(), channel.clone());
        publisher.writes(self.modbus_id.id(), channel.clone());
        publisher.writes(self.meterbus_id.id(), channel.clone());
        publisher.writes(self.mppt_fixed_vmp.id(), channel.clone());
        publisher.writes(self.mppt_fixed_vmp_percent.id(), channel.clone());
        publisher.writes(self.charge_current_limit.id(), channel);
    }

    fn process_writes(&self, mut batch: Pooled<Vec<WriteRequest>>, p: &mut Settings) {
        for r in batch.drain(..) {
            if r.id == self.regulation_voltage.id() {
                p.regulation_voltage = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.float_voltage.id() {
                p.float_voltage = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.time_before_float.id() {
                p.time_before_float = Time::new::<second>(f32!(r));
            } else if r.id == self.time_before_float_low_battery.id() {
                p.time_before_float_low_battery = Time::new::<second>(f32!(r));
            } else if r.id == self.float_low_battery_voltage_trigger.id() {
                p.float_low_battery_voltage_trigger =
                    ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.float_cancel_voltage.id() {
                p.float_cancel_voltage = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.exit_float_time.id() {
                p.exit_float_time = Time::new::<minute>(f32!(r));
            } else if r.id == self.equalize_voltage.id() {
                p.equalize_voltage = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.days_between_equalize_cycles.id() {
                p.days_between_equalize_cycles = Time::new::<day>(f32!(r));
            } else if r.id == self.equalize_time_limit_above_regulation_voltage.id() {
                p.equalize_time_limit_above_regulation_voltage =
                    Time::new::<minute>(f32!(r));
            } else if r.id == self.equalize_time_limit_at_regulation_voltage.id() {
                p.equalize_time_limit_at_regulation_voltage =
                    Time::new::<minute>(f32!(r));
            } else if r.id == self.alarm_on_setting_change.id() {
                p.alarm_on_setting_change = match r.value {
                    Value::True => true,
                    Value::False => false,
                    v => {
                        let m = format!("{:?} not accepted, expected bool", v);
                        warn!("{}", &m);
                        if let Some(reply) = r.send_result {
                            reply.send(Value::Error(Chars::from(m)));
                        }
                        continue;
                    }
                }
            } else if r.id == self.reference_charge_voltage_limit.id() {
                p.reference_charge_voltage_limit =
                    ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.battery_charge_current_limit.id() {
                p.battery_charge_current_limit = ElectricCurrent::new::<ampere>(f32!(r));
            } else if r.id == self.temperature_compensation_coefficent.id() {
                p.temperature_compensation_coefficent =
                    ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.high_voltage_disconnect.id() {
                p.high_voltage_disconnect = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.high_voltage_reconnect.id() {
                p.high_voltage_reconnect = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.maximum_charge_voltage_reference.id() {
                p.maximum_charge_voltage_reference =
                    ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.max_battery_temp_compensation_limit.id() {
                p.max_battery_temp_compensation_limit =
                    ThermodynamicTemperature::new::<degree_celsius>(f32!(r));
            } else if r.id == self.min_battery_temp_compensation_limit.id() {
                p.min_battery_temp_compensation_limit =
                    ThermodynamicTemperature::new::<degree_celsius>(f32!(r));
            } else if r.id == self.load_low_voltage_disconnect.id() {
                p.load_low_voltage_disconnect = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.load_low_voltage_reconnect.id() {
                p.load_low_voltage_reconnect = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.load_high_voltage_disconnect.id() {
                p.load_high_voltage_disconnect = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.load_high_voltage_reconnect.id() {
                p.load_high_voltage_reconnect = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.lvd_load_current_compensation.id() {
                p.lvd_load_current_compensation =
                    ElectricalResistance::new::<ohm>(f32!(r));
            } else if r.id == self.lvd_warning_timeout.id() {
                p.lvd_warning_timeout = Time::new::<second>(f32!(r));
            } else if r.id == self.led_green_to_green_and_yellow_limit.id() {
                p.led_green_to_green_and_yellow_limit =
                    ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.led_green_and_yellow_to_yellow_limit.id() {
                p.led_green_and_yellow_to_yellow_limit =
                    ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.led_yellow_to_yellow_and_red_limit.id() {
                p.led_yellow_to_yellow_and_red_limit =
                    ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.led_yellow_and_red_to_red_flashing_limit.id() {
                p.led_yellow_and_red_to_red_flashing_limit =
                    ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.modbus_id.id() {
                p.modbus_id = match r.value {
                    Value::U32(v) => v as u8,
                    v => {
                        let m = format!("{:?} was not accepted, expected U32", v);
                        warn!("{}", &m);
                        if let Some(reply) = r.send_result {
                            reply.send(Value::Error(Chars::from(m)));
                        }
                        continue;
                    }
                };
            } else if r.id == self.meterbus_id.id() {
                p.meterbus_id = match r.value {
                    Value::U32(v) => v as u8,
                    v => {
                        let m = format!("{:?} was not accepted, expected U32", v);
                        warn!("{}", &m);
                        if let Some(reply) = r.send_result {
                            reply.send(Value::Error(Chars::from(m)));
                        }
                        continue;
                    }
                };
            } else if r.id == self.mppt_fixed_vmp.id() {
                p.mppt_fixed_vmp = ElectricPotential::new::<volt>(f32!(r));
            } else if r.id == self.mppt_fixed_vmp_percent.id() {
                p.mppt_fixed_vmp_percent = f32!(r);
            } else if r.id == self.charge_current_limit.id() {
                p.charge_current_limit = ElectricCurrent::new::<ampere>(f32!(r));
            } else {
                let m = format!("unknown settings field {:?}", r.id);
                warn!("{}", &m);
                if let Some(reply) = r.send_result {
                    reply.send(Value::Error(Chars::from(m)))
                }
            }
        }
    }
}

struct PublishedControl {
    charging: Val,
    load: Val,
    reset: Val,
}

impl PublishedControl {
    fn new(publisher: &Publisher, base: &Path) -> Result<Self> {
        Ok(PublishedControl {
            charging: publisher.publish(base.append("charging"), Value::Null)?,
            load: publisher.publish(base.append("load"), Value::Null)?,
            reset: publisher.publish(base.append("reset"), Value::Null)?,
        })
    }

    fn update(&self, batch: &mut UpdateBatch, st: &Stats) {
        self.charging.update_changed(
            batch,
            match st.charge_state {
                ChargeState::Disconnect | ChargeState::Fault => Value::False,
                ChargeState::UnknownState(_)
                | ChargeState::Absorption
                | ChargeState::BulkMPPT
                | ChargeState::Equalize
                | ChargeState::Fixed
                | ChargeState::Float
                | ChargeState::Night
                | ChargeState::NightCheck
                | ChargeState::Start
                | ChargeState::Slave => Value::True,
            },
        );
        self.load.update_changed(
            batch,
            match st.load_state {
                LoadState::Disconnect | LoadState::Fault | LoadState::LVD => Value::False,
                LoadState::LVDWarning
                | LoadState::Normal
                | LoadState::NormalOff
                | LoadState::NotUsed
                | LoadState::Override
                | LoadState::Start
                | LoadState::Unknown(_) => Value::True,
            },
        );
    }

    fn register_writable(&self, publisher: &Publisher, channel: fmpsc::Sender<Pooled<Vec<WriteRequest>>>) {
        publisher.writes(self.charging.id(), channel.clone());
        publisher.writes(self.load.id(), channel.clone());
        publisher.writes(self.reset.id(), channel);
    }

    fn process_writes(&self, mut batch: Pooled<Vec<WriteRequest>>) -> Vec<FromClient> {
        batch
            .drain(..)
            .filter_map(|r| {
                if r.id == self.charging.id() {
                    Some(FromClient::SetCharging(bool!(r)))
                } else if r.id == self.load.id() {
                    Some(FromClient::SetLoad(bool!(r)))
                } else if r.id == self.reset.id() {
                    Some(FromClient::ResetController)
                } else {
                    let m = format!("control id {:?} not recognized", r.id);
                    warn!("{}", &m);
                    if let Some(reply) = r.send_result {
                        reply.send(Value::Error(Chars::from(m)));
                    }
                    None
                }
            })
            .collect()
    }
}

struct NetidxInner {
    publisher: Publisher,
    stats: PublishedStats,
    settings: PublishedSettings,
    control: PublishedControl,
    current: Option<Settings>,
    to_main: Sender<ToMainLoop>,
}

#[derive(Clone)]
pub(crate) struct Netidx(Arc<Mutex<NetidxInner>>);

impl Netidx {
    async fn handle_writes(self) {
        let (settings_tx, settings_rx) = fmpsc::channel(10);
        let (control_tx, control_rx) = fmpsc::channel(10);
        {
            let inner = self.0.lock();
            inner.settings.register_writable(&inner.publisher, settings_tx);
            inner.control.register_writable(&inner.publisher, control_tx);
        }
        let mut settings_rx = settings_rx.fuse();
        let mut control_rx = control_rx.fuse();
        'main: loop {
            select_biased! {
                m = control_rx.next() => match m {
                    None => break,
                    Some(batch) => {
                        let (to_main, commands) = {
                            let inner = self.0.lock();
                            (inner.to_main.clone(), inner.control.process_writes(batch))
                        };
                        for cmd in commands {
                            let (reply_tx, mut reply_rx) = mpsc::channel(1);
                            let m = ToMainLoop::FromClient(cmd, reply_tx);
                            match to_main.send(m).await {
                                Err(_) => break 'main,
                                Ok(()) => match reply_rx.recv().await {
                                    None => break 'main,
                                    Some(_) => ()
                                }
                            }
                        }
                    }
                },
                m = settings_rx.next() => match m {
                    None => break,
                    Some(batch) => {
                        let (to_main, s) = {
                            let inner = self.0.lock();
                            let mut s = match inner.current.as_ref() {
                                Some(settings) => *settings,
                                None => {
                                    warn!("settings are not initialized");
                                    continue;
                                }
                            };
                            inner.settings.process_writes(batch, &mut s);
                            (inner.to_main.clone(), s)
                        };
                        let (reply_tx, mut reply_rx) = mpsc::channel(1);
                        let msg =
                            ToMainLoop::FromClient(FromClient::WriteSettings(s), reply_tx);
                        match to_main.send(msg).await {
                            Err(_) => break,
                            Ok(()) => (),
                        }
                        match reply_rx.recv().await {
                            None => break,
                            Some(ToClient::Err(e)) =>
                                warn!("failed to update settings {}", e),
                            Some(ToClient::Ok) => {
                                let mut inner = self.0.lock();
                                inner.current = Some(s);
                                info!("settings updated successfully");
                            }
                            Some(_) => {
                                warn!("unexpected response from main loop");
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn new(cfg: &Config, to_main: Sender<ToMainLoop>) -> Result<Self> {
        let resolver = task::block_in_place(|| netidx::config::Config::load_default())?;
        let bindcfg = cfg.netidx_bind.parse::<BindCfg>()?;
        let base = Path::from(cfg.netidx_base.clone());
        let auth = cfg
            .netidx_spn
            .clone()
            .map(|spn| DesiredAuth::Krb5 { spn: Some(spn), upn: None })
            .unwrap_or(DesiredAuth::Anonymous);
        info!("create publisher");
        let publisher = Publisher::new(resolver, auth, bindcfg).await?;
        info!("created publisher");
        let stats = PublishedStats::new(&publisher, &base.append("stats"))?;
        let settings = PublishedSettings::new(&publisher, &base.append("settings"))?;
        let control = PublishedControl::new(&publisher, &base.append("control"))?;
        info!("published stats, settings, control");
        let t = Netidx(Arc::new(Mutex::new(NetidxInner {
            publisher,
            stats,
            settings,
            control,
            current: None,
            to_main,
        })));
        task::spawn(t.clone().handle_writes());
        Ok(t)
    }

    pub(crate) fn start_batch(&self) -> UpdateBatch {
        self.0.lock().publisher.start_batch()
    }

    pub(crate) fn update_stats(&self, batch: &mut UpdateBatch, st: &Stats) {
        let inner = self.0.lock();
        info!("stats updated");
        inner.stats.update(batch, st);
    }

    pub(crate) fn update_settings(&self, batch: &mut UpdateBatch, set: &Settings) {
        let mut inner = self.0.lock();
        info!("settings updated");
        inner.current = Some(*set);
        inner.settings.update(batch, set);
    }

    pub(crate) fn update_control(&self, batch: &mut UpdateBatch, st: &Stats) {
        let inner = self.0.lock();
        info!("control stats updated");
        inner.control.update(batch, st);
    }

    pub(crate) async fn flush(&self, timeout: Duration, batch: UpdateBatch) {
        info!("publisher flush");
        batch.commit(Some(timeout)).await;
        info!("publisher flushed");
    }
}
