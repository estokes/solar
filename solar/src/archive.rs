use morningstar::error as mse;
use morningstar::prostar_mppt as ps;
use solar_client::{self, Config, FromClient, Stats, ToClient};
use std::{
    fs,
    io::{self, LineWriter, Write},
    path::Path,
    sync::mpsc::{channel, Sender},
    thread,
    time::Duration,
};

macro_rules! avg {
    ($r0:ident, $r1:ident, $( $fld:ident ),*) => {
        $(
            $r0.$fld = ($r0.$fld + $r1.$fld) / 2.;
        )*
    }
}

macro_rules! max {
    ($r0:ident, $r1:ident, $( $fld: ident ),*) => {
        $(
            $r0.$fld = max($r0.$fld, $r1.$fld);
        )*
    }
}

macro_rules! maxf {
    ($r0:ident, $r1:ident, $( $fld: ident ),*) => {
        $(
            $r0.$fld = $r0.$fld.max($r1.$fld);
        )*
    }
}

fn stats_accum(acc: &mut Stats, s: &Stats) {
    use std::cmp::max;
    let acc = match acc {
        Stats::V0(ref mut s) => s,
    };
    let s = match s {
        Stats::V0(ref s) => s,
    };
    acc.battery_v_min_daily = acc.battery_v_min_daily.min(s.battery_v_min_daily);
    acc.rts_temperature = match (acc.rts_temperature, s.rts_temperature) {
        (None, None) => None,
        (Some(t), None) => Some(t),
        (None, Some(t)) => Some(t),
        (Some(t0), Some(t1)) => Some(t0.max(t1)),
    };
    maxf!(
        acc,
        s,
        heatsink_temperature,
        battery_temperature,
        ambient_temperature,
        u_inductor_temperature,
        v_inductor_temperature,
        w_inductor_temperature,
        ah_charge_resettable,
        ah_charge_total,
        kwh_charge_resettable,
        kwh_charge_total,
        ah_load_resettable,
        ah_load_total,
        battery_v_max_daily,
        ah_charge_daily,
        ah_load_daily,
        array_voltage_max_daily,
        hourmeter
    );
    max!(
        acc,
        s,
        timestamp,
        software_version,
        battery_voltage_settings_multiplier,
        array_faults,
        load_faults,
        alarms,
        array_faults_daily,
        load_faults_daily,
        alarms_daily,
        charge_state,
        load_state
    );
    avg!(
        acc,
        s,
        supply_3v3,
        supply_12v,
        supply_5v,
        gate_drive_voltage,
        battery_terminal_voltage,
        array_voltage,
        load_voltage,
        charge_current,
        array_current,
        load_current,
        battery_current_net,
        battery_sense_voltage,
        meterbus_voltage,
        battery_voltage_slow,
        target_voltage,
        lvd_setpoint,
        array_power,
        array_vmp,
        array_max_power_sweep,
        array_voc,
        array_voltage_fixed,
        array_voc_percent_fixed
    );
}

#[derive(Debug)]
struct UnexpectedObjectKind;

impl std::error::Error for UnexpectedObjectKind {}
impl std::fmt::Display for UnexpectedObjectKind {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(fmt, "expected a file, found something else")
    }
}

fn file_exists(path: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    match fs::metadata(path) {
        Ok(m) => {
            if m.is_file() {
                Ok(true)
            } else {
                Err(Box::new(UnexpectedObjectKind))
            }
        }
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(Box::new(e))
            }
        }
    }
}

pub fn rotate_log(cfg: &Config) {
    use libflate::gzip::Encoder;
    use std::{fs::OpenOptions, iter::once};
    let todays = cfg.archive_for_date(chrono::Utc::today());
    if file_exists(&todays).expect("error checking file exists") {
        println!("nothing to do");
    } else {
        let current = cfg.log_file();
        let mut tmp = current.clone();
        tmp.set_extension("tmp");
        fs::hard_link(&current, &tmp).expect("failed to create tmp file");
        fs::remove_file(&current).expect("failed to unlink current file");
        solar_client::send_command(&cfg, once(FromClient::LogRotated))
            .expect("failed to reopen log file");
        let mut encoder = Encoder::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&todays)
                .expect("failed to open archive file"),
        )
        .expect("failed to create gzip encoder");
        io::copy(
            &mut fs::File::open(&tmp).expect("failed to open tmp file"),
            &mut encoder,
        )
        .expect("failed to copy tmp file to the archive");
        fs::remove_file(&tmp).expect("failed to remove tmp file");
    }
}
