use libflate::gzip::Encoder;
use chrono::{prelude::*, Duration};
use solar_client::{self, ArchivedDay, Config, FromClient, Stats};
use std::{
    fs::{self, OpenOptions},
    io::{self, BufRead, LineWriter, Write},
    iter::once,
    path::{Path, PathBuf},
};
use morningstar::prostar_mppt as ps;

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

fn open_archive(path: &Path) -> LineWriter<Encoder<fs::File>> {
    LineWriter::new(
        Encoder::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(path)
                .expect("failed to open archive file"),
        )
        .expect("failed to create gzip encoder {}"),
    )
}

fn update_accum(
    acc: Option<(DateTime<Local>, Stats)>,
    s: Stats,
    cutoff: Duration,
    writer: &mut LineWriter<Encoder<fs::File>>,
) -> Option<(DateTime<Local>, Stats)> {
    match acc {
        None => Some((s.timestamp(), s)),
        Some((ts, mut acc)) => {
            stats_accum(&mut acc, &s);
            if acc.timestamp() - ts < cutoff {
                Some((ts, acc))
            } else {
                serde_json::to_writer(writer.by_ref(), &acc).expect("failed to write stats");
                write!(writer, "\n").expect("failed to write newline");
                None
            }
        }
    }
}

fn do_archive_log_file(file: &Path, archive: &ArchivedDay) {
    let one_minute = Duration::seconds(60);
    let ten_minutes = Duration::seconds(600);
    let mut enc = open_archive(&archive.all);
    let mut enc_1m = open_archive(&archive.one_minute_averages);
    let mut enc_10m = open_archive(&archive.ten_minute_averages);
    let mut acc_1m: Option<(DateTime<Local>, Stats)> = None;
    let mut acc_10m: Option<(DateTime<Local>, Stats)> = None;
    for line in io::BufReader::new(fs::File::open(file).expect("open tmp")).lines() {
        let line = line.expect("error reading log file");
        let s = match serde_json::from_str::<Stats>(&line) {
            Ok(s) => s,
            Err(_) => Stats::V0(serde_json::from_str::<ps::Stats>(&line).expect("malformed log"))
        };
        acc_1m = update_accum(acc_1m, s, one_minute, &mut enc_1m);
        acc_10m = update_accum(acc_10m, s, ten_minutes, &mut enc_10m);
        serde_json::to_writer(enc.by_ref(), &s).expect("failed to encode");
        write!(&mut enc, "\n").expect("failed to write newline");
    }
}

pub fn archive_log(cfg: &Config, file: Option<PathBuf>, date: Option<Date<Local>>) {
    let archive = cfg.archive_for_date(date.unwrap_or_else(|| Local::today()));
    let (file, is_current_log) = match file {
        None => (cfg.log_file(), true),
        Some(f) => {
            let is_current_log = f == cfg.log_file();
            (f, is_current_log)
        }
    };
    if archive.exists().expect("failed to test archive") {
        println!("one or more archive files already exist for today");
    } else {
        let file = {
            if is_current_log { file }
            else {
                let current = cfg.log_file();
                let mut tmp = current.clone();
                tmp.set_extension("tmp");
                fs::hard_link(&current, &tmp).expect("failed to create tmp file");
                fs::remove_file(&current).expect("failed to unlink current file");
                solar_client::send_command(&cfg, once(FromClient::LogRotated))
                    .expect("failed to reopen log file");
                tmp
            }
        };
        do_archive_log_file(&file, &archive);
        if is_current_log {
            fs::remove_file(&file).expect("failed to remove tmp file");
        }
    }
}
