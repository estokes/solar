use libflate::gzip::Encoder;
use solar_client::{self, Config, FromClient, Stats};
use std::{
    fs::{self, OpenOptions},
    io,
    iter::once,
    path::Path,
    time::Duration,
};

const ONE_MINUTE: Duration = Duration::from_min(1.);
const TEN_MINUTES: Duration = Duration::from_min(10.);

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

fn open_archive(path: &Path) -> Encoder {
    Encoder::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .expect("failed to open archive file"),
    )
    .expect(format!("failed to create gzip encoder {}", path))
}

fn update_accum(
    acc: Option<(DateTime<Utc>, Stats)>,
    cutoff: Duration,
    writer: &mut LineWriter<Encoder>,
) -> Option<(DateTime<Utc>, Stats)> {
    match acc {
        None => Some((s.timestamp, s)),
        Some((ts, mut acc)) => {
            stats_accum(&mut acc, &s);
            if acc.timestamp - ts < cutoff {
                Some((ts, acc))
            } else {
                serde_json::to_writer(writer, acc).expect("failed to write stats");
                write!(writer, "\n");
                None
            }
        }
    };
}

pub fn rotate_log(cfg: &Config) {
    let today = chrono::Utc::today();
    let todays = cfg.archive_for_date(today);
    let todays_1m = cfg.archive_for_date_1m(today);
    let todays_10m = cfg.archive_for_date_10m(today);
    if file_exists(&todays).expect("error checking file exists")
        || file_exists(&todays_1m).expect("error checking file exists")
        || file_exists(&todays_10m).expect("error checking file exists")
    {
        println!("one or more archive files already exist for today");
    } else {
        let current = cfg.log_file();
        let mut tmp = current.clone();
        tmp.set_extension("tmp");
        fs::hard_link(&current, &tmp).expect("failed to create tmp file");
        fs::remove_file(&current).expect("failed to unlink current file");
        solar_client::send_command(&cfg, once(FromClient::LogRotated))
            .expect("failed to reopen log file");
        let mut todays_enc = io::LineWriter::new(open_archive(&todays));
        let mut todays_1m_enc = io::LineWriter::new(open_archive(&todays_1m));
        let mut todays_10m_enc = io::LineWriter::new(open_archive(&todays_10m));
        let mut logs = io::BufReader::new(fs::File::open(&tmp).expect("failed to open tmp file"));
        let mut acc_1m: Option<(DateTime<Utc>, Stats)> = None;
        let mut acc_10m: Option<(DateTime<Utc>, Stats)> = None;
        for line in logs.lines() {
            let s = serde_json::from_str::<Stats>(&line).expect("malformed log file");
            acc_1m = update_accum(acc_1m, ONE_MINUTE, &mut todays_1m_enc);
            acc_10m = update_accum(acc_10m, TEN_MINUTES, &mut todays_10m_enc);
            write!(&mut todays_enc, "{}\n", line).expect("failed to write line");
        }
        fs::remove_file(&tmp).expect("failed to remove tmp file");
    }
}
