use crate::{send_command, ArchivedDay, Config, FromClient, Stats};
use chrono::{prelude::*, Duration};
use libflate::{
    gzip::{Decoder, EncodeOptions, Encoder},
    lz77::DefaultLz77Encoder,
};
use std::{
    error,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{self, BufRead, LineWriter, Read, Write},
    iter::{self, Iterator},
    path::{Path, PathBuf},
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

pub fn stats_accum(acc: &mut Stats, s: &Stats) {
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

pub struct Decimate<I> {
    acc: Option<(DateTime<Local>, Stats)>,
    cutoff: Duration,
    iter: I,
}

impl<I> Iterator for Decimate<I>
where
    I: Iterator<Item = Stats>,
{
    type Item = Stats;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                None => return self.acc.take().map(|(_, s)| s),
                Some(st) => {
                    self.acc = match self.acc {
                        None => Some((st.timestamp(), st)),
                        Some((ts, mut acc)) => {
                            stats_accum(&mut acc, &st);
                            Some((ts, acc))
                        }
                    };
                    let (ts, acc) = self.acc.unwrap();
                    if acc.timestamp() - ts >= self.cutoff {
                        self.acc = None;
                        return Some(acc);
                    }
                }
            }
        }
    }
}

pub fn decimate<I>(cutoff: Duration, iter: I) -> Decimate<I>
where
    I: Iterator<Item = Stats>,
{
    Decimate {
        cutoff,
        iter,
        acc: None,
    }
}

fn open_archive(path: &Path) -> LineWriter<Encoder<fs::File>> {
    LineWriter::new(
        Encoder::with_options(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(path)
                .expect("failed to open archive file"),
            EncodeOptions::with_lz77(DefaultLz77Encoder::with_window_size(65534)),
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
                serde_json::to_writer(writer.by_ref(), &acc)
                    .expect("failed to write stats");
                write!(writer, "\n").expect("failed to write newline");
                None
            }
        }
    }
}

fn close_encoder(enc: LineWriter<Encoder<fs::File>>) {
    match enc.into_inner() {
        Ok(e) => {
            e.finish()
                .into_result()
                .expect("failed to close gzip stream");
        }
        Err(_) => panic!("close encoder failed"),
    }
}

fn read_history_file(
    file: PathBuf,
) -> Result<impl Iterator<Item = Stats>, Box<dyn error::Error>> {
    use serde_json::error::Category;
    let mut buf : BufReader<Box<dyn Read>> = io::BufReader::new({
        if &file == Path::new("-") {
            Box::new(io::stdin())
        } else {
            let f = fs::File::open(&file)?;
            if file.extension() == Some(OsStr::new("gz")) {
                Box::new(Decoder::new(f)?)
            } else {
                Box::new(f)
            }
        }
    });
    let mut sbuf = String::new();
    Ok(iter::from_fn(move || {
        match buf.by_ref().read_line(&mut sbuf) {
            Ok(_) => (),
            Err(e) => {
                error!("error reading line from log file {:?}, {}", file, e);
                return None
            },
        }
        match serde_json::from_str(&sbuf) {
            Ok(o) => {
                sbuf.clear();
                Some(o)
            }
            Err(e) => {
                match e.classify() {
                    Category::Io | Category::Eof => (),
                    Category::Syntax => error!(
                        "syntax error in log archive, parsing terminated: {:?}, {}",
                        file, e
                    ),
                    Category::Data => error!(
                        "semantic error in log archive, parsing terminated: {:?}, {}",
                        file, e
                    ),
                };
                None
            }
        }
    }))
}

fn do_archive_log_file(file: PathBuf, archive: &ArchivedDay) {
    let one_minute = Duration::seconds(60);
    let ten_minutes = Duration::seconds(600);
    let mut enc = open_archive(&archive.all);
    let mut enc_1m = open_archive(&archive.one_minute_averages);
    let mut enc_10m = open_archive(&archive.ten_minute_averages);
    let mut acc_1m: Option<(DateTime<Local>, Stats)> = None;
    let mut acc_10m: Option<(DateTime<Local>, Stats)> = None;
    for s in read_history_file(file).expect("failed to open archive file") {
        acc_1m = update_accum(acc_1m, s, one_minute, &mut enc_1m);
        acc_10m = update_accum(acc_10m, s, ten_minutes, &mut enc_10m);
        serde_json::to_writer(enc.by_ref(), &s).expect("failed to encode");
        write!(&mut enc, "\n").expect("failed to write newline");
    }
    close_encoder(enc);
    close_encoder(enc_1m);
    close_encoder(enc_10m);
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
            if !is_current_log {
                file
            } else {
                let current = cfg.log_file();
                let mut tmp = current.clone();
                tmp.set_extension("tmp");
                fs::hard_link(&current, &tmp).expect("failed to create tmp file");
                fs::remove_file(&current).expect("failed to unlink current file");
                send_command(&cfg, iter::once(FromClient::LogRotated))
                    .expect("failed to reopen log file");
                tmp
            }
        };
        do_archive_log_file(file.clone(), &archive);
        if is_current_log {
            fs::remove_file(&file).expect("failed to remove tmp file");
        }
    }
}

pub fn read_history(cfg: &Config, mut days: i64) -> impl Iterator<Item = Stats> + '_ {
    info!("read history going back {} days", days);
    let today = Local::today();
    iter::from_fn(move || {
        if days <= 0 {
            None
        } else {
            let d = Duration::days(days);
            days -= 1;
            today
                .checked_sub_signed(d)
                .map(|d| cfg.archive_for_date(d).ten_minute_averages)
                .and_then(|f| match read_history_file(f.clone()) {
                    Ok(i) => Some(i),
                    Err(e) => {
                        error!("error opening log archive, skipping: {:?}, {}", f, e);
                        None
                    }
                })
        }
    })
    .flatten()
    .chain(
        match read_history_file(cfg.log_file()) {
            Ok(i) => Some(decimate(Duration::seconds(600), i)),
            Err(e) => {
                error!("error opening todays log file, skipping: {}", e);
                None
            }
        }
        .into_iter()
        .flatten(),
    )
}
