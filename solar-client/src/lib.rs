#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate anyhow;

use chrono::prelude::*;
use morningstar::prostar_mppt as ps;
use anyhow::{Result, Error};
use std::{
    borrow::Borrow,
    fmt, fs,
    io::{BufRead, BufReader, LineWriter, Write},
    iter::Iterator,
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
};

pub mod archive;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FromClient {
    SetCharging(bool),
    SetLoad(bool),
    ResetController,
    LogRotated,
    Stop,
    TailStats,
    ReadSettings,
    WriteSettings(ps::Settings),
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Phy {
    pub solar: bool,
    pub battery: bool,
    pub master: bool,
}

impl fmt::Display for Phy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "solar: {}\nbattery: {}\nmaster: {}\n",
            self.solar, self.battery, self.master
        )
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Stats {
    V0(ps::Stats),
    V1 {
        controller: ps::Stats,
        phy: Phy,
    },
    V2 {
        timestamp: chrono::DateTime<chrono::offset::Local>,
        controller: Option<ps::Stats>,
        phy: Phy,
    },
    V3 {
        timestamp: chrono::DateTime<chrono::offset::Local>,
        controller: Option<ps::Stats>,
    }
}

impl Stats {
    fn upgrade(self) -> Self {
        match self {
            Stats::V3 { .. } => self,
            Stats::V2 { timestamp, controller, phy: _ } => Stats::V3 {
                timestamp, controller
            },
            Stats::V1 { controller, phy: _ } => Stats::V3 {
                timestamp: controller.timestamp,
                controller: Some(controller),
            },
            Stats::V0(st) => Stats::V3 {
                timestamp: st.timestamp,
                controller: Some(st),
            },
        }
    }

    pub fn timestamp(&self) -> chrono::DateTime<chrono::offset::Local> {
        match self {
            Stats::V0(ref s) => s.timestamp,
            Stats::V1 { controller: ref c, .. } => c.timestamp,
            Stats::V2 { ref timestamp, .. } => *timestamp,
            Stats::V3 { ref timestamp, .. } => *timestamp,
        }
    }

    pub fn timestamp_mut(&mut self) -> &mut chrono::DateTime<chrono::offset::Local> {
        match self {
            Stats::V0(ref mut s) => &mut s.timestamp,
            Stats::V1 { controller: ref mut c, .. } => &mut c.timestamp,
            Stats::V2 { ref mut timestamp, .. } => timestamp,
            Stats::V3 { ref mut timestamp, .. } => timestamp,
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Stats::V0(s) => s.fmt(fmt),
            Stats::V1 { controller, phy } => {
                controller.fmt(fmt)?;
                phy.fmt(fmt)
            }
            Stats::V2 { timestamp, controller, phy } => {
                timestamp.fmt(fmt)?;
                match controller {
                    Some(s) => s.fmt(fmt)?,
                    None => write!(fmt, "controller off")?,
                }
                phy.fmt(fmt)
            }
            Stats::V3 { timestamp, controller } => {
                timestamp.fmt(fmt)?;
                match controller {
                    Some(s) => s.fmt(fmt),
                    None => write!(fmt, "controller off"),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToClient {
    Stats(Stats),
    Settings(ps::Settings),
    Ok,
    Err(String),
}

#[derive(Debug)]
struct UnexpectedObjectKind;

impl std::error::Error for UnexpectedObjectKind {}
impl std::fmt::Display for UnexpectedObjectKind {
    fn fmt(
        &self,
        fmt: &mut std::fmt::Formatter,
    ) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "expected a file, found something else")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedDay {
    pub all: PathBuf,
    pub one_minute_averages: PathBuf,
    pub ten_minute_averages: PathBuf,
}

impl ArchivedDay {
    fn file_exists(path: &Path) -> Result<bool> {
        match fs::metadata(path) {
            Ok(m) => {
                if m.is_file() {
                    Ok(true)
                } else {
                    Err(Error::from(UnexpectedObjectKind))
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(Error::from(e))
                }
            }
        }
    }

    pub fn exists(&self) -> std::result::Result<bool, Box<dyn std::error::Error>> {
        Ok(ArchivedDay::file_exists(&self.all)?
            || ArchivedDay::file_exists(&self.one_minute_averages)?
            || ArchivedDay::file_exists(&self.ten_minute_averages)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub device: String,
    pub modbus_id: u8,
    pub run_directory: PathBuf,
    pub archive_directory: PathBuf,
    pub stats_interval: u64,
    pub log_level: log::LevelFilter,
    pub netidx_base: String,
    pub netidx_bind: String,
    pub netidx_spn: Option<String>,
}

fn cat_paths(p0: impl AsRef<Path>, p1: impl AsRef<Path>) -> PathBuf {
    let mut buf = PathBuf::new();
    buf.push(p0);
    buf.push(p1);
    buf
}

impl Config {
    pub fn pid_file(&self) -> PathBuf {
        cat_paths(&self.run_directory, "solar.pid")
    }

    pub fn control_socket(&self) -> PathBuf {
        cat_paths(&self.run_directory, "control")
    }

    pub fn log_file(&self) -> PathBuf {
        cat_paths(&self.run_directory, "solar.log")
    }

    fn archive_for_date_pfx(&self, date: Date<Local>, pfx: &str) -> PathBuf {
        let d = date.format("%Y%m%d");
        cat_paths(&self.archive_directory, format!("solar.log-{}{}.gz", d, pfx))
    }

    pub fn archive_for_date(&self, date: Date<Local>) -> ArchivedDay {
        ArchivedDay {
            all: self.archive_for_date_pfx(date, ""),
            one_minute_averages: self.archive_for_date_pfx(date, "1m"),
            ten_minute_averages: self.archive_for_date_pfx(date, "10m"),
        }
    }
}

// panics if it can't load
pub fn load_config(path: Option<&str>) -> Config {
    let path = path.unwrap_or("/etc/solar.conf");
    let f = fs::File::open(path).expect("failed to open config file");
    serde_json::from_reader(f).expect("failed to parse config file")
}

pub fn send_command(
    cfg: &Config,
    cmds: impl IntoIterator<Item = impl Borrow<FromClient>>,
) -> Result<()> {
    let con = UnixStream::connect(&cfg.control_socket())?;
    let mut writer = LineWriter::new(con.try_clone()?);
    let mut reader = BufReader::new(con);
    let mut line = String::new();
    for cmd in cmds {
        serde_json::to_writer(writer.by_ref(), cmd.borrow())?;
        write!(writer.by_ref(), "\n")?;
        line.clear();
        reader.read_line(&mut line)?;
        match serde_json::from_str(&line)? {
            ToClient::Ok => (),
            ToClient::Err(e) => bail!(e),
            ToClient::Settings(_) | ToClient::Stats(_) => {
                bail!("got unexpected command reply")
            }
        }
    }
    Ok(())
}

struct Query {
    reader: BufReader<UnixStream>,
    line: String,
}

macro_rules! or_none {
    ($e:expr) => {
        match $e {
            Ok(x) => x,
            Err(_) => return None,
        }
    };
}

impl Iterator for Query {
    type Item = ToClient;

    fn next(&mut self) -> Option<Self::Item> {
        self.line.clear();
        or_none!(self.reader.read_line(&mut self.line));
        Some(or_none!(serde_json::from_str(&self.line)))
    }
}

pub fn send_query(cfg: &Config, q: FromClient) -> Result<impl Iterator<Item = ToClient>> {
    let socket_path = cfg.control_socket();
    let con = UnixStream::connect(&socket_path)?;
    let mut writer = LineWriter::new(con.try_clone()?);
    serde_json::to_writer(writer.by_ref(), &q)?;
    write!(writer.by_ref(), "\n")?;
    Ok(Query { reader: BufReader::new(con), line: String::new() })
}
