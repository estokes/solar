#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate error_chain;

use morningstar::prostar_mppt as ps;
use std::{
    borrow::Borrow,
    fs,
    io::{self, BufRead, BufReader, LineWriter, Write},
    iter::Iterator,
    os::unix::net::UnixStream,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FromClient {
    SetChargingEnabled(bool),
    SetLoadEnabled(bool),
    ResetController,
    LogRotated,
    Stop,
    TailStats,
    ReadSettings,
    WriteSettings(ps::Settings),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToClient {
    Stats(ps::Stats),
    Settings(ps::Settings),
    Ok,
    Err(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub device: String,
    pub modbus_id: u8,
    pub pid_file: String,
    pub stats_log: String,
    pub control_socket: String,
    pub stats_interval: u64,
}

error_chain! {
    foreign_links {
        SerdeJson(serde_json::Error);
        Io(io::Error);
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
    let con = UnixStream::connect(&cfg.control_socket)?;
    let mut writer = LineWriter::new(con.try_clone()?);
    let mut reader = BufReader::new(con);
    let mut line = String::new();
    for cmd in cmds {
        serde_json::to_writer(writer.by_ref(), cmd.borrow())?;
        write!(writer.by_ref(), "\n")?;
        line.clear();
        reader.read_line(&mut line).unwrap();
        match serde_json::from_str(&line)? {
            ToClient::Ok => (),
            ToClient::Err(e) => bail!(e),
            ToClient::Settings(_) | ToClient::Stats(_) => bail!("got unexpected command reply"),
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
    let socket_path = cfg.control_socket.clone();
    let con = UnixStream::connect(&socket_path).unwrap();
    let mut writer = LineWriter::new(con.try_clone()?);
    serde_json::to_writer(writer.by_ref(), &q)?;
    write!(writer.by_ref(), "\n").unwrap();
    Ok(Query {
        reader: BufReader::new(con),
        line: String::new(),
    })
}
