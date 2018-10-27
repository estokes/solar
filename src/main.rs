extern crate morningstar;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate log;
extern crate syslog;
#[macro_use] extern crate error_chain;
extern crate libmodbus_rs;

use morningstar::prostar_mppt as ps;
use libmodbus_rs::prelude as mb;
use std::{thread, sync::mpsc::{Sender, Receiver, channel}, io, fs};

mod modbus_loop;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Config {
    pub(crate) device: String,
    pub(crate) pid_file: String,
    pub(crate) stats_log: String,
    pub(crate) daemonize: bool
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ToMainLoop {
    Stats(Result<ps::Stats, mb::Error>),
    SetCoil(Result<(), mb::Error>),
    Tick
}

fn stats_writer(cfg: &Config) -> Result<Sender<ps::Stats>, io::Error> {
    let log = fs::OpenOptions::new().write(true).append(true).create(true).open(&cfg.stats_log)?;
    let (sender, receiver) = channel();
    thread::spawn(move || {
        
    });
    Ok(sender)
}

fn main() {
    let config : Config = {
        let path = match args().nth(1) {
            Some(p) => p,
            None => "/etc/solar.conf".into()
        };
        let f = File::open(path).expect("failed to open config file");
        serde_json::from_reader(f).expect("failed to parse config file")
    };
}
