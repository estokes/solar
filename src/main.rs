extern crate morningstar;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate log;
extern crate syslog;
#[macro_use] extern crate error_chain;
extern crate libmodbus_rs;

use morningstar::prostar_mppt as ps;
use libmodbus_rs::prelude as mb;
use std::{
    thread, sync::mpsc::{Sender, Receiver, channel}, io, fs,
    time::Duration
};

mod modbus_loop;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Config {
    pub(crate) device: String,
    pub(crate) pid_file: String,
    pub(crate) stats_log: String,
    pub(crate) control_socket: String,
    pub(crate) stats_interval: u32,
    pub(crate) daemonize: bool
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ToMainLoop {
    Stats(ps::Stats),
    CoilWasSet,
    StatsLogged,
    FatalError(String),
    Tick
}

fn ticker(cfg: &Config, to_main: Sender<ToMainLoop>) {
    let interval = Duration::from_millis(cfg.stats_interval);
    thread::spawn(move || loop {
        match to_main.send(ToMainLoop::Tick) {
            Ok(()) => thread::sleep(interval),
            Err(_) => break
        }
    })
}

fn stats_writer(cfg: &Config, to_main: Sender<ToMainLoop>) -> Result<Sender<ps::Stats>, io::Error> {
    let log = fs::OpenOptions::new().write(true).append(true).create(true).open(&cfg.stats_log)?;
    let (sender, receiver) = channel();
    thread::spawn(move || for st in receiver.iter() {        
        match serde_json::to_writer_pretty(&log, &st) {
            Ok(()) =>
                match to_main.send(ToMainLoop::StatsLogged) {
                    Ok(()) => (),
                    Err(_) => break
                },
            Err(e) => {
                let _ = to_main.send(ToMainLoop::FatalError(format!("failed to write stats {}", e)));
                break
            }
        }
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
