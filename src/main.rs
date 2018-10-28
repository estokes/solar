extern crate nix;
extern crate daemonize;
extern crate morningstar;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate log;
extern crate syslog;
#[macro_use] extern crate error_chain;
extern crate libmodbus_rs;
#[macro_use] extern crate structopt;

mod modbus_loop;
mod control_socket;

use nix::sys::signal::{Signal, SigSet};
use daemonize::Daemonize;
use structopt::StructOpt;
use morningstar::prostar_mppt as ps;
use libmodbus_rs::prelude as mb;
use control_socket::ToClient;
use std::{
    thread, sync::mpsc::{Sender, Receiver, channel}, io, fs,
    time::{Duration, Instant}
};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Config {
    pub(crate) device: String,
    pub(crate) pid_file: String,
    pub(crate) stats_log: String,
    pub(crate) control_socket: String,
    pub(crate) stats_interval: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum FromClient {
    SetChargingEnabled(bool),
    SetLoadEnabled(bool),
    ResetController,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ToMainLoop {
    Stats(ps::Stats),
    StatsLogged,
    FatalError(String),
    FromClient(FromClient),
    Hup,
    Tick
}

fn signal(to_main: Sender<ToMainLoop>) {
    thread::spawn(move || {
        let mut sigset = SigSet::empty();
        sigset.add(Signal::SIGHUP);
        sigset.thread_unblock().expect("signal thread failed to unblock signals");
        loop {
            let _ = sigset.wait().expect("error in sigwait");
            to_main.send(ToMainLoop::Hup).expect("failed to send to main")
        }
    })
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

fn send<T>(s: Sender<T>, m: T) {
    match s.send(m) {
        Ok(()) => (),
        Err(e) => {
            error!("failed to write to sender: {}", e);
            panic!("failed to write to sender: {}", e)
        }
    }
}

fn run_server(config: Config) {
    let mut sigmask = SigSet::empty();
    sigmask.add(Signal::SIGHUP);
    sigmask.thread_block().expect("pthread_sigmask failed");
    let (to_main, receiver) = channel();
    ticker(&config, to_main.clone());
    let mut stats_sink = stats_writer(&config, to_main.clone()).expect("failed to open stats log");
    let mb = modbus_loop::start(&config, to_main.clone()).expect("failed to connect to modbus");
    control_socket::run_server(&config, to_main.clone()).expect("failed to open control socket");
    
    let mut last_stats_written = Instant::now();
    for msg in receiver.iter() {
        match msg {
            ToMainLoop::Stats(s) => send(stats_sink, s),
            ToMainLoop::StatsLogged => last_stats_written = Instant::now(),
            ToMainLoop::FromClient(msg) =>
                match msg {
                    FromClient::SetChargingEnabled(b) => send(mb, modbus_loop::Command::Coil(ps::Coil::ChargeDisconnect, !b)),
                    FromClient::SetLoadEnabled(b) => send(mb, modbus_loop::Command::Coil(ps::Coil::LoadDisconnect, !b)),
                    FromClient::ResetController => send(mb, modbus_loop::Command::Coil(ps::Coil::ResetControl, true))
                },
            ToMainLoop::Hup => stats_sink = stats_writer(&config, to_main.clone()).expect("failed to open stats log"),
            ToMainLoop::Tick => {
                if last_stats_written.elapsed() > Duration::from_millis(config.stats_interval * 4) {
                    warn!("stats logging is delayed")
                }
                send(mb, modbus_loop::Command::Stats)
            },
            ToMainLoop::FatalError(s) => {
                error!("exiting on fatal error: {}", s);
                break
            }
        }
    }
}

#[derive(Debug, StructOpt)]
enum SubCommand {
    #[structopt(name = "start")]
    Start {
        #[structopt(short = "d", long = "daemonize")]
        daemonize: bool
    },
    #[structopt(name = "stop")]
    Stop,
    #[structopt(name = "disable-load")]
    DisableLoad,
    #[structopt(name = "enable-load")]
    EnableLoad,
    #[structopt(name = "disable-charging")]
    DisableCharging,
    #[structopt(name = "enable-charging")]
    EnableCharging
}

#[derive(Debug, StructOpt)]
#[structopt(name = "solar", about = "solar power management system")]
enum Options {
    #[structopt(short = "c", long = "config", default = "/etc/solar.conf")]
    config: String,
    #[structopt(subcommand)]
    cmd: SubCommand
}

fn main() {
    let opt = Options::from_args();
    let config : Config = {
        let f = File::open(&opt.config).expect("failed to open config file");
        serde_json::from_reader(f).expect("failed to parse config file")
    };
    match opt.cmd {
        SubCommand::Start {daemonize} => {
            if daemonize {
                let d = Daemonize::new().pid_file(&config.pid_file);
                match d.start() {
                    Ok(()) => run_server(config),
                    Err(e) => panic!("failed to daemonize: {}", e)
                }
            } else {
                run_server(config)
            }
        }
        SubCommand::Stop => unimplemented!(),
        SubCommand::DisableLoad =>
            control_socket::single_command(&config, ToClient::SetLoadEnabled(false))
            .expect("failed to disable load. Is the daemon running?"),
        SubCommand::EnableLoad =>
            control_socket::single_command(&config, ToClient::SetLoadEnabled(true))
            .expect("failed to enable load. Is the daemon running?"),
        SubCommand:DisableCharging =>
            control_socket::single_command(&config, ToClient::SetChargingEnabled(false))
            .expect("failed to disable charging. Is the daemon running?"),
        SubCommand::EnableCharging =>
            control_socket::single_command(&config, ToClient::SetChargingEnabled(true))
            .expect("failed to enable charging. Is the daemon running?")
    }
}
