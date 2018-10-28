extern crate simple_logger;
extern crate nix;
extern crate daemonize;
extern crate morningstar;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate log;
extern crate syslog;
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
    FatalError {thread: String, msg: String},
    FromClient(FromClient),
    Hup,
    Tick
}

pub(crate) fn current_thread() -> String {
    thread::current().name()
        .map(|s| s.to_owned())
        .unwrap_or_else(|| "<anonymous>".to_owned())
}

macro_rules! or_fatal {
    ($to_main:ident, $e:expr, $msg:expr) => {
        match $e {
            Ok(r) => r,
            Err(e) => {
                let thread = current_thread();
                match $to_main.send(ToMain::FatalError {thread, msg: format!($msg, e)}) {
                    Ok(()) => (),
                    Err(f) => error!(
                        "thread {} failed to send fatal error to main: {}",
                        format!($msg, e)
                    )
                }
                return
            }
        }
    };
    ($e:expr, $msg:expr) => {
        match $e {
            Ok(r) => r,
            Err(e) => {
                error!($msg, current_thread(), e);
                return
            }
        }
    }
}

fn signal(to_main: Sender<ToMainLoop>) {
    thread::Builder::new().name("signal").stack_size(256).spawn(move || {
        let mut sigset = SigSet::empty();
        sigset.add(Signal::SIGHUP);
        or_fatal!(to_main, sigset.thread_unblock(), "failed to set sigmask {}");
        loop {
            or_fatal!(to_main, sigset.wait(), "error in sigwait {}");
            or_fatal!(to_main.send(ToMainLoop::Hup), "thread {} failed to send to main {}")
        }
    })
}

fn ticker(cfg: &Config, to_main: Sender<ToMainLoop>) {
    let interval = Duration::from_millis(cfg.stats_interval);
    thread::Builder::new().name("ticker").stack_size(256).spawn.(move || loop {
        or_fatal!(to_main.send(ToMainLoop::Tick), "thread {} failed to send to main {}");
        thread::sleep(interval);
    })
}

fn stats_writer(cfg: &Config, to_main: Sender<ToMainLoop>) -> Sender<ps::Stats> {
    let (sender, receiver) = channel();
    let path = cfg.stats_log.clone();
    thread::Builder::new().name("stats").stack_size(4096).spawn(move || {
        let log = or_fatal!(
            to_main,
            fs::OpenOptions::new().write(true).append(true).create(true).open(&path),
            "failed to open stats log {}"
        );
        for st in receiver.iter() {
            or_fatal!(to_main, serde_json::to_writer(&log, &st), "failed to write stats {}");
            or_fatal!(to_main, write!(&log, "\n"), "failed to write record sep {}");
            or_fatal!(to_main.send(ToMainLoop::StatsLogged), "thread {} failed to send to main {}");
        }
    });
    sender
}

fn run_server(config: Config) {
    let mut sigmask = SigSet::empty();
    sigmask.add(Signal::SIGHUP);
    or_fatal!(sigmask.thread_block(), "{} pthread_sigmask failed {}");
    let (to_main, receiver) = channel();
    signal(to_main.clone());
    ticker(&config, to_main.clone());
    let mut stats_sink = stats_writer(&config, to_main.clone());
    let mb = modbus_loop::start(&config, to_main.clone());
    control_socket::run_server(&config, to_main.clone());;

    let mut last_stats_written = Instant::now();
    for msg in receiver.iter() {
        match msg {
            ToMainLoop::Stats(s) => or_fatal!(stats_sink.send(s), "{} failed to send stats"),
            ToMainLoop::StatsLogged => last_stats_written = Instant::now(),
            ToMainLoop::FromClient(msg) => {
                let m = match msg {
                    FromClient::SetChargingEnabled(b) =>
                        modbus_loop::Command::Coil(ps::Coil::ChargeDisconnect, !b),
                    FromClient::SetLoadEnabled(b) =>
                        modbus_loop::Command::Coil(ps::Coil::LoadDisconnect, !b),
                    FromClient::ResetController =>
                        modbus_loop::Command::Coil(ps::Coil::ResetControl, true)
                };
                or_fatal!(mb.send(m), "{} failed to send msg to modbus {}");
            },
            ToMainLoop::Hup => stats_sink = stats_writer(&config, to_main.clone()),
            ToMainLoop::Tick => {
                let timeout = Duration::from_millis(config.stats_interval * 4);
                let elapsed = last_stats_written.elapsed();
                if elapsed > timeout { warn!("stats logging is delayed {}", elapsed) }
                or_fatal!(mb.send(modbus_loop::Command::Stats),
                          "{} failed to send to modbus {}")
            },
            ToMainLoop::FatalError {thread, msg} => {
                error!("fatal error {} in thread {}, exiting", msg, thread);
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
                syslog::init(syslog::Facility::LOG_DAEMON, syslog::LevelFilter::Trace, Some("solar"))
                    .expect("failed to init syslog");
                let d = Daemonize::new().pid_file(&config.pid_file);
                match d.start() {
                    Ok(()) => run_server(config),
                    Err(e) => panic!("failed to daemonize: {}", e)
                }
            } else {
                simple_logger::init().expect("failed to init simple logger");
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
