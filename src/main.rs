extern crate simple_logger;
extern crate daemonize;
extern crate morningstar;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate log;
extern crate syslog;
extern crate libmodbus_rs;
#[macro_use] extern crate structopt;

macro_rules! or_fatal {
    ($to_main:ident, $e:expr, $msg:expr) => {
        match $e {
            Ok(r) => r,
            Err(e) => {
                let thread = current_thread();
                match $to_main.send(ToMainLoop::FatalError {thread, msg: format!($msg, e)}) {
                    Ok(()) => (),
                    Err(_) => error!(
                        "thread {} failed to send fatal error to main: {}",
                        current_thread(), format!($msg, e)
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

mod modbus_loop;
mod control_socket;

use daemonize::Daemonize;
use structopt::StructOpt;
use morningstar::prostar_mppt as ps;
use std::{
    thread, sync::mpsc::{Sender, channel},
    io::Write, fs, time::{Duration, Instant}
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Config {
    pub(crate) device: String,
    pub(crate) pid_file: String,
    pub(crate) stats_log: String,
    pub(crate) control_socket: String,
    pub(crate) stats_interval: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum FromClient {
    SetChargingEnabled(bool),
    SetLoadEnabled(bool),
    ResetController,
    LogRotated,
    Stop
}

#[derive(Debug, Clone)]
pub(crate) enum ToMainLoop {
    Stats(ps::Stats),
    StatsLogged,
    FatalError {thread: String, msg: String},
    FromClient(FromClient),
    Tick
}

pub(crate) fn current_thread() -> String {
    thread::current().name()
        .map(|s| s.to_owned())
        .unwrap_or_else(|| "<anonymous>".to_owned())
}

fn ticker(cfg: &Config, to_main: Sender<ToMainLoop>) {
    let interval = Duration::from_millis(cfg.stats_interval);
    thread::Builder::new().name("ticker".into()).stack_size(256).spawn(move || loop {
        or_fatal!(to_main.send(ToMainLoop::Tick), "thread {} failed to send to main {}");
        thread::sleep(interval);
    }).unwrap();
}

fn stats_writer(cfg: &Config, to_main: Sender<ToMainLoop>) -> Sender<ps::Stats> {
    let (sender, receiver) = channel();
    let path = cfg.stats_log.clone();
    thread::Builder::new().name("stats".into()).stack_size(4096).spawn(move || {
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
    }).unwrap();
    sender
}

fn run_server(config: Config) {
    let (to_main, receiver) = channel();
    ticker(&config, to_main.clone());
    let mut stats_sink = stats_writer(&config, to_main.clone());
    let mb = modbus_loop::start(&config, to_main.clone());
    control_socket::run_server(&config, to_main.clone());;

    let mut last_stats_written = Instant::now();
    for msg in receiver.iter() {
        match msg {
            ToMainLoop::Stats(s) => or_fatal!(stats_sink.send(s), "{} failed to send stats {}"),
            ToMainLoop::StatsLogged => last_stats_written = Instant::now(),
            ToMainLoop::FromClient(msg) => match msg {
                FromClient::SetChargingEnabled(b) => or_fatal!(
                    mb.send(modbus_loop::Command::Coil(ps::Coil::ChargeDisconnect, !b)),
                    "{} failed to send modbus command {}"),
                FromClient::SetLoadEnabled(b) => or_fatal!(
                    mb.send(modbus_loop::Command::Coil(ps::Coil::LoadDisconnect, !b)),
                    "{} failed to send modbus command {}"),
                FromClient::ResetController => or_fatal!(
                    mb.send(modbus_loop::Command::Coil(ps::Coil::ResetControl, true)),
                    "{} failed to send modbus command {}"),
                FromClient::LogRotated => stats_sink = stats_writer(&config, to_main.clone()),
                FromClient::Stop => break,
            },
            ToMainLoop::Tick => {
                let timeout = Duration::from_millis(config.stats_interval * 4);
                let elapsed = last_stats_written.elapsed();
                if elapsed > timeout { warn!("stats logging is delayed {:?}", elapsed) }
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
    EnableCharging,
    #[structopt(name = "log-rotated")]
    LogRotated,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "solar", about = "solar power management system")]
struct Options {
    #[structopt(short = "c", long = "config", default_value = "/etc/solar.conf")]
    config: String,
    #[structopt(subcommand)]
    cmd: SubCommand
}

fn main() {
    let opt = Options::from_args();
    let config : Config = {
        let f = fs::File::open(&opt.config).expect("failed to open config file");
        serde_json::from_reader(f).expect("failed to parse config file")
    };
    match opt.cmd {
        SubCommand::Start {daemonize} => {
            if daemonize {
                syslog::init(syslog::Facility::LOG_DAEMON, log::LevelFilter::Trace, Some("solar"))
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
        SubCommand::Stop =>
            control_socket::single_command(&config, FromClient::Stop)
            .expect("failed to stop the daemon"),
        SubCommand::DisableLoad =>
            control_socket::single_command(&config, FromClient::SetLoadEnabled(false))
            .expect("failed to disable load. Is the daemon running?"),
        SubCommand::EnableLoad =>
            control_socket::single_command(&config, FromClient::SetLoadEnabled(true))
            .expect("failed to enable load. Is the daemon running?"),
        SubCommand::DisableCharging =>
            control_socket::single_command(&config, FromClient::SetChargingEnabled(false))
            .expect("failed to disable charging. Is the daemon running?"),
        SubCommand::EnableCharging =>
            control_socket::single_command(&config, FromClient::SetChargingEnabled(true))
            .expect("failed to enable charging. Is the daemon running?"),
        SubCommand::LogRotated =>
            control_socket::single_command(&config, FromClient::LogRotated)
            .expect("failed to reopen log file")
    }
}
