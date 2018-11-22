#![feature(await_macro, async_await, futures_api)]
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate log;
#[macro_use] extern crate tokio;

mod modbus_loop;
mod control_socket;

use daemonize::Daemonize;
use structopt::StructOpt;
use morningstar::prostar_mppt as ps;
use std::{
    thread, sync::mpsc::{Sender, channel},
    io::{Write, LineWriter}, fs, time::{Duration, Instant}
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
    Stop,
    TailStats,
    GetSettings
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum ToClient {
    Stats(ps::Stats),
    Settings(ps::Settings),
}

#[derive(Debug, Clone)]
pub(crate) enum ToMainLoop {
    Stats(ps::Stats),
    Settings(ps::Settings),
    StatsLogged,
    FatalError {thread: String, msg: String},
    FromClient(FromClient, Sender<ToClient>),
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

fn log_writer(cfg: &Config, to_main: Sender<ToMainLoop>) -> Sender<ps::Stats> {
    let (sender, receiver) = channel();
    let path = cfg.stats_log.clone();
    thread::Builder::new().name("stats".into()).stack_size(4096).spawn(move || {
        let log = or_fatal!(
            to_main,
            fs::OpenOptions::new().write(true).append(true).create(true).open(&path),
            "failed to open stats log {}"
        );
        let mut log = LineWriter::new(log);
        for st in receiver.iter() {
            or_fatal!(to_main, serde_json::to_writer(log.by_ref(), &st), "failed to write stats {}");
            or_fatal!(to_main, write!(log.by_ref(), "\n"), "failed to write record sep {}");
            or_fatal!(to_main.send(ToMainLoop::StatsLogged), "thread {} failed to send to main {}");
        }
    }).unwrap();
    sender
}

fn run_server(config: Config) {
    let (to_main, receiver) = channel();
    ticker(&config, to_main.clone());
    let mut log_sink = log_writer(&config, to_main.clone());
    let mb = modbus_loop::start(&config, to_main.clone());
    control_socket::run_server(&config, to_main.clone());;

    let mut last_stats_written = Instant::now();
    let mut tailing : Vec<Sender<ToClient>> = Vec::new();
    let mut wait_settings: Vec<Sender<ToClient>> = Vec::new();
    for msg in receiver.iter() {
        match msg {
            ToMainLoop::Stats(s) => {
                or_fatal!(log_sink.send(s), "{} failed to send stats {}");
                let mut i = 0;
                while i < tailing.len() {
                    match tailing[i].send(ToClient::Stats(s)) {
                        Ok(()) => i += 1,
                        Err(_) => { tailing.remove(i); }
                    }
                }
            },
            ToMainLoop::Settings(s) => {
                for sender in wait_settings.drain(0..) {
                    let _ = sender.send(ToClient::Settings(s));
                }
            },
            ToMainLoop::StatsLogged => last_stats_written = Instant::now(),
            ToMainLoop::FromClient(msg, reply) => match msg {
                FromClient::SetChargingEnabled(b) => or_fatal!(
                    mb.send(modbus_loop::Command::Coil(ps::Coil::ChargeDisconnect, !b)),
                    "{} failed to send modbus command {}"),
                FromClient::SetLoadEnabled(b) => or_fatal!(
                    mb.send(modbus_loop::Command::Coil(ps::Coil::LoadDisconnect, !b)),
                    "{} failed to send modbus command {}"),
                FromClient::ResetController => or_fatal!(
                    mb.send(modbus_loop::Command::Coil(ps::Coil::ResetControl, true)),
                    "{} failed to send modbus command {}"),
                FromClient::LogRotated => log_sink = log_writer(&config, to_main.clone()),
                FromClient::TailStats => tailing.push(reply),
                FromClient::GetSettings => {
                    wait_settings.push(reply);
                    if wait_settings.len() == 1 {
                        or_fatal!(
                            mb.send(modbus_loop::Command::Settings),
                            "{} failed to send modbus command {}");
                    }
                },
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
    #[structopt(name = "cancel-float")]
    CancelFloat,
    #[structopt(name = "log-rotated")]
    LogRotated,
    #[structopt(name = "reset-controller")]
    ResetController,
    #[structopt(name = "tail-stats")]
    TailStats {
        #[structopt(short = "j", long = "json")]
        json: bool
    },
    #[structopt(name = "get-settings")]
    GetSettings {
        #[structopt(short = "j", long = "json")]
        json: bool
    },
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
    use std::iter::once;
    match opt.cmd {
        SubCommand::Start {daemonize} => {
            if daemonize {
                syslog::init(syslog::Facility::LOG_DAEMON, log::LevelFilter::Warn, Some("solar"))
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
            control_socket::send_command(&config, once(FromClient::Stop))
            .expect("failed to stop the daemon"),
        SubCommand::DisableLoad =>
            control_socket::send_command(&config, once(FromClient::SetLoadEnabled(false)))
            .expect("failed to disable load. Is the daemon running?"),
        SubCommand::EnableLoad =>
            control_socket::send_command(&config, once(FromClient::SetLoadEnabled(true)))
            .expect("failed to enable load. Is the daemon running?"),
        SubCommand::DisableCharging =>
            control_socket::send_command(&config, once(FromClient::SetChargingEnabled(false)))
            .expect("failed to disable charging. Is the daemon running?"),
        SubCommand::EnableCharging =>
            control_socket::send_command(&config, once(FromClient::SetChargingEnabled(true)))
            .expect("failed to enable charging. Is the daemon running?"),
        SubCommand::CancelFloat =>
            control_socket::send_command(&config, &[
                FromClient::SetChargingEnabled(false),
                FromClient::SetChargingEnabled(true)
            ]).expect("failed to cancel float"),
        SubCommand::ResetController =>
            control_socket::send_command(&config, once(FromClient::ResetController))
            .expect("failed to reset the controller"),
        SubCommand::LogRotated =>
            control_socket::send_command(&config, once(FromClient::LogRotated))
            .expect("failed to reopen log file"),
        SubCommand::TailStats {json} => {
            let (send, recv) = channel();
            control_socket::send_query(&config, send, FromClient::TailStats);
            for m in recv.iter() {
                match m {
                    ToClient::Settings(_) => panic!("unexpected response"),
                    ToClient::Stats(s) => {
                        if json { println!("{}", serde_json::to_string_pretty(&s).unwrap()) }
                        else { println!("{}", s) }
                    }
                }
            }
        },
        SubCommand::GetSettings {json} => {
            let (send, recv) = channel();
            control_socket::send_query(&config, send, FromClient::GetSettings);
            match recv.recv().unwrap() {
                ToClient::Stats(_) => panic!("unexpected response"),
                ToClient::Settings(s) => {
                    if json { println!("{}", serde_json::to_string_pretty(&s).unwrap()) }
                    else { println!("{}", s) }
                },
            }
        }
    }
}
