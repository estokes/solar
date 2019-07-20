#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;

macro_rules! log_fatal {
    ($e:expr, $m:expr, $a:expr) => {
        match $e {
            Ok(r) => r,
            Err(e) => {
                error!($m, e);
                $a
            }
        }
    };
}

mod control_socket;
mod modbus;
mod rpi;

use daemonize::Daemonize;
use morningstar::error as mse;
use morningstar::prostar_mppt as ps;
use solar_client::{self, archive, Config, FromClient, Stats, ToClient};
use std::{
    fs,
    io::{self, LineWriter, Write},
    sync::mpsc::{channel, Sender},
    thread,
    time::Duration,
};
use structopt::StructOpt;

#[derive(Debug, Clone)]
pub(crate) enum ToMainLoop {
    FromClient(FromClient, Sender<ToClient>),
    Tick,
}

fn ticker(cfg: &Config, to_main: Sender<ToMainLoop>) {
    let interval = Duration::from_millis(cfg.stats_interval);
    thread::Builder::new()
        .name("ticker".into())
        .stack_size(256)
        .spawn(move || loop {
            let _ = to_main.send(ToMainLoop::Tick);
            thread::sleep(interval);
        })
        .unwrap();
}

fn open_log(cfg: &Config) -> Result<LineWriter<fs::File>, io::Error> {
    let log = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&cfg.log_file())?;
    Ok(LineWriter::new(log))
}

fn send_reply(r: mse::Result<()>, s: Sender<ToClient>) {
    match r {
        Ok(()) => {
            let _ = s.send(ToClient::Ok);
        }
        Err(e) => {
            let _ = s.send(ToClient::Err(e.to_string()));
        }
    }
}

fn run_server(config: Config) {
    let (to_main, receiver) = channel();
    ticker(&config, to_main.clone());
    let mut log = log_fatal!(open_log(&config), "failed to open log {}", return);
    let mut mb = modbus::Connection::new(config.device.clone(), config.modbus_id);
    control_socket::run_server(&config, to_main);
    let mut tailing: Vec<Sender<ToClient>> = Vec::new();
    let mut last_st: Option<ps::Stats> = None;

    for msg in receiver.iter() {
        match msg {
            ToMainLoop::FromClient(msg, reply) => match msg {
                FromClient::SetCharging(b) => {
                    send_reply(mb.write_coil(ps::Coil::ChargeDisconnect, !b), reply)
                }
                FromClient::SetLoad(b) => {
                    send_reply(mb.write_coil(ps::Coil::LoadDisconnect, !b), reply)
                }
                FromClient::SetPhySolar(b) => {
                    mb.rpi_mut().set_solar(b);
                    let _ = reply.send(ToClient::Ok);
                }
                FromClient::SetPhyBattery(b) => {
                    mb.rpi_mut().set_battery(b);
                    let _ = reply.send(ToClient::Ok);
                }
                FromClient::SetPhyMaster(b) => {
                    if mb.rpi_mut().set_master(b) == b {
                        let _ = reply.send(ToClient::Ok);
                    } else {
                        let _ = reply.send(ToClient::Err(
                            "design rules prohibit setting the master relay".into(),
                        ));
                    }
                }
                FromClient::ResetController => {
                    send_reply(mb.write_coil(ps::Coil::ResetControl, true), reply)
                }
                FromClient::LogRotated => {
                    log = log_fatal!(open_log(&config), "failed to open log {}", break);
                    send_reply(Ok(()), reply)
                }
                FromClient::TailStats => tailing.push(reply),
                FromClient::WriteSettings(settings) => {
                    send_reply(mb.write_settings(&settings), reply)
                }
                FromClient::ReadSettings => match mb.read_settings() {
                    Ok(s) => {
                        let _ = reply.send(ToClient::Settings(s));
                    }
                    Err(e) => {
                        let _ = reply.send(ToClient::Err(e.to_string()));
                    }
                },
                FromClient::Stop => {
                    let _ = reply.send(ToClient::Ok);
                    thread::sleep(Duration::from_millis(200));
                    break;
                }
            },
            ToMainLoop::Tick => {
                if mb.rpi().master() && mb.rpi().battery() || last_st.is_some() {
                    let controller = {
                        if mb.rpi().master() && mb.rpi().battery() {
                            last_st = Some(log_fatal!(
                                mb.read_stats(),
                                "fatal: failed to read stats {}",
                                break
                            ));
                        }
                        last_st.unwrap()
                    };
                    let phy = solar_client::Phy {
                        solar: mb.rpi().solar(),
                        battery: mb.rpi().battery(),
                        master: mb.rpi().master(),
                    };
                    let st = Stats::V1 { controller, phy };
                    log_fatal!(
                        serde_json::to_writer(log.by_ref(), &st),
                        "fatal: failed to log stats {}",
                        break
                    );
                    log_fatal!(
                        write!(log.by_ref(), "\n"),
                        "fatal: failed to log stats {}",
                        break
                    );
                    let mut i = 0;
                    while i < tailing.len() {
                        match tailing[i].send(ToClient::Stats(st)) {
                            Ok(()) => i += 1,
                            Err(_) => {
                                tailing.remove(i);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, StructOpt)]
enum OnOff {
    #[structopt(name = "on")]
    On,
    #[structopt(name = "off")]
    Off,
}

impl OnOff {
    fn get(&self) -> bool {
        match self {
            OnOff::On => true,
            OnOff::Off => false,
        }
    }
}

#[derive(Debug, StructOpt)]
enum Phy {
    #[structopt(name = "solar", help = "enable/disable solar panels")]
    Solar(OnOff),
    #[structopt(name = "battery", help = "enable/disable battery")]
    Battery(OnOff),
    #[structopt(name = "master", help = "enable/disable master")]
    Master(OnOff),
}

#[derive(Debug, StructOpt)]
enum Settings {
    #[structopt(name = "read", help = "read charge controller settings")]
    Read {
        #[structopt(short = "j", long = "json")]
        json: bool,
    },
    #[structopt(name = "write", help = "write charge controller settings")]
    Write { file: String },
}

#[derive(Debug, StructOpt)]
enum SubCommand {
    #[structopt(name = "start")]
    Start {
        #[structopt(short = "d", long = "daemonize")]
        daemonize: bool,
    },
    #[structopt(name = "stop")]
    Stop,
    #[structopt(name = "load")]
    Load(OnOff),
    #[structopt(name = "charging")]
    Charging(OnOff),
    #[structopt(name = "phy")]
    Phy(Phy),
    #[structopt(name = "cancel-float")]
    CancelFloat,
    #[structopt(name = "archive", help = "archive todays log file")]
    ArchiveLog {
        #[structopt(short = "f", long = "file", help = "file to read, - to read stdin")]
        file: Option<String>,
        #[structopt(short = "d", long = "to-date")]
        to_date: Option<String>,
    },
    #[structopt(name = "reset", help = "reset the charge controller")]
    ResetController,
    #[structopt(name = "tail")]
    TailStats {
        #[structopt(short = "j", long = "json")]
        json: bool,
    },
    #[structopt(name = "settings", help = "read/write charge controller settings")]
    Settings(Settings),
    #[structopt(name = "night", help = "night power save mode")]
    Night,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "solar", about = "solar power management system")]
struct Options {
    #[structopt(short = "c", long = "config", default_value = "/etc/solar.conf")]
    config: String,
    #[structopt(subcommand)]
    cmd: SubCommand,
}

fn main() {
    let opt = Options::from_args();
    let config = solar_client::load_config(Some(&opt.config));
    use std::iter::once;
    match opt.cmd {
        SubCommand::Start { daemonize } => {
            if daemonize {
                syslog::init(
                    syslog::Facility::LOG_DAEMON,
                    log::LevelFilter::Warn,
                    Some("solar"),
                )
                .expect("failed to init syslog");
                let d = Daemonize::new().pid_file(&config.pid_file());
                match d.start() {
                    Ok(()) => run_server(config),
                    Err(e) => panic!("failed to daemonize: {}", e),
                }
            } else {
                simple_logger::init().expect("failed to init simple logger");
                run_server(config)
            }
        }
        SubCommand::Stop => solar_client::send_command(&config, once(FromClient::Stop))
            .expect("failed to stop the daemon"),
        SubCommand::Load(v) => solar_client::send_command(
            &config,
            &[
                FromClient::SetCharging(false),
                FromClient::SetLoad(v.get()),
                FromClient::SetCharging(true),
            ],
        )
        .expect("failed to set the load. Is the daemon running?"),
        SubCommand::Charging(v) => solar_client::send_command(
            &config,
            once(FromClient::SetCharging(v.get())),
        )
        .expect("failed to disable charging. Is the daemon running?"),
        SubCommand::CancelFloat => solar_client::send_command(
            &config,
            &[
                FromClient::SetCharging(false),
                FromClient::SetCharging(true),
            ],
        )
        .expect("failed to cancel float"),
        SubCommand::ResetController => {
            solar_client::send_command(&config, once(FromClient::ResetController))
                .expect("failed to reset the controller")
        }
        SubCommand::ArchiveLog { file, to_date } => {
            let to_date = to_date.map(|d| {
                use chrono::{naive::NaiveDate, offset::LocalResult, prelude::*};
                let nd = NaiveDate::parse_from_str(&d, "%Y%m%d")
                    .expect("invalid date, %Y%m%d");
                match Local.from_local_date(&nd) {
                    LocalResult::Single(d) => d,
                    LocalResult::None => panic!("invalid timezone conversion"),
                    LocalResult::Ambiguous(_, _) => {
                        panic!("ambiguous timezone conversion")
                    }
                }
            });
            archive::archive_log(&config, file.map(|p| p.into()), to_date)
        }
        SubCommand::TailStats { json } => {
            for m in solar_client::send_query(&config, FromClient::TailStats)
                .expect("failed to tail stats")
            {
                match m {
                    ToClient::Ok | ToClient::Err(_) | ToClient::Settings(_) => {
                        panic!("unexpected response")
                    }
                    ToClient::Stats(s) => {
                        if json {
                            println!("{}", serde_json::to_string_pretty(&s).unwrap())
                        } else {
                            println!("{}", s)
                        }
                    }
                }
            }
        }
        SubCommand::Settings(Settings::Read { json }) => {
            match solar_client::send_query(&config, FromClient::ReadSettings)
                .expect("failed to get settings")
                .next()
            {
                None => panic!("no response from server"),
                Some(ToClient::Stats(_))
                | Some(ToClient::Ok)
                | Some(ToClient::Err(_)) => panic!("unexpected response"),
                Some(ToClient::Settings(s)) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&s).unwrap())
                    } else {
                        println!("{}", s)
                    }
                }
            }
        }
        SubCommand::Settings(Settings::Write { file }) => {
            let file = fs::File::open(&file).expect("failed to open settings");
            let settings =
                serde_json::from_reader(&file).expect("failed to parse settings");
            solar_client::send_command(&config, once(FromClient::WriteSettings(settings)))
                .expect("failed to write settings")
        }
        SubCommand::Phy(Phy::Solar(v)) => {
            solar_client::send_command(&config, once(FromClient::SetPhySolar(v.get())))
                .expect("failed to set physical solar")
        }
        SubCommand::Phy(Phy::Battery(v)) => {
            solar_client::send_command(&config, once(FromClient::SetPhyBattery(v.get())))
                .expect("failed to set physical battery")
        }
        SubCommand::Phy(Phy::Master(v)) => {
            solar_client::send_command(&config, once(FromClient::SetPhyMaster(v.get())))
                .expect("failed to set physical battery")
        }
        SubCommand::Night => {
            solar_client::send_command(&config, &[
                FromClient::SetPhyMaster(false),
                FromClient::SetPhySolar(false),
                FromClient::SetPhyBattery(false)
            ]).expect("failed to enter night mode")
        }
    }
}
