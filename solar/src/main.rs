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

use chrono;
use daemonize::Daemonize;
use morningstar::error as mse;
use morningstar::prostar_mppt as ps;
use solar_client::{self, Config, FromClient, ToClient};
use std::{
    fs,
    io::{self, LineWriter, Write},
    path::Path,
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

    for msg in receiver.iter() {
        match msg {
            ToMainLoop::FromClient(msg, reply) => match msg {
                FromClient::SetChargingEnabled(b) => {
                    send_reply(mb.write_coil(ps::Coil::ChargeDisconnect, !b), reply)
                }
                FromClient::SetLoadEnabled(b) => {
                    send_reply(mb.write_coil(ps::Coil::LoadDisconnect, !b), reply)
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
                let st = solar_client::Stats(log_fatal!(
                    mb.read_stats(),
                    "fatal: failed to read stats {}",
                    break
                ));
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

fn file_exists(path: &Path) -> bool {
    match fs::metadata(&todays) {
        Ok(m) => m.is_file(),
        Error(e) => e.kind = std::io::ErrorKind::NotFound,
    }
}

fn rotate_log(cfg: &Config) {
    use libflate::gzip::Encoder;
    use std::{fs::OpenOptions, io};
    let todays = cfg.archive_for_date(chrono::Utc::today());
    if file_exists(&todays) {
        println!("nothing to do");
    } else {
        let current = cfg.log_file();
        let mut tmp = current.clone();
        tmp.set_extension("tmp");
        fs::hard_link(&current, &tmp).expect("failed to create tmp file");
        fs::remove_file(&current).expect("failed to unlink current file");
        solar_client::send_command(&config, once(FromClient::LogRotated))
            .expect("failed to reopen log file");
        let encoder = Encoder::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&todays)
                .expect("failed to open archive file"),
        );
        io::copy(
            &mut fs::File::open(&tmp).expect("failed to open tmp file"),
            &mut encoder,
        ).expect("failed to copy tmp file to the archive");
        fs::remove_file(&tmp).expect("failed to remove tmp file");
    }
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
    #[structopt(name = "rotate-log")]
    RotateLog,
    #[structopt(name = "reset-controller")]
    ResetController,
    #[structopt(name = "tail-stats")]
    TailStats {
        #[structopt(short = "j", long = "json")]
        json: bool,
    },
    #[structopt(name = "read-settings")]
    ReadSettings {
        #[structopt(short = "j", long = "json")]
        json: bool,
    },
    #[structopt(name = "write-settings")]
    WriteSettings { file: String },
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
        SubCommand::DisableLoad => solar_client::send_command(
            &config,
            &[
                FromClient::SetChargingEnabled(false),
                FromClient::SetLoadEnabled(false),
                FromClient::SetChargingEnabled(true),
            ],
        )
        .expect("failed to disable load. Is the daemon running?"),
        SubCommand::EnableLoad => solar_client::send_command(
            &config,
            &[
                FromClient::SetChargingEnabled(false),
                FromClient::SetLoadEnabled(true),
                FromClient::SetChargingEnabled(true),
            ],
        )
        .expect("failed to enable load. Is the daemon running?"),
        SubCommand::DisableCharging => {
            solar_client::send_command(&config, once(FromClient::SetChargingEnabled(false)))
                .expect("failed to disable charging. Is the daemon running?")
        }
        SubCommand::EnableCharging => {
            solar_client::send_command(&config, once(FromClient::SetChargingEnabled(true)))
                .expect("failed to enable charging. Is the daemon running?")
        }
        SubCommand::CancelFloat => solar_client::send_command(
            &config,
            &[
                FromClient::SetChargingEnabled(false),
                FromClient::SetChargingEnabled(true),
            ],
        )
        .expect("failed to cancel float"),
        SubCommand::ResetController => {
            solar_client::send_command(&config, once(FromClient::ResetController))
                .expect("failed to reset the controller")
        }
        SubCommand::RotateLog => rotate_logs(&config),
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
        SubCommand::ReadSettings { json } => {
            match solar_client::send_query(&config, FromClient::ReadSettings)
                .expect("failed to get settings")
                .next()
            {
                None => panic!("no response from server"),
                Some(ToClient::Stats(_)) | Some(ToClient::Ok) | Some(ToClient::Err(_)) => {
                    panic!("unexpected response")
                }
                Some(ToClient::Settings(s)) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&s).unwrap())
                    } else {
                        println!("{}", s)
                    }
                }
            }
        }
        SubCommand::WriteSettings { file } => {
            let file = fs::File::open(&file).expect("failed to open settings");
            let settings = serde_json::from_reader(&file).expect("failed to parse settings");
            solar_client::send_command(&config, once(FromClient::WriteSettings(settings)))
                .expect("failed to write settings")
        }
    }
}
