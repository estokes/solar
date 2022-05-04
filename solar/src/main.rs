#![recursion_limit = "1024"]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate netidx_core;

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
mod publisher;

use anyhow::Result;
use daemonize::Daemonize;
use futures::{prelude::*, select_biased};
use morningstar::prostar_mppt as ps;
use publisher::Netidx;
use solar_client::{self, archive, Config, FromClient, Stats, ToClient};
use std::time::Duration;
use structopt::StructOpt;
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
    runtime::Runtime,
    sync::mpsc::{channel, Sender},
    time,
};

#[derive(Debug, Clone)]
pub(crate) enum ToMainLoop {
    FromClient(FromClient, Sender<ToClient>),
    Tick,
}

async fn open_log(cfg: &Config) -> Result<io::BufWriter<fs::File>, io::Error> {
    let log = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&cfg.log_file())
        .await?;
    Ok(io::BufWriter::new(log))
}

async fn send_reply(r: Result<()>, s: Sender<ToClient>) {
    match r {
        Ok(()) => {
            let _ = s.send(ToClient::Ok).await;
        }
        Err(e) => {
            let _ = s.send(ToClient::Err(e.to_string())).await;
        }
    }
}

async fn run_server(config: Config) {
    let (to_main, mut receiver) = channel(100);
    let mut log = log_fatal!(open_log(&config).await, "failed to open log {}", return);
    let mut mb = modbus::Connection::new(config.device.clone(), config.modbus_id).await;
    let mut tick = time::interval(Duration::from_secs(config.stats_interval));
    control_socket::run_server(&config, to_main.clone());
    let netidx =
        log_fatal!(Netidx::new(&config, to_main).await, "init publisher {}", return);
    let mut tailing: Vec<Sender<ToClient>> = Vec::new();
    let mut statsbuf = Vec::new();
    let mut initsettings = false;
    let mut batch = netidx.start_batch();
    loop {
        let msg = select_biased! {
            _ = tick.tick().fuse() => ToMainLoop::Tick,
            m = receiver.recv().fuse() => match m {
                None => break,
                Some(m) => m
            }
        };
        debug!("run_server: {:?}", msg);
        match msg {
            ToMainLoop::FromClient(msg, reply) => match msg {
                FromClient::SetCharging(b) => {
                    send_reply(mb.write_coil(ps::Coil::ChargeDisconnect, !b).await, reply)
                        .await
                }
                FromClient::SetLoad(b) => {
                    send_reply(mb.write_coil(ps::Coil::LoadDisconnect, !b).await, reply)
                        .await
                }
                FromClient::ResetController => {
                    send_reply(mb.write_coil(ps::Coil::ResetControl, true).await, reply)
                        .await
                }
                FromClient::LogRotated => {
                    log = log_fatal!(
                        open_log(&config).await,
                        "failed to open log {}",
                        break
                    );
                    send_reply(Ok(()), reply).await
                }
                FromClient::TailStats => tailing.push(reply),
                FromClient::WriteSettings(settings) => {
                    let r = mb.write_settings(&settings).await;
                    if r.is_ok() {
                        netidx.update_settings(&mut batch, &settings);
                    }
                    send_reply(r, reply).await
                }
                FromClient::ReadSettings => match mb.read_settings().await {
                    Ok(s) => {
                        netidx.update_settings(&mut batch, &s);
                        reply.send(ToClient::Settings(s)).await.ok();
                    }
                    Err(e) => {
                        reply.send(ToClient::Err(e.to_string())).await.ok();
                    }
                },
                FromClient::Stop => {
                    reply.send(ToClient::Ok).await.ok();
                    time::sleep(Duration::from_millis(200)).await;
                    break;
                }
            },
            ToMainLoop::Tick => {
                let controller = {
                    if !initsettings {
                        debug!("tick: reading initial settings");
                        match mb.read_settings().await {
                            Ok(s) => {
                                initsettings = true;
                                netidx.update_settings(&mut batch, &s);
                            }
                            Err(_) => (),
                        }
                    }
                    debug!("tick: reading stats");
                    match mb.read_stats().await {
                        Ok(s) => {
                            netidx.update_stats(&mut batch, &s);
                            netidx.update_control(&mut batch, &s);
                            Some(s)
                        }
                        Err(e) => {
                            error!("reading stats failed: {}", e);
                            None
                        }
                    }
                };
                debug!("tick: flushing publisher");
                if batch.len() > 0 {
                    batch.commit(Some(Duration::from_secs(10))).await;
                    batch = netidx.start_batch();
                }
                let timestamp = chrono::Local::now();
                let st = Stats::V3 { timestamp, controller };
                statsbuf.clear();
                log_fatal!(
                    serde_json::to_writer(&mut statsbuf, &st),
                    "fatal: failed to format stats {}",
                    break
                );
                statsbuf.push(b'\n');
                log_fatal!(
                    log.write_all(&statsbuf).await,
                    "fatal: failed to log stats {}",
                    break
                );
                let mut i = 0;
                debug!("tick: writing stats to tailing clients");
                while i < tailing.len() {
                    match tailing[i].send(ToClient::Stats(st)).await {
                        Ok(()) => i += 1,
                        Err(_) => {
                            tailing.remove(i);
                        }
                    }
                }
                debug!("tick: finished");
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
    use std::fs;
    let opt = Options::from_args();
    let config = solar_client::load_config(Some(&opt.config));
    match fs::metadata(&config.run_directory) {
        Ok(_) => (),
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => {
                fs::create_dir_all(&config.run_directory).expect("failed to open rundir")
            }
            _ => panic!("failed to open rundir {}", e),
        },
    }
    use std::iter::once;
    match opt.cmd {
        SubCommand::Start { daemonize } => {
            if daemonize {
                syslog::init(
                    syslog::Facility::LOG_DAEMON,
                    config.log_level,
                    Some("solar"),
                )
                .expect("failed to init syslog");
                let d = Daemonize::new().pid_file(&config.pid_file());
                match d.start() {
                    Ok(()) => Runtime::new().unwrap().block_on(run_server(config)),
                    Err(e) => panic!("failed to daemonize: {}", e),
                }
            } else {
                if let Some(level) = config.log_level.to_level() {
                    simple_logger::init_with_level(level).unwrap();
                }
                Runtime::new().unwrap().block_on(run_server(config))
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
        SubCommand::Charging(v) => {
            solar_client::send_command(&config, once(FromClient::SetCharging(v.get())))
                .expect("failed to disable charging. Is the daemon running?")
        }
        SubCommand::CancelFloat => solar_client::send_command(
            &config,
            &[FromClient::SetCharging(false), FromClient::SetCharging(true)],
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
    }
}
