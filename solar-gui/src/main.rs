#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
use actix::prelude::*;
use actix_files;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use chrono::{prelude::*, Duration};
use libflate::gzip;
use solar_client::{
    archive, load_config, send_query, Config, FromClient, Stats, ToClient,
};
use std::{
    error,
    ffi::OsStr,
    fs,
    io::{self, BufRead, Read},
    iter::{self, Iterator},
    path::PathBuf,
    sync::{Arc, RwLock},
    thread,
};

#[derive(Copy, Clone)]
struct StatContainer {
    current: Stats,
    decimated: Stats,
    decimated_acc: Stats,
    decimated_ts: DateTime<Local>,
}

#[derive(Clone)]
struct AppData {
    stats: Arc<RwLock<Option<StatContainer>>>,
    config: Config,
}

#[derive(Debug, Serialize, Deserialize)]
enum Target {
    Load,
    Charging,
    PhySolar,
    PhyController,
}

#[derive(Debug, Serialize, Deserialize)]
enum ToBrowser {
    CmdOk,
    CmdErr(String),
    Stats(Stats),
    StatsDecimated(Stats),
    Status(Target, bool),
    EndOfHistory,
}

impl ToBrowser {
    fn enc(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|e| {
            serde_json::to_string(&ToBrowser::CmdErr(e.to_string()))
                .unwrap_or_else(|e| format!("json encoding is completely broken: {}", e))
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum FromBrowser {
    StatsHistory(i64),
    StatsCurrent,
    StatsDecimated,
    Set(Target, bool),
}

struct ControlSocket(AppData);

impl Actor for ControlSocket {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<ws::Message, ws::ProtocolError> for ControlSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        info!("handle websocket message: {:?}", msg);
        match msg {
            ws::Message::Ping(m) => ctx.pong(&m),
            ws::Message::Close(_) => ctx.stop(),
            ws::Message::Pong(_) | ws::Message::Binary(_) | ws::Message::Nop => (),
            ws::Message::Text(m) => match serde_json::from_str::<FromBrowser>(&m) {
                Err(e) => ctx.text(
                    ToBrowser::CmdErr(format!(
                        "invalid command: {},  {}",
                        m,
                        e.to_string()
                    ))
                    .enc(),
                ),
                Ok(cmd) => match cmd {
                    FromBrowser::StatsCurrent => match *self.0.stats.read().unwrap() {
                        None => ctx.text(ToBrowser::CmdErr("not available".into()).enc()),
                        Some(c) => ctx.text(ToBrowser::Stats(c.current).enc()),
                    }
                    FromBrowser::StatsDecimated => match *self.0.stats.read().unwrap() {
                        None => ctx.text(ToBrowser::CmdErr("not available".into()).enc()),
                        Some(c) => ctx.text(ToBrowser::StatsDecimated(c.decimated).enc()),
                    },
                    FromBrowser::StatsHistory(days) => {
                        info!("fetching current stats going back {}", days);
                        for s in read_history(&self.0.config, days) {
                            ctx.text(ToBrowser::Stats(s).enc())
                        }
                        ctx.text(ToBrowser::EndOfHistory.enc());
                        info!("done fetching stats");
                    }
                    FromBrowser::Set(_, _) => {
                        ctx.text(ToBrowser::CmdErr("not implemented".into()).enc())
                    }
                },
            },
        }
    }
}

fn control_socket(r: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    let d = r.app_data::<AppData>().unwrap();
    ws::start(ControlSocket(d.clone()), &r, stream)
}

fn read_stats(appdata: AppData) {
    let ten_minutes = Duration::seconds(600);
    let iter =
        send_query(&appdata.config, FromClient::TailStats).expect("failed to tail stats");
    thread::spawn(move || {
        for s in iter {
            match s {
                ToClient::Settings(_) | ToClient::Ok | ToClient::Err(_) => (),
                ToClient::Stats(s) => {
                    let sc = appdata.stats.write().unwrap();
                    *sc = match *sc {
                        None => Some(StatContainer {
                            current: s,
                            decimated: s,
                            decimated_acc: s,
                            decimated_ts: s.timestamp,
                        }),
                        Some(mut c) => {
                            c.current = s;
                            if c.decimated_acc.timestamp - c.decimated_ts < ten_minutes {
                                archive::stats_accum(&mut c.decimated_acc, &s);
                            } else {
                                c.decimated = c.decimated_acc;
                                c.decimated_acc = s;
                                c.decimated_ts = s.timestamp;
                            }
                            Some(c)
                        }
                    }
                }
            }
        }
    });
}

fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    info!("starting server");
    HttpServer::new(|| {
        let appdata = AppData {
            stats: Arc::new(RwLock::new(None)),
            config: load_config(None),
        };
        read_stats(appdata.clone());
        App::new()
            .data(appdata)
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::get().to(|| {
                HttpResponse::Found()
                    .header("LOCATION", "/static/index.html")
                    .finish()
            })))
            .service(actix_files::Files::new("/static/", "./static/"))
            .service(web::resource("/ws/").route(web::get().to(control_socket)))
    })
    .bind("0.0.0.0:8080")?
    .run()
}
