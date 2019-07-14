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
use solar_client::{load_config, send_query, Config, FromClient, Stats, ToClient};
use std::{
    error,
    ffi::OsStr,
    fs,
    io::{self, Read},
    iter::{self, Iterator},
    path::PathBuf,
    sync::{Arc, RwLock},
    thread,
};

#[derive(Clone)]
struct AppData {
    stats: Arc<RwLock<Option<Stats>>>,
    config: Config,
}

fn read_history_file(
    file: PathBuf,
) -> Result<impl Iterator<Item = Stats>, Box<dyn error::Error>> {
    use serde_json::error::Category;
    let reader: Box<dyn io::Read> = {
        let f = fs::File::open(&file)?;
        if file.extension() != Some(OsStr::new("gz")) {
            Box::new(f)
        } else {
            Box::new(gzip::Decoder::new(f)?)
        }
    };
    let mut buf = io::BufReader::new(reader);
    Ok(iter::from_fn(move || {
        match serde_json::from_reader(buf.by_ref()) {
            Ok(o) => Some(o),
            Err(e) => match e.classify() {
                Category::Io | Category::Eof => None,
                Category::Syntax => {
                    error!(
                        "syntax error in log archive, parsing terminated: {:?}, {}",
                        file, e
                    );
                    None
                }
                Category::Data => {
                    error!(
                        "semantic error in log archive, parsing terminated: {:?}, {}",
                        file, e
                    );
                    None
                }
            },
        }
    }))
}

fn read_history(cfg: &Config, mut days: i64) -> impl Iterator<Item = Stats> + '_ {
    let today = Local::today();
    iter::from_fn(move || {
        if days <= 0 {
            None
        } else {
            let d = Duration::days(days);
            days -= 1;
            let file = today
                .checked_sub_signed(d)
                .map(|d| cfg.archive_for_date(d).one_minute_averages);
            file.and_then(|f| match read_history_file(f.clone()) {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("error opening log archive, skipping: {:?}, {}", f, e);
                    None
                }
            })
        }
    })
    .chain(iter::from_fn(move || {
        match read_history_file(cfg.log_file()) {
            Ok(i) => Some(i),
            Err(e) => {
                error!("error opening todays log file, skipping: {}", e);
                None
            }
        }
    }))
    .flatten()
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
    Set(Target, bool),
}

struct ControlSocket(AppData);

impl Actor for ControlSocket {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<ws::Message, ws::ProtocolError> for ControlSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
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
                        Some(s) => ctx.text(ToBrowser::Stats(s).enc()),
                    },
                    FromBrowser::StatsHistory(days) => {
                        for s in read_history(&self.0.config, days) {
                            ctx.text(ToBrowser::Stats(s).enc())
                        }
                        ctx.text(ToBrowser::EndOfHistory.enc())
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
    let iter =
        send_query(&appdata.config, FromClient::TailStats).expect("failed to tail stats");
    thread::spawn(move || {
        for s in iter {
            match s {
                ToClient::Stats(s) => *appdata.stats.write().unwrap() = Some(s),
                ToClient::Settings(_) | ToClient::Ok | ToClient::Err(_) => (),
            }
        }
    });
}

fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
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
