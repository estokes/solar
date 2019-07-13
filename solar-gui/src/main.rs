#[macro_use]
extern crate serde_derive;
use actix::prelude::*;
use actix_files;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use solar_client::{load_config, send_query, FromClient, Stats, ToClient};
use std::{
    sync::{Arc, RwLock},
    thread,
};

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
    StatsHistory { start: String, end: String },
    StatsCurrent,
    Set(Target, bool),
}

struct ControlSocket {
    stats: Arc<RwLock<Option<Stats>>>,
}

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
                    ToBrowser::CmdErr(format!("invalid command: {},  {}", m, e.to_string()))
                        .enc(),
                ),
                Ok(cmd) => match cmd {
                    FromBrowser::StatsCurrent => match *self.stats.read().unwrap() {
                        None => ctx.text(ToBrowser::CmdErr("not available".into()).enc()),
                        Some(s) => ctx.text(ToBrowser::Stats(s).enc()),
                    },
                    FromBrowser::StatsHistory { .. } | FromBrowser::Set(_, _) => {
                        ctx.text(ToBrowser::CmdErr("not implemented".into()).enc())
                    }
                },
            },
        }
    }
}

fn control_socket(r: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    let d = r.app_data::<Arc<RwLock<Option<Stats>>>>().unwrap();
    ws::start(ControlSocket { stats: d.clone() }, &r, stream)
}

fn read_stats(stats: Arc<RwLock<Option<Stats>>>) {
    let config = load_config(None);
    let iter = send_query(&config, FromClient::TailStats).expect("failed to tail stats");
    thread::spawn(move || {
        for s in iter {
            match s {
                ToClient::Stats(s) => *stats.write().unwrap() = Some(s),
                ToClient::Settings(_) | ToClient::Ok | ToClient::Err(_) => (),
            }
        }
    });
}

fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
    HttpServer::new(|| {
        let stats: Arc<RwLock<Option<Stats>>> = Arc::new(RwLock::new(None));
        read_stats(stats.clone());
        App::new()
            .data(stats)
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
