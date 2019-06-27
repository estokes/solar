use actix::prelude::*;
use actix_files;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use morningstar::prostar_mppt::Stats;
use solar_client::{load_config, send_query, FromClient, ToClient};
use std::{
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

static STATS_INTERVAL: Duration = Duration::from_secs(1);

struct ControlSocket {
    stats: Arc<RwLock<Option<Stats>>>,
}

impl Actor for ControlSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let stats = self.stats.clone();
        ctx.run_interval(STATS_INTERVAL, move |act, ctx| {
            match *stats.read().unwrap() {
                None => (),
                Some(ref s) => ctx.text(format!("{}", s)),
            }
        });
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for ControlSocket {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        match msg {
            ws::Message::Ping(m) => ctx.pong(&m),
            ws::Message::Close(_) => ctx.stop(),
            ws::Message::Pong(_)
            | ws::Message::Text(_)
            | ws::Message::Binary(_)
            | ws::Message::Nop => (),
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
