#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
use actix::prelude::*;
use actix_identity::{CookieIdentityPolicy, Identity, IdentityService};
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use chrono::{prelude::*, Duration};
use rustls::internal::pemfile::{certs, pkcs8_private_keys};
use rustls::{NoClientAuth, ServerConfig};
use solar_client::{
    archive, load_config, send_command, send_query, Config, FromClient, Stats, ToClient,
};
use std::{
    fs::File,
    io::BufReader,
    iter,
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
    PhyBattery,
    PhyMaster,
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
                    },
                    FromBrowser::StatsDecimated => match *self.0.stats.read().unwrap() {
                        None => ctx.text(ToBrowser::CmdErr("not available".into()).enc()),
                        Some(c) => ctx.text(ToBrowser::StatsDecimated(c.decimated).enc()),
                    },
                    FromBrowser::StatsHistory(days) => {
                        info!("fetching current stats going back {}", days);
                        for s in archive::read_history(&self.0.config, days) {
                            ctx.text(ToBrowser::Stats(s).enc())
                        }
                        ctx.text(ToBrowser::EndOfHistory.enc());
                        info!("done fetching stats");
                    }
                    FromBrowser::Set(tgt, v) => {
                        let cmd = match tgt {
                            Target::Load => FromClient::SetLoad(v),
                            Target::Charging => FromClient::SetCharging(v),
                            Target::PhySolar => FromClient::SetPhySolar(v),
                            Target::PhyBattery => FromClient::SetPhyBattery(v),
                            Target::PhyMaster => FromClient::SetPhyMaster(v),
                        };
                        match send_command(&self.0.config, iter::once(cmd)) {
                            Ok(()) => ctx.text(ToBrowser::CmdOk.enc()),
                            Err(e) => ctx.text(ToBrowser::CmdErr(e.to_string()).enc()),
                        }
                    }
                },
            },
        }
    }
}

fn control_socket(
    r: HttpRequest,
    id: Identity,
    d: web::Data<AppData>,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    info!("{:?}", r);
    match id.identity() {
        None => Ok(HttpResponse::Unauthorized().finish()),
        Some(_) => ws::start(ControlSocket(d.get_ref().clone()), &r, stream),
    }
}

fn read_stats(appdata: AppData) {
    let ten_minutes = Duration::seconds(600);
    thread::spawn(move || loop {
        let i: Box<dyn Iterator<Item = ToClient>> =
            match send_query(&appdata.config, FromClient::TailStats) {
                Ok(i) => Box::new(i),
                Err(e) => {
                    error!("failed to tail stats {}", e);
                    thread::sleep(std::time::Duration::from_secs(60));
                    Box::new(None.into_iter())
                }
            };
        for s in i {
            match s {
                ToClient::Settings(_) | ToClient::Ok | ToClient::Err(_) => (),
                ToClient::Stats(s) => {
                    let mut sc = appdata.stats.write().unwrap();
                    *sc = match *sc {
                        None => Some(StatContainer {
                            current: s,
                            decimated: s,
                            decimated_acc: s,
                            decimated_ts: s.timestamp(),
                        }),
                        Some(mut c) => {
                            c.current = s;
                            if c.decimated_acc.timestamp() - c.decimated_ts < ten_minutes
                            {
                                archive::stats_accum(&mut c.decimated_acc, &s);
                            } else {
                                c.decimated = c.decimated_acc;
                                c.decimated_acc = s;
                                c.decimated_ts = s.timestamp();
                            }
                            Some(c)
                        }
                    }
                }
            }
        }
    });
}

#[derive(Debug, Deserialize)]
struct LoginParams {
    user: String,
    password: String,
}

fn handle_login(
    id: Identity,
    params: web::Form<LoginParams>,
) -> Result<HttpResponse, Error> {
    use pam::Authenticator;
    let fail = || Ok(HttpResponse::Found().header("LOCATION", "/login-failed").finish());
    match Authenticator::with_password("system-auth") {
        Err(e) => {
            error!("failed to create authenticator {}", e);
            fail()
        }
        Ok(mut auth) => {
            auth.get_handler().set_credentials(&params.user, &params.password);
            match auth.authenticate() {
                Err(_) => fail(),
                Ok(()) => {
                    id.remember(params.user.clone());
                    Ok(HttpResponse::Found().header("LOCATION", "/").finish())
                }
            }
        }
    }
}

macro_rules! inc {
    ($name:expr, $file:expr, $auth:expr, $ctyp:expr) => {
        web::resource($name).route(web::get().to(|id: Identity| {
            let resp = HttpResponse::Ok().content_type($ctyp).body(include_str!($file));
            if !$auth {
                resp
            } else {
                match id.identity() {
                    None => HttpResponse::Found().header("LOCATION", "/login").finish(),
                    Some(_) => resp,
                }
            }
        }))
    };
    ($name:expr, $file:expr) => {
        inc!($name, $file, true, "text/html; charset=utf-8")
    };
}

fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    info!("starting server");
    let mut config = ServerConfig::new(NoClientAuth::new());
    let cert_file =
        &mut BufReader::new(File::open("cert.pem").expect("failed to open certificate"));
    let key_file =
        &mut BufReader::new(File::open("key.pem").expect("failed to open private key"));
    let cert_chain = certs(cert_file).expect("failed to read certificate");
    let mut keys = pkcs8_private_keys(key_file).expect("failed to read private key");
    config
        .set_single_cert(cert_chain, keys.pop().expect("no private key found"))
        .expect("failed to configure rustls");
    let cookie_key = {
        use rand::prelude::*;
        let mut a = [0; 512];
        let mut rng = rand::thread_rng();
        for i in 0..512 {
            a[i] = rng.gen();
        }
        a
    };
    HttpServer::new(move || {
        let appdata =
            AppData { stats: Arc::new(RwLock::new(None)), config: load_config(None) };
        read_stats(appdata.clone());
        App::new()
            .data(appdata)
            .wrap(middleware::Logger::default())
            .wrap(IdentityService::new(
                CookieIdentityPolicy::new(&cookie_key).name("auth-cookie").secure(true),
            ))
            .service(inc!("/", "../static/index.html"))
            .service(inc!(
                "/Chart.bundle.min.js",
                "../static/Chart.bundle.min.js",
                true,
                "text/javascript; charset=utf-8"
            ))
            .service(inc!(
                "/jquery-3.4.1.min.js",
                "../static/jquery-3.4.1.min.js",
                true,
                "text/javascript; charset=utf-8"
            ))
            .service(inc!("/ui.css", "../static/ui.css", true, "text/css; charset=utf-8"))
            .service(inc!(
                "/ui.js",
                "../static/ui.js",
                true,
                "text/javascript; charset=utf-8"
            ))
            .service(
                inc!("/login", "../static/login.html", false, "text/html; charset=utf-8")
                    .route(web::post().to(handle_login)),
            )
            .service(inc!(
                "/login-failed",
                "../static/login-failed.html",
                false,
                "text/html; charset=utf-8"
            ))
            .service(web::resource("/ws/").route(web::get().to(control_socket)))
    })
    .bind_rustls("0.0.0.0:8443", config)?
    .run()
}
