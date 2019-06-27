use actix_web::{middleware, web, App, HttpRequest, HttpServer};
use actix_files;
use morningstar::prostar_mppt::Stats;
use solar_client::{load_config, send_query, FromClient, ToClient};
use std::{
    sync::{Arc, RwLock},
    thread,
};

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
            .service(actix_files::Files::new("/static/", "/static/"))
    })
    .bind("0.0.0.0:8080")?
    .run()
}
