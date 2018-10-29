use morningstar::prostar_mppt as ps;
use std::{
    thread, fs, sync::mpsc::{Sender, Receiver, channel},
    io, os::unix::net::{UnixStream, UnixListener}
};
use serde_json;
use Config;
use ToMainLoop;
use FromClient;

fn handle_client(stream: UnixStream, to_main: Sender<ToMainLoop>) {
    loop {
        let m: FromClient = match serde_json::from_reader(&stream) {
            Ok(m) => m,
            Err(e) => {
                error!("malformed message from client: {}", e);
                break
            }
        };
        or_fatal!(to_main.send(ToMainLoop::FromClient(m)), "{} failed to send {}");
    }
}

pub(crate) fn run_server(cfg: &Config, to_main: Sender<ToMainLoop>) {
    let path = cfg.control_socket.clone();
    thread::Builder::new().name("client_listener").stack_size(256).spawn(move || {
        let listener = or_fatal!(
            to_main, UnixListener::bind(&path),
            "failed to create control socket {}"
        );
        for client in listener.incoming() {
            let to_main = to_main.clone();
            thread::Builder::new().name("client_handler").stack_size(1024)
                .spawn(move || handle_client(to_main, client));
        }
    })
}

pub(crate) fn single_command(cfg: &Config, m: FromClient) -> Result<(), io::Error> {
    let con = UnixStream::connect(&cfg.control_socket)?;
    Ok(serde_json::to_writer(&m)?)
}
