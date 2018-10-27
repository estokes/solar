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
        let msg: FromClient = match serde_json::from_reader(&stream) {
            Ok(m) => m,
            Err(e) => {
                error!("malformed message from client: {}", e);
                break
            }
        };
        let _ = to_main.send(ToMainLoop::FromClient(msg));
    }
}

pub(crate) fn run_server(cfg: &Config, to_main: Sender<ToMainLoop>) -> Result<(), io::Error> {
    let listener = UnixListener::bind(&cfg.control_socket)?;
    thread::spawn(move || for client in listener.incoming() {
        let to_main = to_main.clone();
        thread::spawn(move || handle_client(to_main, client));
    });
    Ok(())
}
