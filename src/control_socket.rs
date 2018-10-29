use std::{
    thread, sync::mpsc::Sender,
    io, os::unix::net::{UnixStream, UnixListener}
};
use serde_json;
use Config;
use ToMainLoop;
use FromClient;
use current_thread;

fn client_loop(stream: UnixStream, to_main: Sender<ToMainLoop>) {
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

fn accept_loop(path: String, to_main: Sender<ToMainLoop>) {
    let listener = or_fatal!(
        to_main, UnixListener::bind(&path),
        "failed to create control socket {}"
    );
    for client in listener.incoming() {
        let client = or_fatal!(to_main, client, "accept failed {}");
        let to_main = to_main.clone();
        thread::Builder::new().name("client_handler".into()).stack_size(1024)
            .spawn(move || client_loop(client, to_main)).unwrap();
    }
}

pub(crate) fn run_server(cfg: &Config, to_main: Sender<ToMainLoop>) {
    let path = cfg.control_socket.clone();
    let to_main_ = to_main.clone();
    thread::Builder::new().name("client_listener".into()).stack_size(256)
        .spawn(move || accept_loop(path, to_main_)).unwrap();
}

pub(crate) fn single_command(cfg: &Config, m: FromClient) -> Result<(), io::Error> {
    let con = UnixStream::connect(&cfg.control_socket)?;
    Ok(serde_json::to_writer(&con, &m)?)
}
