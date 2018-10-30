use std::{
    thread, sync::mpsc::Sender, io::{self, Write}, fs,
    os::unix::net::{UnixStream, UnixListener},
    borrow::Borrow
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
    let _ = fs::remove_file(&path);
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

pub(crate) fn send(cfg: &Config, cmds: impl IntoIterator<Item = impl Borrow<FromClient>>) -> Result<(), io::Error> {
    let mut con = UnixStream::connect(&cfg.control_socket)?;
    for cmd in cmds { serde_json::to_writer(&con, cmd.borrow())?; }
    Ok(con.flush()?)
}
