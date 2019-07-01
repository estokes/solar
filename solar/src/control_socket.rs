use crate::ToMainLoop;
use serde_json;
use solar_client::{Config, FromClient};
use std::{
    fs,
    io::{self, BufRead, BufReader, LineWriter, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::PathBuf,
    sync::mpsc::{channel, Sender},
    thread,
};

error_chain! {
    foreign_links {
        SerdeJson(serde_json::Error);
        Io(io::Error);
    }
}

fn client_loop(stream: UnixStream, to_main: Sender<ToMainLoop>) {
    trace!("client loop started");
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = LineWriter::new(stream);
    let mut line = String::new();
    loop {
        trace!("client loop waiting for a message");
        line.clear();
        reader.read_line(&mut line).unwrap();
        let m: FromClient = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(_) => break,
        };
        trace!("client loop message from client: {:?}", m);
        let (send_reply, recv_reply) = channel();
        log_fatal!(
            to_main.send(ToMainLoop::FromClient(m, send_reply)),
            "failed to send {}",
            return
        );
        for s in recv_reply.iter() {
            trace!(
                "client loop reply to client: {:?}",
                serde_json::to_string(&s).unwrap()
            );
            match serde_json::to_writer(writer.by_ref(), &s) {
                Err(_) => break,
                Ok(()) => write!(writer.by_ref(), "\n").unwrap(),
            }
        }
    }
}

fn accept_loop(path: PathBuf, to_main: Sender<ToMainLoop>) {
    let _ = fs::remove_file(&path);
    let listener = log_fatal!(
        UnixListener::bind(&path),
        "failed to create control socket {}",
        return
    );
    for client in listener.incoming() {
        let client = match client {
            Ok(client) => client,
            Err(e) => {
                warn!("accepting client connection failed {}", e);
                continue;
            }
        };
        let to_main = to_main.clone();
        thread::Builder::new()
            .name("client_handler".into())
            .stack_size(1024)
            .spawn(move || client_loop(client, to_main))
            .unwrap();
    }
}

pub(crate) fn run_server(cfg: &Config, to_main: Sender<ToMainLoop>) {
    let path = cfg.control_socket();
    let to_main_ = to_main.clone();
    thread::Builder::new()
        .name("client_listener".into())
        .stack_size(256)
        .spawn(move || accept_loop(path, to_main_))
        .unwrap();
}
