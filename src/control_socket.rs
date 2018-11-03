use std::{
    thread, sync::mpsc::{Sender, channel}, fs,
    os::unix::net::{UnixStream, UnixListener},
    borrow::Borrow, io::{self, LineWriter, BufReader, BufRead, Write}
};
use serde_json;
use Config;
use ToMainLoop;
use FromClient;
use ToClient;
use current_thread;

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
            Err(_) => break
        };
        trace!("client loop message from client: {:?}", m);
        let (send_reply, recv_reply) = channel();
        or_fatal!(to_main.send(ToMainLoop::FromClient(m, send_reply)), "{} failed to send {}");
        for s in recv_reply.iter() {
            trace!("client loop reply to client: {:?}", serde_json::to_string(&s).unwrap());
            match serde_json::to_writer(writer.by_ref(), &s) {
                Err(_) => break,
                Ok(()) => write!(writer.by_ref(), "\n").unwrap()
            }
        }
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

pub(crate) fn send_command(cfg: &Config, cmds: impl IntoIterator<Item = impl Borrow<FromClient>>) -> Result<(), io::Error> {
    let con = UnixStream::connect(&cfg.control_socket)?;
    let mut con = LineWriter::new(con);
    for cmd in cmds {
        serde_json::to_writer(con.by_ref(), cmd.borrow())?;
        write!(con.by_ref(), "\n");
    }
    Ok(())
}

pub(crate) fn send_query(cfg: &Config, r: Sender<ToClient>, q: FromClient) {
    let socket_path = cfg.control_socket.clone();
    thread::Builder::new().name("query_thread".into()).stack_size(256).spawn(move || {
        let con = UnixStream::connect(&socket_path).unwrap();
        let mut writer = LineWriter::new(con.try_clone().unwrap());
        let mut reader = BufReader::new(con);
        serde_json::to_writer(writer.by_ref(), &q).unwrap();
        write!(writer.by_ref(), "\n").unwrap();
        let mut line = String::new();
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            match serde_json::from_str(&line) {
                Ok(m) => r.send(m).unwrap(),
                Err(e) => panic!("error reading query reply {}", e)
            }
        }
    }).unwrap();
}
