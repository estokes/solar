use crate::ToMainLoop;
use anyhow::{Result, Error};
use futures::prelude::*;
use serde_json;
use solar_client::{Config, FromClient};
use std::{fs, path::PathBuf, time::Duration};
use tokio::{
    io::BufStream,
    net::{UnixListener, UnixStream},
    prelude::*,
    sync::mpsc::{channel, Sender},
    task, time,
};

static STO: Duration = Duration::from_secs(2);

async fn client_loop(stream: UnixStream, mut to_main: Sender<ToMainLoop>) {
    trace!("client loop started");
    let mut stream = BufStream::new(stream);
    let mut line = String::new();
    let mut buf = Vec::new();
    let res: Result<()> = loop {
        line.clear();
        debug!("client loop waiting for line");
        try_cf!(stream.read_line(&mut line).await);
        let m: FromClient = try_cf!(serde_json::from_str(&line));
        debug!("client loop message from client: {:?}", m);
        let (send_reply, mut recv_reply) = channel(100);
        try_cf!(to_main.send(ToMainLoop::FromClient(m, send_reply)).await);
        try_cf!(loop {
            buf.clear();
            match recv_reply.next().await {
                None => {
                    debug!("no more replies for this query");
                    break Ok(())
                },
                Some(s) => {
                    debug!("reply to client {:?}", s);
                    try_cf!(serde_json::to_writer(&mut buf, &s));
                    buf.push(b'\n');
                    try_cf!(try_cf!(time::timeout(STO, stream.write_all(&buf)).await));
                    try_cf!(try_cf!(time::timeout(STO, stream.flush()).await));
                }
            }
        });
    };
    info!("client loop shutting down {:?}", res);
}

async fn accept_loop(path: PathBuf, to_main: Sender<ToMainLoop>) {
    let _ = fs::remove_file(&path);
    let mut listener = log_fatal!(
        UnixListener::bind(&path),
        "failed to create control socket {}",
        return
    );
    while let Some(client) = listener.next().await {
        let client = match client {
            Ok(client) => client,
            Err(e) => {
                warn!("accepting client connection failed {}", e);
                continue;
            }
        };
        let to_main = to_main.clone();
        task::spawn(client_loop(client, to_main.clone()));
    }
    info!("client accept loop shutting down");
}

pub(crate) fn run_server(cfg: &Config, to_main: Sender<ToMainLoop>) {
    task::spawn(accept_loop(cfg.control_socket(), to_main.clone()));
}
