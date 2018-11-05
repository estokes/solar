use morningstar::{error as mse, prostar_mppt as ps};
use std::{thread, sync::mpsc::{Receiver, Sender, channel}, time::{Instant, Duration}};
use Config;
use ToMainLoop;
use current_thread;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Command {
    Stats,
    Coil(ps::Coil, bool)
}

struct ConWithRetry {
    con: Option<ps::Connection>,
    device: String,
    address: u8
}

impl ConWithRetry {
    fn new(device: String, address: u8) -> Self {
        ConWithRetry { con: None, device, address }
    }

    fn get_con(&mut self) -> mse::Result<&ps::Connection> {
        match self.con {
            Some(ref con) => Ok(con),
            None =>
                match ps::Connection::new(&self.device, self.address) {
                    Err(e) => Err(e),
                    Ok(con) => {
                        self.con = Some(con);
                        Ok(self.con.as_ref().unwrap())
                    }
                }
        }
    }

    fn eval<F, R>(&mut self, mut f: F) -> mse::Result<R>
    where F: FnMut(&ps::Connection) -> mse::Result<R> {
        let mut tries = 0;
        loop {
            let r = match self.get_con() {
                Ok(con) => f(con),
                Err(e) => Err(e)
            };
            match r {
                Ok(r) => break Ok(r),
                Err(e) => {
                    if tries >= 3 { break Err(e) }
                    else {
                        thread::sleep(Duration::from_millis(5000));
                        self.con = None;
                        tries += 1
                    }
                }
            }
        }
    }
}

fn modbus_loop(
    device: String,
    to_main: Sender<ToMainLoop>,
    command_receiver: Receiver<Command>
) {
    let mut last_command = Instant::now();
    let mut con = ConWithRetry::new(device, 1);
    for command in command_receiver.iter() {
        while last_command.elapsed() < Duration::from_millis(500) {
            thread::sleep(Duration::from_millis(50))
        }
        last_command = Instant::now();
        match command {
            Command::Coil(coil, bit) => or_fatal!(
                to_main,
                con.eval(move |c| c.write_coil(coil, bit)),
                "failed to set coil {}"
            ),
            Command::Stats => {
                let s = or_fatal!(
                    to_main, con.eval(|c| c.stats()), "failed to get stats {}");
                or_fatal!(
                    to_main.send(ToMainLoop::Stats(s)), "{} failed to send to main {}");
            }
        }
    }
}

pub(crate) fn start(cfg: &Config, to_main: Sender<ToMainLoop>) -> Sender<Command> {
    let (command_sender, command_receiver) = channel();
    let device = cfg.device.clone();
    thread::Builder::new().name("modbus".into()).stack_size(1024)
        .spawn(move || modbus_loop(device, to_main, command_receiver)).unwrap();
    command_sender
}
