use morningstar::prostar_mppt as ps;
use libmodbus_rs::prelude as mb;
use std::{thread, fs::File, sync::mpsc::{Receiver, Sender, channel}};
use Config;
use ToMainloop;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Command {
    Stats,
    Coil(ps::Coil, bool)
}

pub(crate) fn start(cfg: &Config, to_main: Sender<ToMainLoop>) -> Result<Sender<Command>, mb::Error> {
    let con = ps::Con::connect(&cfg.device, 1)?;
    let (command_sender, command_receiver) = channel();
    thread::spawn(move || for command in command_receiver.iter() {
        match command {
            Command::Stats =>
                match con.stats() {
                    Ok(s) =>
                        match to_main.send(ToMainLoop::Stats(s)) {
                            Ok(()) => (),
                            Err(_) => break
                        }
                    Err(e) => {
                        let _ = to_main.send(ToMainLoop::FatalError(format!("failed to fetch stats {}", e)));
                        break
                    }
                },
            Command::Coil(coil, bit) =>
                match con.write_coil(coil, bit) {
                    Ok(()) =>
                        match to_main.send(ToMainLoop::CoilWasSet) {
                            Ok(()) => (),
                            Err(_) => break
                        },
                    Err(e) => {
                        let _ = to_main.send(ToMainLoop::FatalError(format!("failed to set coil {}", e)));
                        break
                    }
                }
        }
    });
    Ok(command_sender)
}
