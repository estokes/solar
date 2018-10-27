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

fn send<T: 'static>(sender: Sender<T>, v: T) {
    match sender.send(v) {
        Ok(()) => (),
        Err(e) => {
            error!("failed to send response: {}", e);
            panic!("failed to send response: {}", e)
        }
    }
}

pub(crate) fn start(cfg: &Config, to_main: Sender<ToMainLoop>) -> Result<Sender<Command>, mb::Error> {
    let con = ps::Con::connect(&cfg.device, 1)?;
    let (command_sender, command_receiver) = channel();
    thread::spawn(move || for command in command_receiver.iter() {
        match command {
            Command::Stats => send(to_main, ToMainLoop::Stats(con.stats())),
            Command::Coil(coil, bit) => send(to_main, ToMainLoop::SetCoil(con.write_coil(coil, bit))),
            Command::Stop => {
                info!("modbus loop shutting down as reqested");
                break
            }
        }
    });
    Ok(command_sender)
}
