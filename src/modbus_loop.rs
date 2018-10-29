use morningstar::prostar_mppt as ps;
use libmodbus_rs::prelude as mb;
use std::{thread, fs::File, sync::mpsc::{Receiver, Sender, channel}};
use Config;
use ToMainLoop;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Command {
    Stats,
    Coil(ps::Coil, bool)
}

pub(crate) fn start(cfg: &Config, to_main: Sender<ToMainLoop>) -> Sender<Command> {
    let (command_sender, command_receiver) = channel();
    let device = cfg.device.clone();
    thread::spawn(move || {
        let con = or_fatal!(
            to_main, ps::Con::connect(&device, 1),
            "failed to connect to modbus {}"
        );
        for command in command_receiver.iter() {
            match command {
                Command::Coil(coil, bit) =>
                    or_fatal!(to_main, con.write_coil(coil, bit), "failed to set coil {}"),
                Command::Stats => {
                    let s = or_fatal!(to_main, con.stats(), "failed to get stats {}");
                    or_fatal!(to_main.send(ToMainLoop::Stats(s)), "{} failed to send to main {}");
                }
            }
        }
    });
    command_sender
}
