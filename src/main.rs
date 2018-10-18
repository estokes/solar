extern crate morningstar;
use morningstar::prostar_mppt as ps;
use std::{thread::sleep, time::Duration};

fn main() {
    let c = ps::Con::connect("/dev/ttyUSB0", 1).expect("error connecting to controller");
    loop {
        println!("{:#?}", c.stats().expect("failed to fetch stats"));
        sleep(Duration::from_secs(1))
    }
}
