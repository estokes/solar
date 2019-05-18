use rppal::gpio::{Gpio, OutputPin, Result};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
enum Relay {
    R0 = 26,
    R1 = 19,
    R2 = 13,
    R3 = 6,
}

struct Relays {
    r0: OutputPin,
    r1: OutputPin,
    r2: OutputPin,
    r3: OutputPin,
}

impl Relays {
    fn new() -> Result<Relays> {
        let io = Gpio::new()?;
        let r0 = io.get(Relay::R0 as u8)?.into_output();
        let r1 = io.get(Relay::R1 as u8)?.into_output();
        let r2 = io.get(Relay::R2 as u8)?.into_output();
        let r3 = io.get(Relay::R3 as u8)?.into_output();
        Ok(Relays { r0, r1, r2, r3 })
    }

    fn get(&mut self, r: Relay) -> &mut OutputPin {
        match r {
            Relay::R0 => &mut self.r0,
            Relay::R1 => &mut self.r1,
            Relay::R2 => &mut self.r2,
            Relay::R3 => &mut self.r3,
        }
    }

    fn on(&mut self, r: Relay) {
        self.get(r).set_high();
    }

    fn off(&mut self, r: Relay) {
        self.get(r).set_low();
    }
}

use std::{time::Duration, thread};

const SOLAR: Relay = Relay::R2;
const BATTERY: Relay = Relay::R1;
const MASTER: Relay = Relay::R3;
const DELAY_RELAY: Duration = Duration::from_millis(500);
const DELAY_REBOOT: Duration = Duration::from_secs(15);

pub struct Rpi(Relays);

impl Rpi {
    pub fn new() -> Result<Rpi> {
        let mut relays = Relays::new()?;
        // start in a known and safe state
        relays.off(MASTER);
        relays.off(BATTERY);
        relays.off(SOLAR);
        Ok(Rpi(relays))
    }

    pub fn mpptc_enable(&mut self) {
        self.0.on(BATTERY);
        // allow the relay to flip on
        thread::sleep(DELAY_RELAY);
        self.0.on(MASTER);
        // allow the controller to boot up
        thread::sleep(DELAY_REBOOT);
        self.0.on(SOLAR);
    }

    pub fn mpptc_disable(&mut self) {
        self.0.off(MASTER);
        // allow the relay to flip, and the voltage converter to drain
        thread::sleep(DELAY_RELAY);
        self.0.off(SOLAR);
        self.0.off(BATTERY);
    }

    pub fn mpptc_reboot(&mut self) {
        self.mpptc_disable();
        self.mpptc_enable();
    }
}
