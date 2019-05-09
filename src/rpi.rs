use rppal::gpio::{Gpio, OutputPin, Result};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum Relay {
    R0 = 26,
    R1 = 19,
    R2 = 13,
    R3 = 6,
}

pub struct Relays {
    io: Gpio,
    r0: OutputPin,
    r1: OutputPin,
    r2: OutputPin,
    r3: OutputPin,
}

impl Relays {
    pub fn new() -> Result<Relays> {
        let io = Gpio::new()?;
        let r0 = io.get(Relay::R0 as u8)?.into_output();
        let r1 = io.get(Relay::R1 as u8)?.into_output();
        let r2 = io.get(Relay::R2 as u8)?.into_output();
        let r3 = io.get(Relay::R3 as u8)?.into_output();
        Ok(Relays { io, r0, r1, r2, r3 })
    }

    fn get(&mut self, r: Relay) -> &mut OutputPin {
        match r {
            Relay::R0 => &mut self.r0,
            Relay::R1 => &mut self.r1,
            Relay::R2 => &mut self.r2,
            Relay::R3 => &mut self.r3,
        }
    }

    pub fn set(&mut self, r: Relay, state: bool) {
        if state {
            self.on(r);
        } else {
            self.off(r);
        }
    }

    pub fn on(&mut self, r: Relay) {
        self.get(r).set_high();
    }

    pub fn off(&mut self, r: Relay) {
        self.get(r).set_low();
    }
}
