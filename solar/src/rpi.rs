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

    fn get(&self, r: Relay) -> &OutputPin {
        match r {
            Relay::R0 => &self.r0,
            Relay::R1 => &self.r1,
            Relay::R2 => &self.r2,
            Relay::R3 => &self.r3,
        }
    }

    fn get_mut(&mut self, r: Relay) -> &mut OutputPin {
        match r {
            Relay::R0 => &mut self.r0,
            Relay::R1 => &mut self.r1,
            Relay::R2 => &mut self.r2,
            Relay::R3 => &mut self.r3,
        }
    }

    fn on(&mut self, r: Relay) {
        self.get_mut(r).set_high();
    }

    fn off(&mut self, r: Relay) {
        self.get_mut(r).set_low();
    }
}

use std::time::Duration;
use tokio::time;

const SOLAR: Relay = Relay::R2;
const BATTERY: Relay = Relay::R1;
const MASTER: Relay = Relay::R3;
const DELAY_RELAY: Duration = Duration::from_millis(500);
const DELAY_REBOOT: Duration = Duration::from_secs(15);

pub struct Rpi(Relays);

impl Rpi {
    pub async fn new() -> Result<Rpi> {
        let mut relays = Relays::new()?;
        // start in a known and safe state
        relays.off(MASTER);
        relays.off(BATTERY);
        relays.off(SOLAR);
        time::delay_for(DELAY_RELAY).await;
        Ok(Rpi(relays))
    }

    pub async fn mpptc_enable(&mut self) {
        self.0.on(BATTERY);
        // allow the relay to flip on
        time::delay_for(DELAY_RELAY).await;
        self.0.on(MASTER);
        // allow the controller to boot up
        time::delay_for(DELAY_REBOOT).await;
        self.0.on(SOLAR);
    }

    pub async fn mpptc_disable(&mut self) {
        self.0.off(MASTER);
        // allow the relay to flip, and the voltage converter to drain
        time::delay_for(DELAY_RELAY).await;
        self.0.off(SOLAR);
        self.0.off(BATTERY);
    }

    pub async fn mpptc_reboot(&mut self) {
        self.mpptc_disable().await;
        self.mpptc_enable().await;
    }

    // the voltage converter will literally explode if it is allowed
    // to run without a load, so we must turn it off if we ever turn
    // off both loads
    async fn disable_non_master(&mut self, to_disable: Relay, other: Relay) {
        if self.0.get(other).is_set_low() {
            self.0.off(MASTER);
            // allow the voltage converter to switch off and drain
            time::delay_for(DELAY_RELAY).await;
        }
        self.0.off(to_disable)
    }

    async fn enable_non_master(&mut self, to_enable: Relay) {
        self.0.on(to_enable);
        time::delay_for(DELAY_RELAY).await;
    }

    pub async fn set_solar(&mut self, b: bool) {
        if b {
            self.enable_non_master(SOLAR).await;
        } else {
            self.disable_non_master(SOLAR, BATTERY).await;
        }
    }

    pub async fn set_battery(&mut self, b: bool) {
        if b {
            self.enable_non_master(BATTERY).await;
        } else {
            self.disable_non_master(BATTERY, SOLAR).await;
        }
    }

    // returns the state of the relay
    pub fn set_master(&mut self, b: bool) -> bool {
        if b {
            if self.0.get(BATTERY).is_set_high() || self.0.get(SOLAR).is_set_high() {
                self.0.on(MASTER);
                true
            } else {
                self.0.off(MASTER); // better safe than fire
                false
            }
        } else {
            self.0.off(MASTER);
            false
        }
    }

    pub fn solar(&self) -> bool {
        self.0.get(SOLAR).is_set_high()
    }

    pub fn battery(&self) -> bool {
        self.0.get(BATTERY).is_set_high()
    }

    pub fn master(&self) -> bool {
        self.0.get(MASTER).is_set_high()
    }
}
