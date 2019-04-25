use morningstar::{error as mse, prostar_mppt as ps};
use std::{thread::sleep, time::{Instant, Duration}};

pub struct Connection {
    con: Option<ps::Connection>,
    device: String,
    address: u8,
    last_command: Instant,
}

impl Connection {
    pub fn new(device: String, address: u8) -> Self {
        Connection { con: None, device, address, last_command: Instant::now() }
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
                    if tries >= 4 { break Err(e) }
                    else if tries >= 3 {
                        sleep(Duration::from_millis(5000));
                        self.con = None;
                        match self.get_con() {
                            Err(e) => break Err(e),
                            Ok(con) => {
                                let _ = con.write_coil(ps::Coil::ResetControl, true);
                                sleep(Duration::from_millis(5000))
                            }
                        }
                        tries += 1
                    } else {
                        sleep(Duration::from_millis(1000));
                        self.con = None;
                        tries += 1
                    }
                }
            }
        }
    }

    fn wait_for_throttle(&mut self) {
        let throttle = Duration::from_secs(1);
        let now = Instant::now();
        let elapsed = now - self.last_command;
        if elapsed < throttle { sleep(throttle - elapsed) }
        self.last_command = now;
    }

    pub fn write_coil(&mut self, coil: ps::Coil, bit: bool) -> mse::Result<()> {
        self.wait_for_throttle();
        match (coil, bit) {
            (ps::Coil::ResetControl, true) => {
                // the reset coil will always fail because the controller resets
                // before sending the reply.
                let c = self.get_con()?;
                let _ = c.write_coil(coil, bit);
                Ok(())
            },
            (_, _) => Ok(self.eval(move |c| c.write_coil(coil, bit))?)
        }
    }

    pub fn read_stats(&mut self) -> mse::Result<ps::Stats> {
        self.wait_for_throttle();
        Ok(self.eval(|c| c.stats())?)
    }

    pub fn read_settings(&mut self) -> mse::Result<ps::Settings> {
        self.wait_for_throttle();
        Ok(self.eval(|c| c.read_settings())?)
    }

    pub fn write_settings(&mut self, settings: &ps::Settings) -> mse::Result<()> {
        self.wait_for_throttle();
        Ok(self.eval(|c| c.write_settings(settings))?)
    }
}
