use crate::rpi::Rpi;
use anyhow::Result;
use morningstar::prostar_mppt as ps;
use std::{
    ops::Drop,
    time::{Duration, Instant},
};
use tokio::time;

pub struct Connection {
    rpi: Rpi,
    con: Option<ps::Connection>,
    device: String,
    address: u8,
    last_command: Instant,
}

enum Command<'a> {
    WriteCoil(ps::Coil, bool),
    ReadStats(&'a mut ps::Stats),
    ReadSettings(&'a mut ps::Settings),
    WriteSettings(&'a ps::Settings),
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.rpi.mpptc_disable();
    }
}

impl Connection {
    pub fn new(device: String, address: u8) -> Self {
        let mut rpi = log_fatal!(
            Rpi::new(),
            "failed to init gpio {}",
            panic!("failed to init gpio")
        );
        rpi.mpptc_enable();
        Connection { rpi, con: None, device, address, last_command: Instant::now() }
    }

    async fn get_con(&mut self) -> Result<&mut ps::Connection> {
        match self.con {
            Some(ref mut con) => Ok(con),
            None => match ps::Connection::new(&self.device, self.address).await {
                Err(e) => Err(e),
                Ok(con) => {
                    self.con = Some(con);
                    Ok(self.con.as_mut().unwrap())
                }
            },
        }
    }

    async fn eval_command(&mut self, mut command: Command<'_>) -> Result<()> {
        let mut tries: usize = 0;
        loop {
            let r = match self.get_con().await {
                Err(e) => Err(e),
                Ok(con) => match &mut command {
                    Command::WriteCoil(coil, bit) => con.write_coil(*coil, *bit).await,
                    Command::ReadStats(st) => match con.stats().await {
                        Err(e) => Err(e),
                        Ok(s) => {
                            **st = s;
                            Ok(())
                        }
                    },
                    Command::ReadSettings(set) => match con.read_settings().await {
                        Err(e) => Err(e),
                        Ok(s) => {
                            **set = s;
                            Ok(())
                        }
                    },
                    Command::WriteSettings(set) => con.write_settings(set).await,
                },
            };
            match r {
                Ok(r) => break Ok(r),
                Err(e) => {
                    if tries >= 4 {
                        break Err(e);
                    } else if tries >= 3 {
                        self.con = None;
                        self.rpi.mpptc_reboot();
                        tries += 1
                    } else {
                        time::delay_for(Duration::from_millis(1000)).await;
                        self.con = None;
                        tries += 1
                    }
                }
            }
        }
    }

    async fn wait_for_throttle(&mut self) {
        let throttle = Duration::from_secs(1);
        let now = Instant::now();
        let elapsed = now - self.last_command;
        if elapsed < throttle {
            time::delay_for(throttle - elapsed).await
        }
        self.last_command = now;
    }

    pub async fn write_coil(&mut self, coil: ps::Coil, bit: bool) -> Result<()> {
        self.wait_for_throttle().await;
        match (coil, bit) {
            (ps::Coil::ResetControl, true) => {
                // the reset coil will always fail because the controller resets
                // before sending the reply.
                let c = self.get_con().await?;
                let _ = c.write_coil(coil, bit).await;
                Ok(())
            }
            (_, _) => Ok(self.eval_command(Command::WriteCoil(coil, bit)).await?),
        }
    }

    pub async fn read_stats(&mut self) -> Result<ps::Stats> {
        self.wait_for_throttle().await;
        let mut stats = ps::Stats::default();
        self.eval_command(Command::ReadStats(&mut stats)).await?;
        Ok(stats)
    }

    pub async fn read_settings(&mut self) -> Result<ps::Settings> {
        self.wait_for_throttle().await;
        let mut settings = ps::Settings::default();
        self.eval_command(Command::ReadSettings(&mut settings)).await?;
        Ok(settings)
    }

    pub async fn write_settings(&mut self, settings: &ps::Settings) -> Result<()> {
        self.wait_for_throttle().await;
        Ok(self.eval_command(Command::WriteSettings(settings)).await?)
    }

    pub fn rpi(&self) -> &Rpi {
        &self.rpi
    }

    pub fn rpi_mut(&mut self) -> &mut Rpi {
        &mut self.rpi
    }
}
