use anyhow::{Error, Result};
use log::warn;
use morningstar::prostar_mppt as ps;
use std::time::{Duration, Instant};
use tokio::time;

static CMDTO: Duration = Duration::from_secs(30);

pub struct Connection {
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

impl Connection {
    pub async fn new(device: String, address: u8) -> Self {
        Connection { con: None, device, address, last_command: Instant::now() }
    }

    async fn get_con(&mut self) -> Result<&mut ps::Connection> {
        match self.con {
            Some(ref mut con) => Ok(con),
            None => match ps::Connection::new(&self.device, self.address).await {
                Err(e) => {
                    warn!("failed to connect to controller {}", e);
                    Err(e)
                }
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
                    Command::WriteCoil(coil, bit) => {
                        match time::timeout(CMDTO, con.write_coil(*coil, *bit)).await {
                            Err(e) => Err(Error::from(e)),
                            Ok(Err(e)) => Err(e),
                            Ok(Ok(())) => Ok(()),
                        }
                    }
                    Command::ReadStats(st) => {
                        match time::timeout(CMDTO, con.stats()).await {
                            Err(e) => Err(Error::from(e)),
                            Ok(Err(e)) => Err(e),
                            Ok(Ok(s)) => {
                                **st = s;
                                Ok(())
                            }
                        }
                    }
                    Command::ReadSettings(set) => {
                        match time::timeout(CMDTO, con.read_settings()).await {
                            Err(e) => Err(Error::from(e)),
                            Ok(Err(e)) => Err(e),
                            Ok(Ok(s)) => {
                                **set = s;
                                Ok(())
                            }
                        }
                    }
                    Command::WriteSettings(set) =>
                        match time::timeout(CMDTO, con.write_settings(set)).await {
                            Err(e) => Err(Error::from(e)),
                            Ok(Err(e)) => Err(e),
                            Ok(Ok(())) => Ok(())
                        }
                },
            };
            match r {
                Ok(r) => break Ok(r),
                Err(e) => {
                    warn!("failed to eval controller command {}", e);
                    if tries >= 4 {
                        break Err(e);
                    } else {
                        time::sleep(Duration::from_millis(1000)).await;
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
            time::sleep(throttle - elapsed).await
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
}
