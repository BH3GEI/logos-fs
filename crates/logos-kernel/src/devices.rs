use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};

/// A device driver — pluggable hardware/virtual device abstraction.
///
/// RFC 002 §2: `logos://devices/` namespace for physical or virtual devices.
#[async_trait]
pub trait DeviceDriver: Send + Sync {
    fn name(&self) -> &str;
    fn capabilities(&self) -> serde_json::Value;
    async fn state(&self) -> Result<String, VfsError>;
    async fn control(&self, command: &str) -> Result<(), VfsError>;
}

/// Devices namespace — `logos://devices/`.
///
/// URI routing:
///   logos://devices/                           → read: device name list
///   logos://devices/{name}                     → read: device overview (capabilities)
///   logos://devices/{name}/state               → read: current state JSON
///   logos://devices/{name}/capabilities        → read: capabilities JSON
///   logos://devices/{name}/control             → write: send command JSON
pub struct DevicesNs {
    drivers: HashMap<String, Arc<dyn DeviceDriver>>,
}

impl DevicesNs {
    pub fn new() -> Self {
        Self {
            drivers: HashMap::new(),
        }
    }

    pub fn register(&mut self, driver: Arc<dyn DeviceDriver>) {
        self.drivers.insert(driver.name().to_string(), driver);
    }
}

#[async_trait]
impl Namespace for DevicesNs {
    fn name(&self) -> &str {
        "devices"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        match path.len() {
            0 => {
                let names: Vec<&str> = self.drivers.keys().map(|s| s.as_str()).collect();
                Ok(serde_json::to_string(&names).unwrap_or_else(|_| "[]".to_string()))
            }
            1 => {
                let driver = self.drivers.get(path[0]).ok_or_else(|| {
                    VfsError::NotFound(format!("device not found: {}", path[0]))
                })?;
                Ok(driver.capabilities().to_string())
            }
            2 if path[1] == "state" => {
                let driver = self.drivers.get(path[0]).ok_or_else(|| {
                    VfsError::NotFound(format!("device not found: {}", path[0]))
                })?;
                driver.state().await
            }
            2 if path[1] == "capabilities" => {
                let driver = self.drivers.get(path[0]).ok_or_else(|| {
                    VfsError::NotFound(format!("device not found: {}", path[0]))
                })?;
                Ok(driver.capabilities().to_string())
            }
            _ => Err(VfsError::InvalidPath(format!(
                "unexpected devices path: {}",
                path.join("/")
            ))),
        }
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.len() == 2 && path[1] == "control" {
            let driver = self.drivers.get(path[0]).ok_or_else(|| {
                VfsError::NotFound(format!("device not found: {}", path[0]))
            })?;
            return driver.control(content).await;
        }
        Err(VfsError::InvalidPath(format!(
            "devices write only supports .../control: {}",
            path.join("/")
        )))
    }

    async fn patch(&self, path: &[&str], _partial: &str) -> Result<(), VfsError> {
        Err(VfsError::InvalidPath(format!(
            "devices does not support patch: {}",
            path.join("/")
        )))
    }
}

// --- macOS System Driver ---

/// Built-in device driver for the Mac itself.
///
/// Capabilities: volume, battery, screenshot, system info, dark mode.
pub struct MacSystemDriver;

#[async_trait]
impl DeviceDriver for MacSystemDriver {
    fn name(&self) -> &str {
        "system"
    }

    fn capabilities(&self) -> serde_json::Value {
        serde_json::json!({
            "name": "system",
            "description": "macOS system controls",
            "capabilities": [
                {"name": "volume", "read": true, "write": true, "description": "Audio volume (0-100) and mute state"},
                {"name": "battery", "read": true, "write": false, "description": "Battery level and charging status"},
                {"name": "screenshot", "read": false, "write": true, "description": "Capture screen to file"},
                {"name": "info", "read": true, "write": false, "description": "System hardware/software info"},
                {"name": "dark_mode", "read": true, "write": true, "description": "Dark mode on/off"},
            ]
        })
    }

    async fn state(&self) -> Result<String, VfsError> {
        let volume = run_osascript("output volume of (get volume settings)").await;
        let muted = run_osascript("output muted of (get volume settings)").await;
        let dark_mode = run_osascript(
            "tell application \"System Events\" to tell appearance preferences to get dark mode",
        )
        .await;
        let battery = run_cmd("pmset", &["-g", "batt"]).await;

        // Parse battery
        let battery_pct = battery
            .lines()
            .find(|l| l.contains("InternalBattery"))
            .and_then(|l| l.split('\t').nth(1))
            .and_then(|s| s.split('%').next())
            .unwrap_or("unknown")
            .trim()
            .to_string();
        let charging = battery.contains("charging") && !battery.contains("discharging");

        Ok(serde_json::json!({
            "volume": volume.trim().parse::<i32>().unwrap_or(-1),
            "muted": muted.trim() == "true",
            "dark_mode": dark_mode.trim() == "true",
            "battery_percent": battery_pct,
            "battery_charging": charging,
        })
        .to_string())
    }

    async fn control(&self, command: &str) -> Result<(), VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(command).map_err(|e| VfsError::InvalidJson(e.to_string()))?;

        let action = val["action"].as_str().unwrap_or("");

        match action {
            "set_volume" => {
                let level = val["value"].as_i64().unwrap_or(50);
                run_osascript(&format!("set volume output volume {level}")).await;
                Ok(())
            }
            "mute" => {
                run_osascript("set volume output muted true").await;
                Ok(())
            }
            "unmute" => {
                run_osascript("set volume output muted false").await;
                Ok(())
            }
            "screenshot" => {
                let path = val["path"]
                    .as_str()
                    .unwrap_or("/tmp/logos-screenshot.png");
                let output = tokio::process::Command::new("screencapture")
                    .args(["-x", path])
                    .output()
                    .await
                    .map_err(|e| VfsError::Io(format!("screenshot: {e}")))?;
                if !output.status.success() {
                    return Err(VfsError::Io(format!(
                        "screenshot failed: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )));
                }
                Ok(())
            }
            "dark_mode" => {
                let enabled = val["value"].as_bool().unwrap_or(true);
                run_osascript(&format!(
                    "tell application \"System Events\" to tell appearance preferences to set dark mode to {enabled}"
                )).await;
                Ok(())
            }
            _ => Err(VfsError::InvalidPath(format!("unknown action: {action}"))),
        }
    }
}

async fn run_osascript(script: &str) -> String {
    tokio::process::Command::new("osascript")
        .args(["-e", script])
        .output()
        .await
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default()
}

async fn run_cmd(cmd: &str, args: &[&str]) -> String {
    tokio::process::Command::new(cmd)
        .args(args)
        .output()
        .await
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn list_devices() {
        let mut ns = DevicesNs::new();
        ns.register(Arc::new(MacSystemDriver));
        let list = ns.read(&[]).await.unwrap();
        assert!(list.contains("system"));
    }

    #[tokio::test]
    async fn read_capabilities() {
        let mut ns = DevicesNs::new();
        ns.register(Arc::new(MacSystemDriver));
        let caps = ns.read(&["system", "capabilities"]).await.unwrap();
        assert!(caps.contains("volume"));
        assert!(caps.contains("battery"));
    }

    #[tokio::test]
    async fn read_state() {
        let mut ns = DevicesNs::new();
        ns.register(Arc::new(MacSystemDriver));
        let state = ns.read(&["system", "state"]).await.unwrap();
        let val: serde_json::Value = serde_json::from_str(&state).unwrap();
        // Volume should be a number
        assert!(val["volume"].is_number());
        // Battery should be present
        assert!(val["battery_percent"].is_string());
    }

    #[tokio::test]
    async fn unknown_device() {
        let ns = DevicesNs::new();
        assert!(ns.read(&["nonexistent"]).await.is_err());
    }
}
