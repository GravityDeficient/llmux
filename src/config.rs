//! Configuration for llmux v2.
//!
//! All model lifecycle management is delegated to user-provided scripts.
//! llmux only handles request routing, draining, and policy decisions.
//!
//! Supports both YAML and JSON config files (detected by extension).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Top-level configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Models to manage
    pub models: HashMap<String, ModelConfig>,

    /// Switch policy configuration
    #[serde(default)]
    pub policy: PolicyConfig,

    /// Proxy port
    #[serde(default = "default_port")]
    pub port: u16,

    /// Address to bind the HTTP server to.
    #[serde(default = "default_bind_address")]
    pub bind_address: String,

    /// Authentication for inference and control traffic.
    #[serde(default)]
    pub auth: AuthConfig,

    /// Request-aware orchestration settings.
    #[serde(default)]
    pub orchestration: OrchestrationConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Optional bearer token for inference routes. Use `env:NAME` to read it
    /// from an environment variable when llmux starts.
    #[serde(default)]
    pub inference_bearer_token: Option<String>,

    /// Optional, independent bearer token for `/control/v1/*`.
    #[serde(default)]
    pub control_bearer_token: Option<String>,
}

impl AuthConfig {
    pub fn resolve_inference_token(&self) -> Result<Option<String>> {
        resolve_token(
            self.inference_bearer_token.as_deref(),
            "inference_bearer_token",
        )
    }

    pub fn resolve_control_token(&self) -> Result<Option<String>> {
        resolve_token(self.control_bearer_token.as_deref(), "control_bearer_token")
    }
}

fn resolve_token(value: Option<&str>, field: &str) -> Result<Option<String>> {
    let Some(value) = value else { return Ok(None) };
    let resolved = if let Some(name) = value.strip_prefix("env:") {
        std::env::var(name)
            .with_context(|| format!("{field} references missing environment variable {name}"))?
    } else {
        value.to_string()
    };
    anyhow::ensure!(!resolved.is_empty(), "{field} must not be empty");
    Ok(Some(resolved))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationConfig {
    /// Rolling lease renewed when an interactive response finishes.
    #[serde(default = "default_interactive_lease_secs")]
    pub interactive_lease_secs: u64,

    /// Maximum time an inactive background request waits.
    #[serde(default = "default_background_max_wait_secs")]
    pub background_max_wait_secs: u64,

    /// Optional state file used to persist a model pin across restarts.
    #[serde(default)]
    pub state_path: Option<PathBuf>,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            interactive_lease_secs: default_interactive_lease_secs(),
            background_max_wait_secs: default_background_max_wait_secs(),
            state_path: None,
        }
    }
}

/// Configuration for a single model.
///
/// Each model is defined by a target port (where the inference server listens)
/// and three lifecycle hooks. Hooks can be either a path to an executable script
/// or an inline shell script:
///
/// ```yaml
/// models:
///   llama:
///     port: 8001
///     wake: ./scripts/wake-llama.sh
///     sleep: |
///       kill $(cat /tmp/llama.pid)
///       rm /tmp/llama.pid
///     alive: curl -sf http://localhost:8001/health
/// ```
///
/// All hooks are executed via `sh -c` with LLMUX_MODEL set in the environment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Port where the model's inference server listens
    pub port: u16,

    /// Script to bring the model to a running state (idempotent).
    /// Can be a path to an executable or an inline shell script.
    /// Called with LLMUX_MODEL env var set to the model name.
    /// Must exit 0 when the model is ready to serve requests.
    pub wake: String,

    /// Script to put the model to sleep / free resources.
    /// Can be a path to an executable or an inline shell script.
    /// Called with LLMUX_MODEL env var set to the model name.
    /// Must exit 0 when the model is fully stopped/sleeping.
    pub sleep: String,

    /// Script to check if the model is alive and healthy.
    /// Can be a path to an executable or an inline shell script.
    /// Called with LLMUX_MODEL env var set to the model name.
    /// Exit 0 = healthy, non-zero = unhealthy.
    pub alive: String,

    /// Optional operator-facing data returned by the control API. It does
    /// not affect scheduling or hooks.
    #[serde(default)]
    pub metadata: ModelMetadata,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelMetadata {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub topology: Option<String>,
    #[serde(default)]
    pub context_length: Option<u64>,
    #[serde(default)]
    pub quantization: Option<String>,
    #[serde(default)]
    pub speculative_decoding: Option<String>,
    #[serde(default)]
    pub impact: Vec<String>,
}

fn default_port() -> u16 {
    3000
}

fn default_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_interactive_lease_secs() -> u64 {
    600
}

fn default_background_max_wait_secs() -> u64 {
    1800
}

impl Config {
    /// Load configuration from a YAML or JSON file (detected by extension).
    pub async fn from_file(path: &Path) -> Result<Self> {
        let contents = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        match ext {
            "yaml" | "yml" => serde_yaml::from_str(&contents)
                .with_context(|| format!("Failed to parse YAML config: {}", path.display())),
            _ => serde_json::from_str(&contents)
                .with_context(|| format!("Failed to parse JSON config: {}", path.display())),
        }
    }
}

/// Policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// Request timeout in seconds. None = unlimited (requests wait forever).
    #[serde(default)]
    pub request_timeout_secs: Option<u64>,

    /// Whether to drain in-flight requests before switching
    #[serde(default = "default_drain_before_switch")]
    pub drain_before_switch: bool,

    /// Minimum seconds a model must stay active before it can be put to sleep.
    /// Prevents rapid wake/sleep thrashing. Default: 0 (no minimum).
    #[serde(default)]
    pub min_active_secs: u64,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            request_timeout_secs: None,
            drain_before_switch: default_drain_before_switch(),
            min_active_secs: 0,
        }
    }
}

fn default_drain_before_switch() -> bool {
    true
}

impl PolicyConfig {
    pub fn build_policy(&self) -> Box<dyn crate::policy::SwitchPolicy> {
        Box::new(crate::policy::FifoPolicy::new(
            self.request_timeout_secs.map(Duration::from_secs),
            self.drain_before_switch,
            Duration::from_secs(self.min_active_secs),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json() {
        let json = r#"{
            "models": {
                "llama": {
                    "port": 8001,
                    "wake": "./scripts/wake-llama.sh",
                    "sleep": "./scripts/sleep-llama.sh",
                    "alive": "./scripts/alive-llama.sh"
                },
                "mistral": {
                    "port": 8002,
                    "wake": "./scripts/wake-mistral.sh",
                    "sleep": "./scripts/sleep-mistral.sh",
                    "alive": "./scripts/alive-mistral.sh"
                }
            },
            "policy": {
                "request_timeout_secs": 30
            },
            "port": 3000
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.models.len(), 2);
        assert_eq!(config.models["llama"].port, 8001);
        assert_eq!(config.policy.request_timeout_secs, Some(30));
    }

    #[test]
    fn test_parse_yaml() {
        let yaml = r#"
models:
  llama:
    port: 8001
    wake: ./scripts/wake-llama.sh
    sleep: |
      kill $(cat /tmp/llama.pid)
      rm /tmp/llama.pid
    alive: curl -sf http://localhost:8001/health
policy:
  request_timeout_secs: 60
port: 4000
"#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.models.len(), 1);
        assert_eq!(config.models["llama"].port, 8001);
        assert_eq!(config.models["llama"].wake, "./scripts/wake-llama.sh");
        assert!(config.models["llama"].sleep.contains("kill"));
        assert_eq!(config.policy.request_timeout_secs, Some(60));
        assert_eq!(config.port, 4000);
    }

    #[test]
    fn test_defaults() {
        let json = r#"{
            "models": {
                "llama": {
                    "port": 8001,
                    "wake": "./wake.sh",
                    "sleep": "./sleep.sh",
                    "alive": "./alive.sh"
                }
            }
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.port, 3000);
        assert_eq!(config.bind_address, "0.0.0.0");
        assert_eq!(config.policy.request_timeout_secs, None);
        assert!(config.policy.drain_before_switch);
        assert_eq!(config.policy.min_active_secs, 0);
        assert_eq!(config.orchestration.interactive_lease_secs, 600);
        assert_eq!(config.orchestration.background_max_wait_secs, 1800);
    }

    #[test]
    fn test_auth_token_from_environment() {
        unsafe { std::env::set_var("LLMUX_TEST_TOKEN", "secret") };
        let auth = AuthConfig {
            inference_bearer_token: Some("env:LLMUX_TEST_TOKEN".into()),
            control_bearer_token: None,
        };
        assert_eq!(
            auth.resolve_inference_token().unwrap().as_deref(),
            Some("secret")
        );
        unsafe { std::env::remove_var("LLMUX_TEST_TOKEN") };
    }
}
