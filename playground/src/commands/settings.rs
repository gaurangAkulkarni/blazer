use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tauri::AppHandle;
use tauri::Manager;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProviderSettings {
    pub api_key: String,
    pub model: String,
    pub temperature: f32,
    /// Optional custom base URL for OpenAI-compatible endpoints (LM Studio, vLLM, Azure, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OllamaSettings {
    pub base_url: String,
    pub model: String,
    pub temperature: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CustomSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionAlias {
    pub id: String,
    pub name: String,
    /// DuckDB extension type: "postgres", "mysql", "sqlite", "delta", "iceberg", etc.
    pub ext_type: String,
    /// Connection string / table path (empty for non-DB extensions like httpfs/spatial)
    #[serde(default)]
    pub connection_string: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    // ── Azure credentials (delta / iceberg on ADLS Gen2) ──────────────────────
    /// Auth method: "none" | "service_principal" | "account_key" | "sas" | "azure_cli"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_auth: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_client_secret: Option<String>,
    /// Full Azure storage connection string or SAS URL (account_key / sas auth)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_storage_connection_string: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppSettings {
    pub active_provider: String,
    pub openai: ProviderSettings,
    pub claude: ProviderSettings,
    #[serde(default)]
    pub ollama: OllamaSettings,
    pub active_skills: Vec<String>,
    pub custom_skills: Vec<CustomSkill>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub show_follow_up_chips: Option<bool>,
    #[serde(default = "default_context_history_limit")]
    pub context_history_limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calling_enabled: Option<bool>,
    #[serde(default)]
    pub connections: Vec<ConnectionAlias>,
}

fn default_context_history_limit() -> u32 { 20 }

impl Default for OllamaSettings {
    fn default() -> Self {
        OllamaSettings {
            base_url: "http://localhost:11434".to_string(),
            model: "llama3.2".to_string(),
            temperature: 0.3,
        }
    }
}

impl Default for AppSettings {
    fn default() -> Self {
        AppSettings {
            active_provider: "openai".to_string(),
            openai: ProviderSettings {
                api_key: String::new(),
                model: "gpt-4o".to_string(),
                temperature: 0.3,
                base_url: None,
            },
            claude: ProviderSettings {
                api_key: String::new(),
                model: "claude-sonnet-4-20250514".to_string(),
                temperature: 0.3,
                base_url: None,
            },
            ollama: OllamaSettings::default(),
            active_skills: vec!["blazer-engine".to_string()],
            custom_skills: vec![],
            show_follow_up_chips: None,
            context_history_limit: 20,
            max_output_tokens: None,
            tool_calling_enabled: None,
            connections: vec![],
        }
    }
}

fn settings_path(app: &AppHandle) -> PathBuf {
    let data_dir = app.path().app_data_dir().unwrap_or_else(|_| {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("blazer")
    });
    fs::create_dir_all(&data_dir).ok();
    data_dir.join("settings.json")
}

#[tauri::command]
pub fn load_settings(app: AppHandle) -> AppSettings {
    let path = settings_path(&app);
    if let Ok(contents) = fs::read_to_string(&path) {
        serde_json::from_str(&contents).unwrap_or_default()
    } else {
        AppSettings::default()
    }
}

#[tauri::command]
pub fn save_settings(app: AppHandle, settings: AppSettings) -> Result<(), String> {
    let path = settings_path(&app);
    let json = serde_json::to_string_pretty(&settings).map_err(|e| e.to_string())?;
    fs::write(&path, json).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_settings() {
        let s = AppSettings::default();
        assert_eq!(s.active_provider, "openai");
        assert_eq!(s.context_history_limit, 20);
        assert!(s.active_skills.contains(&"blazer-engine".to_string()));
        assert!(s.connections.is_empty());
        assert!(s.custom_skills.is_empty());
    }

    #[test]
    fn test_default_ollama_settings() {
        let o = OllamaSettings::default();
        assert_eq!(o.base_url, "http://localhost:11434");
        assert_eq!(o.model, "llama3.2");
    }

    #[test]
    fn test_settings_serialize_deserialize_roundtrip() {
        let original = AppSettings::default();
        let json = serde_json::to_string(&original).unwrap();
        let parsed: AppSettings = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.active_provider, original.active_provider);
        assert_eq!(parsed.context_history_limit, original.context_history_limit);
    }

    #[test]
    fn test_settings_deserialize_missing_fields_uses_defaults() {
        let json = r#"{"active_provider":"claude","openai":{"api_key":"k","model":"gpt-4","temperature":0.5},"claude":{"api_key":"c","model":"claude-3","temperature":0.5},"active_skills":[],"custom_skills":[]}"#;
        let s: AppSettings = serde_json::from_str(json).unwrap();
        assert_eq!(s.active_provider, "claude");
        assert_eq!(s.context_history_limit, 20); // default
        assert!(s.connections.is_empty()); // default
    }

    #[test]
    fn test_provider_settings_fields() {
        let p = ProviderSettings {
            api_key: "sk-test".to_string(),
            model: "gpt-4o".to_string(),
            temperature: 0.3,
            base_url: Some("https://custom.api".to_string()),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("sk-test"));
        assert!(json.contains("gpt-4o"));
        assert!(json.contains("custom.api"));
    }

    #[test]
    fn test_provider_settings_no_base_url_omits_field() {
        let p = ProviderSettings {
            api_key: "key".to_string(),
            model: "m".to_string(),
            temperature: 0.1,
            base_url: None,
        };
        let json = serde_json::to_string(&p).unwrap();
        // base_url should be omitted (skip_serializing_if = Option::is_none)
        assert!(!json.contains("base_url"));
    }

    #[test]
    fn test_custom_skill_fields() {
        let skill = CustomSkill {
            id: "skill-1".to_string(),
            name: "My Skill".to_string(),
            description: "Does things".to_string(),
            prompt: "You are a helper".to_string(),
        };
        assert_eq!(skill.id, "skill-1");
        assert_eq!(skill.name, "My Skill");
    }

    #[test]
    fn test_connection_alias_defaults() {
        let json = r#"{"id":"c1","name":"My DB","ext_type":"postgres"}"#;
        let c: ConnectionAlias = serde_json::from_str(json).unwrap();
        assert_eq!(c.connection_string, ""); // default
        assert!(c.description.is_none());
        assert!(c.azure_auth.is_none());
    }
}
