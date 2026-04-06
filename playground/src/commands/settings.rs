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
    /// DuckDB extension type: "postgres", "mysql", "sqlite", "httpfs", "spatial", etc.
    pub ext_type: String,
    /// Connection string (empty for non-DB extensions like httpfs/spatial)
    #[serde(default)]
    pub connection_string: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
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
