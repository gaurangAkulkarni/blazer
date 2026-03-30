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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CustomSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppSettings {
    pub active_provider: String,
    pub openai: ProviderSettings,
    pub claude: ProviderSettings,
    pub active_skills: Vec<String>,
    pub custom_skills: Vec<CustomSkill>,
}

impl Default for AppSettings {
    fn default() -> Self {
        AppSettings {
            active_provider: "openai".to_string(),
            openai: ProviderSettings {
                api_key: String::new(),
                model: "gpt-4o".to_string(),
                temperature: 0.3,
            },
            claude: ProviderSettings {
                api_key: String::new(),
                model: "claude-sonnet-4-20250514".to_string(),
                temperature: 0.3,
            },
            active_skills: vec!["blazer-engine".to_string()],
            custom_skills: vec![],
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
