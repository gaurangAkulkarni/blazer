use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter};

#[derive(Debug, Deserialize)]
pub struct LlmMessage {
    pub role: String,
    pub content: String,
    /// tool_call_id for role=tool messages
    #[serde(default)]
    pub tool_call_id: Option<String>,
    /// tool_calls for role=assistant messages that triggered tools
    #[serde(default)]
    pub tool_calls: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmProvider {
    Openai,
    Claude,
    Ollama,
}

#[derive(Debug, Deserialize)]
pub struct StreamLlmArgs {
    pub provider: LlmProvider,
    pub api_key: String,
    pub model: String,
    pub temperature: f64,
    pub messages: Vec<LlmMessage>,
    pub stream_id: String,
    /// Optional custom base URL — Ollama (e.g. "http://localhost:11434") or OpenAI-compatible proxy
    pub base_url: Option<String>,
    /// Maximum output tokens (None = use provider default)
    pub max_tokens: Option<u32>,
    /// Tool schemas to send (OpenAI-compat format)
    pub tools: Option<Vec<serde_json::Value>>,
    /// Pre-serialized tool result messages to append before sending
    pub tool_messages: Option<Vec<serde_json::Value>>,
}

#[derive(Serialize, Clone)]
pub struct LlmChunkEvent {
    pub stream_id: String,
    pub chunk: String,
}

#[derive(Serialize, Clone)]
pub struct LlmEndEvent {
    pub stream_id: String,
    pub error: Option<String>,
    pub tokens_in: Option<u32>,
    pub tokens_out: Option<u32>,
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct ToolCallInfo {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Serialize, Clone)]
pub struct LlmToolCallsEvent {
    pub stream_id: String,
    pub tool_calls: Vec<ToolCallInfo>,
    pub assistant_text: String,
}

#[tauri::command]
pub async fn stream_llm(app: AppHandle, args: StreamLlmArgs) -> Result<(), String> {
    match args.provider {
        LlmProvider::Openai => stream_openai(app, args).await,
        LlmProvider::Claude => stream_claude(app, args).await,
        LlmProvider::Ollama => stream_ollama(app, args).await,
    }
}

async fn stream_openai(app: AppHandle, args: StreamLlmArgs) -> Result<(), String> {
    let client = reqwest::Client::new();

    let mut messages: Vec<serde_json::Value> = args
        .messages
        .iter()
        .map(|m| {
            let mut obj = serde_json::json!({ "role": m.role, "content": m.content });
            if let Some(tcid) = &m.tool_call_id {
                obj["tool_call_id"] = serde_json::json!(tcid);
            }
            if let Some(tcs) = &m.tool_calls {
                obj["tool_calls"] = serde_json::json!(tcs);
            }
            obj
        })
        .collect();

    // Append pre-serialized tool result messages if provided
    if let Some(extra_msgs) = args.tool_messages {
        for msg in extra_msgs {
            messages.push(msg);
        }
    }

    let base = args
        .base_url
        .as_deref()
        .map(|u| u.trim_end_matches('/'))
        .filter(|u| !u.is_empty())
        .unwrap_or("https://api.openai.com/v1");
    let endpoint = format!("{}/chat/completions", base);

    // stream_options (include_usage) is OpenAI-only — omit for custom/local endpoints
    let is_official_openai = base.contains("api.openai.com");
    let mut body = serde_json::json!({
        "model": args.model,
        "messages": messages,
        "temperature": args.temperature,
        "stream": true,
    });
    if let Some(max_tok) = args.max_tokens {
        body["max_tokens"] = serde_json::json!(max_tok);
    }
    if is_official_openai {
        body["stream_options"] = serde_json::json!({ "include_usage": true });
    }
    // Inject tool schemas if provided
    if let Some(tools) = args.tools {
        body["tools"] = serde_json::json!(tools);
        body["tool_choice"] = serde_json::json!("auto");
    }

    let response = client
        .post(&endpoint)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", args.api_key))
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        let msg = extract_error_message(&text).unwrap_or_else(|| format!("OpenAI error {status}"));
        let _ = app.emit("llm-end", LlmEndEvent { stream_id: args.stream_id, error: Some(msg), tokens_in: None, tokens_out: None });
        return Ok(());
    }

    let mut stream = response.bytes_stream();
    let mut tokens_in: Option<u32> = None;
    let mut tokens_out: Option<u32> = None;

    // Tool call accumulation
    let mut tool_call_map: std::collections::BTreeMap<u64, (String, String, String)> = std::collections::BTreeMap::new();
    let mut assistant_text = String::new();

    while let Some(item) = stream.next().await {
        let bytes = match item {
            Ok(b) => b,
            Err(e) => {
                let _ = app.emit("llm-end", LlmEndEvent { stream_id: args.stream_id, error: Some(e.to_string()), tokens_in: None, tokens_out: None });
                return Ok(());
            }
        };

        let text = String::from_utf8_lossy(&bytes);
        for line in text.lines() {
            let trimmed = line.strip_prefix("data: ").unwrap_or("").trim();
            if trimmed.is_empty() || trimmed == "[DONE]" {
                continue;
            }
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(trimmed) {
                // Content delta
                if let Some(delta) = json["choices"][0]["delta"]["content"].as_str() {
                    assistant_text.push_str(delta);
                    let _ = app.emit("llm-chunk", LlmChunkEvent {
                        stream_id: args.stream_id.clone(),
                        chunk: delta.to_string(),
                    });
                }
                // Tool call deltas
                if let Some(tcs) = json["choices"][0]["delta"]["tool_calls"].as_array() {
                    for tc in tcs {
                        let idx = tc["index"].as_u64().unwrap_or(0);
                        let entry = tool_call_map.entry(idx).or_insert_with(|| (String::new(), String::new(), String::new()));
                        if let Some(id) = tc["id"].as_str() { if !id.is_empty() { entry.0 = id.to_string(); } }
                        if let Some(name) = tc["function"]["name"].as_str() { if !name.is_empty() { entry.1 = name.to_string(); } }
                        if let Some(fargs) = tc["function"]["arguments"].as_str() { entry.2.push_str(fargs); }
                    }
                }
                // Usage chunk (final chunk with include_usage=true)
                if let Some(usage) = json.get("usage").filter(|u| !u.is_null()) {
                    tokens_in  = usage["prompt_tokens"].as_u64().map(|v| v as u32);
                    tokens_out = usage["completion_tokens"].as_u64().map(|v| v as u32);
                }
            }
        }
    }

    // Emit tool calls event if any were accumulated
    if !tool_call_map.is_empty() {
        let tool_calls: Vec<ToolCallInfo> = tool_call_map.into_values()
            .filter(|(_, name, _)| !name.is_empty())
            .map(|(id, name, arguments)| ToolCallInfo { id, name, arguments })
            .collect();
        let _ = app.emit("llm-tool-calls", LlmToolCallsEvent {
            stream_id: args.stream_id.clone(),
            tool_calls,
            assistant_text: assistant_text.clone(),
        });
    }

    let _ = app.emit("llm-end", LlmEndEvent { stream_id: args.stream_id, error: None, tokens_in, tokens_out });
    Ok(())
}

async fn stream_claude(app: AppHandle, args: StreamLlmArgs) -> Result<(), String> {
    let client = reqwest::Client::new();

    let system_content: Vec<String> = args
        .messages
        .iter()
        .filter(|m| m.role == "system")
        .map(|m| m.content.clone())
        .collect();

    let user_messages: Vec<serde_json::Value> = args
        .messages
        .iter()
        .filter(|m| m.role != "system")
        .map(|m| serde_json::json!({ "role": m.role, "content": m.content }))
        .collect();

    let mut body = serde_json::json!({
        "model": args.model,
        "max_tokens": args.max_tokens.unwrap_or(4096),
        "temperature": args.temperature,
        "messages": user_messages,
        "stream": true,
    });

    if !system_content.is_empty() {
        body["system"] = serde_json::Value::String(system_content.join("\n\n"));
    }

    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("Content-Type", "application/json")
        .header("x-api-key", &args.api_key)
        .header("anthropic-version", "2023-06-01")
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        let msg = extract_error_message(&text).unwrap_or_else(|| format!("Anthropic error {status}"));
        let _ = app.emit("llm-end", LlmEndEvent { stream_id: args.stream_id, error: Some(msg), tokens_in: None, tokens_out: None });
        return Ok(());
    }

    let mut stream = response.bytes_stream();
    let mut tokens_in: Option<u32> = None;
    let mut tokens_out: Option<u32> = None;

    while let Some(item) = stream.next().await {
        let bytes = match item {
            Ok(b) => b,
            Err(e) => {
                let _ = app.emit("llm-end", LlmEndEvent { stream_id: args.stream_id, error: Some(e.to_string()), tokens_in: None, tokens_out: None });
                return Ok(());
            }
        };

        let text = String::from_utf8_lossy(&bytes);
        for line in text.lines() {
            let trimmed = line.strip_prefix("data: ").unwrap_or("").trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(trimmed) {
                match json["type"].as_str() {
                    Some("content_block_delta") => {
                        if let Some(text) = json["delta"]["text"].as_str() {
                            let _ = app.emit("llm-chunk", LlmChunkEvent {
                                stream_id: args.stream_id.clone(),
                                chunk: text.to_string(),
                            });
                        }
                    }
                    // input token count arrives in message_start
                    Some("message_start") => {
                        tokens_in = json["message"]["usage"]["input_tokens"].as_u64().map(|v| v as u32);
                    }
                    // output token count arrives in message_delta
                    Some("message_delta") => {
                        tokens_out = json["usage"]["output_tokens"].as_u64().map(|v| v as u32);
                    }
                    _ => {}
                }
            }
        }
    }

    let _ = app.emit("llm-end", LlmEndEvent { stream_id: args.stream_id, error: None, tokens_in, tokens_out });
    Ok(())
}

/// Ollama uses the OpenAI-compatible `/v1/chat/completions` endpoint.
/// No Authorization header is needed; base_url defaults to http://localhost:11434.
async fn stream_ollama(app: AppHandle, args: StreamLlmArgs) -> Result<(), String> {
    let client = reqwest::Client::new();

    let base = args
        .base_url
        .as_deref()
        .unwrap_or("http://localhost:11434")
        .trim_end_matches('/');
    let url = format!("{}/v1/chat/completions", base);

    let mut messages: Vec<serde_json::Value> = args
        .messages
        .iter()
        .map(|m| {
            let mut obj = serde_json::json!({ "role": m.role, "content": m.content });
            if let Some(tcid) = &m.tool_call_id {
                obj["tool_call_id"] = serde_json::json!(tcid);
            }
            if let Some(tcs) = &m.tool_calls {
                obj["tool_calls"] = serde_json::json!(tcs);
            }
            obj
        })
        .collect();

    // Append pre-serialized tool result messages if provided
    if let Some(extra_msgs) = args.tool_messages {
        for msg in extra_msgs {
            messages.push(msg);
        }
    }

    let mut body = serde_json::json!({
        "model": args.model,
        "messages": messages,
        "temperature": args.temperature,
        "stream": true,
    });
    if let Some(max_tok) = args.max_tokens {
        body["max_tokens"] = serde_json::json!(max_tok);
    }
    // Inject tool schemas if provided
    if let Some(tools) = args.tools {
        body["tools"] = serde_json::json!(tools);
        body["tool_choice"] = serde_json::json!("auto");
    }

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        let msg = extract_error_message(&text)
            .unwrap_or_else(|| format!("Ollama error {status}: {text}"));
        let _ = app.emit("llm-end", LlmEndEvent {
            stream_id: args.stream_id, error: Some(msg), tokens_in: None, tokens_out: None,
        });
        return Ok(());
    }

    let mut stream = response.bytes_stream();
    let mut tokens_in: Option<u32> = None;
    let mut tokens_out: Option<u32> = None;

    // Tool call accumulation
    let mut tool_call_map: std::collections::BTreeMap<u64, (String, String, String)> = std::collections::BTreeMap::new();
    let mut assistant_text = String::new();

    while let Some(item) = stream.next().await {
        let bytes = match item {
            Ok(b) => b,
            Err(e) => {
                let _ = app.emit("llm-end", LlmEndEvent {
                    stream_id: args.stream_id, error: Some(e.to_string()), tokens_in: None, tokens_out: None,
                });
                return Ok(());
            }
        };

        let text = String::from_utf8_lossy(&bytes);
        for line in text.lines() {
            let trimmed = line.strip_prefix("data: ").unwrap_or("").trim();
            if trimmed.is_empty() || trimmed == "[DONE]" {
                continue;
            }
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(trimmed) {
                if let Some(delta) = json["choices"][0]["delta"]["content"].as_str() {
                    assistant_text.push_str(delta);
                    let _ = app.emit("llm-chunk", LlmChunkEvent {
                        stream_id: args.stream_id.clone(),
                        chunk: delta.to_string(),
                    });
                }
                // Tool call deltas
                if let Some(tcs) = json["choices"][0]["delta"]["tool_calls"].as_array() {
                    for tc in tcs {
                        let idx = tc["index"].as_u64().unwrap_or(0);
                        let entry = tool_call_map.entry(idx).or_insert_with(|| (String::new(), String::new(), String::new()));
                        if let Some(id) = tc["id"].as_str() { if !id.is_empty() { entry.0 = id.to_string(); } }
                        if let Some(name) = tc["function"]["name"].as_str() { if !name.is_empty() { entry.1 = name.to_string(); } }
                        if let Some(fargs) = tc["function"]["arguments"].as_str() { entry.2.push_str(fargs); }
                    }
                }
                // Ollama may include usage in the final chunk
                if let Some(usage) = json.get("usage").filter(|u| !u.is_null()) {
                    tokens_in  = usage["prompt_tokens"].as_u64().map(|v| v as u32);
                    tokens_out = usage["completion_tokens"].as_u64().map(|v| v as u32);
                }
            }
        }
    }

    // Emit tool calls event if any were accumulated
    if !tool_call_map.is_empty() {
        let tool_calls: Vec<ToolCallInfo> = tool_call_map.into_values()
            .filter(|(_, name, _)| !name.is_empty())
            .map(|(id, name, arguments)| ToolCallInfo { id, name, arguments })
            .collect();
        let _ = app.emit("llm-tool-calls", LlmToolCallsEvent {
            stream_id: args.stream_id.clone(),
            tool_calls,
            assistant_text: assistant_text.clone(),
        });
    }

    let _ = app.emit("llm-end", LlmEndEvent { stream_id: args.stream_id, error: None, tokens_in, tokens_out });
    Ok(())
}

fn extract_error_message(text: &str) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(text).ok()?;
    json["error"]["message"].as_str().map(|s| s.to_string())
}

/// Fetch available models from an OpenAI-compatible /models endpoint.
/// Routed through Rust/reqwest so it works regardless of WebView CORS or ATS restrictions.
#[tauri::command]
pub async fn fetch_openai_models(base_url: String, api_key: Option<String>) -> Result<Vec<String>, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(6))
        .build()
        .map_err(|e| e.to_string())?;

    let base = base_url.trim_end_matches('/');
    let url = format!("{}/models", base);

    let mut req = client.get(&url).header("Content-Type", "application/json");
    if let Some(key) = &api_key {
        if !key.is_empty() {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
    }

    let res = req.send().await.map_err(|e| format!("Request failed: {e}"))?;
    if !res.status().is_success() {
        return Err(format!("HTTP {}", res.status().as_u16()));
    }

    let json: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;

    // Handle OpenAI format { "data": [{ "id": "..." }] }
    // and some servers that return { "models": [{ "id"|"name": "..." }] }
    let items = json["data"]
        .as_array()
        .or_else(|| json["models"].as_array())
        .cloned()
        .unwrap_or_default();

    let mut names: Vec<String> = items
        .iter()
        .filter_map(|m| {
            m["id"].as_str().or_else(|| m["name"].as_str()).map(|s| s.to_string())
        })
        .filter(|s| !s.is_empty())
        .collect();

    names.sort();
    Ok(names)
}
