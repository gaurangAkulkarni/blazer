# Blazer Studio — Phase 1: Tool-Calling + Auto-Profiling

> Add structured tool-calling bridge between LLM and DuckDB, plus auto-profiling on file attach.
> Provider-agnostic: works with any model/provider that supports function calling (Ollama, LM Studio, MLX, cloud).
> ALL existing functionality must remain unchanged. This is additive — tool-call path runs alongside existing text-based path.

---

## 1. Rust — Tool Dispatcher

Create `src-tauri/src/tools.rs`

### Tool schemas (sent to LLM via provider API)

5 tools, all following OpenAI function-calling format (works with Ollama, LM Studio, MLX, OpenAI, Anthropic):

| Tool | Purpose | Required args |
|------|---------|---------------|
| `run_sql` | Execute DuckDB SQL, return rows as JSON + column metadata | `sql: string`, optional `limit: int (default 100)` |
| `describe_tables` | List all attached tables/files with column names and types. LLM should call this FIRST. | none |
| `get_sample_rows` | First N rows from a table for quick inspection | `table: string`, optional `n: int (default 10)` |
| `column_stats` | Statistical profile: count, nulls, distinct, min, max, mean, median, top 5 values | `table: string`, optional `columns: string[]` |
| `export_result` | Save last result to file | `format: csv|parquet|json|xlsx`, `filename: string` |

### Dispatcher

```rust
pub fn dispatch_tool_call(name: &str, arguments: Value, db: &DuckDbState) -> Result<Value, String> {
    match name {
        "run_sql" => handle_run_sql(arguments, db),
        "describe_tables" => handle_describe_tables(arguments, db),
        "get_sample_rows" => handle_get_sample_rows(arguments, db),
        "column_stats" => handle_column_stats(arguments, db),
        "export_result" => handle_export_result(arguments, db),
        _ => Ok(json!({"success": false, "error": format!("Unknown tool: {}", name)})),
    }
}
```

**Critical**: Errors returned as `{"success": false, "error": "..."}` JSON — never as Rust `Err()`. The LLM needs to see errors to self-correct in the agentic loop.

### Handler implementations

**run_sql**: Take `sql` + optional `limit`. Add `LIMIT` if not present and not DDL. Execute via existing DuckDB query function. Return `{success, columns: [{name, type}], rows, row_count, total_rows, truncated, sql_executed}`. On error: `{success: false, error, sql_attempted}`.

**describe_tables**: Query `information_schema.columns` grouped by table. Also include attached files from existing state. Return `{tables: [{name, columns, column_count}], attached_files}`.

**get_sample_rows**: Sanitize table ref, build `SELECT * FROM {table} LIMIT {n}`, delegate to `handle_run_sql`.

**column_stats**: For each column, run two queries: (1) COUNT, nulls, distinct, min, max, mean (via TRY_CAST to DOUBLE), median. (2) Top 5 frequent values. Handle errors per-column (don't fail entire call if one column errors). Return `{table, column_profiles: [{column, total_count, null_count, null_percentage, distinct_count, min, max, mean, median, top_values}]}`.

**export_result**: Sanitize filename (strip path separators, special chars). Use DuckDB `COPY` to export dir. Return `{success, path, format}`.

### SQL injection prevention

```rust
fn sanitize_table_ref(table: &str) -> String {
    if table.contains('.') && !table.contains('\'') {
        format!("read_csv_auto('{}')", table.replace('\'', ""))
    } else {
        format!("\"{}\"", table.replace('"', ""))
    }
}
```

### Tauri command registration

```rust
#[tauri::command]
async fn execute_tool_call(name: String, arguments: Value, state: State<'_, DuckDbState>) -> Result<Value, String> {
    tools::dispatch_tool_call(&name, arguments, &state)
}
```

Add to invoke_handler alongside existing commands.

---

## 2. Frontend — Tool-Call Integration

### 2.1 Runtime capability detection

No hardcoded model list. Detect at runtime with session caching:

```typescript
// src/lib/llm/toolCallSupport.ts
const sessionCache = new Map<string, boolean>();

export function shouldSendTools(provider: string, model: string): boolean {
  const key = `${provider}::${model}`;
  if (sessionCache.has(key)) return sessionCache.get(key)!;
  return true; // optimistic first attempt
}

export function cacheToolCallSupport(provider: string, model: string, supported: boolean) {
  sessionCache.set(`${provider}::${model}`, supported);
}
```

Strategy: Send `tools[]` with first request. If response contains `tool_calls` → cache as supported. If plain text only → cache as unsupported, fall back to existing text-based path for rest of session.

### 2.2 Inject tools into provider request

In your existing provider API call, conditionally add tools:

```typescript
if (options.toolCallingEnabled && shouldSendTools(provider, model)) {
  body.tools = getToolSchemas(); // mirrors Rust schemas as JSON
}
```

This works uniformly across Ollama (`/api/chat`), LM Studio (`/v1/chat/completions`), MLX (`/v1/chat/completions`) — all accept OpenAI-format `tools[]`.

### 2.3 Stream parser extension

Extend existing stream parser to detect `tool_calls` alongside `content`:

- Ollama: `data.message.tool_calls`
- OpenAI-compatible (LM Studio, MLX): `data.choices[0].delta.tool_calls`

If `tool_calls` present → trigger tool-call path. If only `content` → existing text path. Parse `arguments` from string to object if provider sends it as string.

### 2.4 Tool execution via Tauri IPC

```typescript
// src/lib/tools/executeToolCall.ts
export async function executeToolCall(name: string, args: Record<string, any>): Promise<any> {
  try {
    return await invoke('execute_tool_call', { name, arguments: args });
  } catch (error) {
    return { success: false, error: String(error) };
  }
}
```

---

## 3. Agentic Loop Upgrade

### What stays (DO NOT MODIFY)

All existing refs and logic: `agenticActiveRef`, `agenticIterationRef`, `agenticPlanStepsRef`, `MAX_AGENTIC_ITER`, `AgenticTimeline` component, DONE detection + heuristics, bare DONE rejection, plan persistence in SQLite, 800ms debounce (still needed for text path).

### What changes — branch in stream handler

```typescript
async function handleStreamResponse(response, provider, model, history) {
  let content = '';
  let toolCalls = [];
  let usedToolCalling = false;

  await processStream(response, provider, {
    onToken(token) { content += token; appendToChat(token); },
    onToolCalls(tcs) { usedToolCalling = true; toolCalls.push(...tcs); cacheToolCallSupport(provider, model, true); },
    async onDone() {
      if (usedToolCalling && toolCalls.length > 0) {
        await handleToolCallContinuation(toolCalls, history, content, provider, model);
      } else {
        if (!usedToolCalling) cacheToolCallSupport(provider, model, false);
        handleExistingTextResponse(content); // existing path unchanged
      }
    }
  });
}
```

### Tool-call continuation

1. Execute each tool call via `executeToolCall()` — show running/success/error in chat UI
2. Increment `agenticIterationRef`, check `MAX_AGENTIC_ITER`
3. Build continuation messages: append assistant message (with `tool_calls` field) + `role: "tool"` messages with JSON results
4. Send back to LLM for next turn
5. Process next response recursively — might be more tool calls or final text with DONE

Key difference from text path: instead of injecting markdown tables into a user message, use proper `role: "tool"` messages with structured JSON. Same iteration tracking, same termination logic.

---

## 4. Auto-Profiling on File Attach

### Trigger

In existing file attachment handler, after registering files with DuckDB:

```typescript
if (toolCallingEnabled && shouldSendTools(provider, model)) {
  await triggerAutoProfile(files);
}
```

### Profile prompt

```typescript
const prompt = `New data files attached:\n${fileList}\n
Analyze in order:
1. Call describe_tables for all columns and types
2. Call get_sample_rows for each file (5 rows)
3. Call column_stats on 3-5 key columns per file (numeric, date, low-cardinality categorical)
4. Write a concise profile: what the data represents, row/column counts, key columns, data quality issues (nulls, outliers), join keys if multiple files, 3 analytical questions this data could answer. 4-6 paragraphs max, specific numbers.`;
```

Send through normal `sendMessage` flow with `isAutoProfile: true` flag.

### Auto-profile UI (`AutoProfileCard.tsx`)

Visually distinct from regular messages:

- **Header**: sparkle icon + "Data profile" + file count
- **Body**: streaming LLM content rendered with existing markdown renderer
- **Tool chips**: collapsible chips showing each tool call (name, row count, duration)
- **Action buttons**: "Explore distributions", "Find anomalies", "Show trends" — each sends a follow-up message to chat via existing input

### Tool call chip (`ToolCallChip.tsx`)

Compact inline display: icon (spinner/check/x) + summary text (`SQL · 5 rows · 23ms`) + expand chevron. Expanded view shows raw JSON result in `<pre>` block. Max height 12rem with overflow scroll.

---

## 5. Data Analyst Skill

### Built-in skill definition

```json
{
  "id": "data-analyst",
  "name": "Data Analyst",
  "description": "Private data analyst. Uses DuckDB tools for analysis — all local.",
  "isBuiltIn": true,
  "autoActivateOnToolCalling": true
}
```

### System prompt

```
You are a senior data analyst in Blazer Studio. Analyze data using DuckDB SQL through the provided tools. All data stays local.

Tools: describe_tables (call FIRST), get_sample_rows, column_stats, run_sql, export_result.

Workflow: describe_tables → get_sample_rows → column_stats on key columns → run_sql for analysis → explain findings.

DuckDB notes: read_csv_auto(), read_parquet(), st_read() for files. ILIKE, QUALIFY, SAMPLE, EXCLUDE supported. Use CTEs for complex queries.

Style: Concise, specific numbers ("grew 23% from $4.2M to $5.2M"), flag unexpected findings, note data quality issues upfront, suggest chart types when relevant.
```

### Auto-activation

When tool calling is enabled for the current model, auto-activate this skill. When tool calling is OFF, deactivate and restore user's previous skill selection. Layer user's custom skill on top if they have one selected alongside.

---

## 6. Settings — Tool Calling Toggle

Single toggle in existing chat/model settings:

```
Tool calling  [ON/OFF]
Let the AI call data analysis tools directly.
Auto-profiling requires this to be enabled.
```

Default: ON.

When OFF: no tools sent, no auto-profiling, no Data Analyst skill auto-activation. Blazer behaves exactly as it does today.

When toggled mid-session: clear session capability cache.

---

## 7. New & Modified Files

### New:
```
src-tauri/src/tools.rs              — Schemas, dispatcher, 5 handlers
src/lib/llm/toolCallSupport.ts      — Runtime detection + session cache
src/lib/llm/toolSchemas.ts          — JSON tool schemas
src/lib/tools/executeToolCall.ts    — Tauri IPC wrapper
src/lib/autoProfile.ts             — Auto-profile trigger
src/components/AutoProfileCard.tsx  — Profile message UI
src/components/ToolCallChip.tsx     — Tool call display chip
```

### Modified:
```
src-tauri/src/main.rs               — Register execute_tool_call, add mod tools
src/lib/llm/provider.ts            — Add tools[] to request body
src/hooks/useChat.ts               — Branch handleStreamResponse
src/components/ChatMessage.tsx      — Render AutoProfileCard + ToolCallChip
src/hooks/useFileAttachment.ts     — Call triggerAutoProfile
src/lib/skills.ts                  — Register data-analyst skill
src/components/Settings.tsx         — Add tool calling toggle
```

---

## 8. Testing Checklist

### No regressions:
- [ ] Text chat with all existing providers works unchanged (tool calling OFF)
- [ ] Auto-run mode works unchanged (tool calling OFF)
- [ ] Agentic mode text-based works unchanged (tool calling OFF)
- [ ] Skills, Data Explorer, SQL Playground, connections unchanged
- [ ] xlsx folder reading + Parquet export unchanged

### Tool-calling path:
- [ ] Single tool call (run_sql) returns results
- [ ] Multi-tool turn (describe → sample → query) works
- [ ] Agentic loop with plan steps and DONE via tool calls
- [ ] Model without tool support → graceful fallback to text path, no crash
- [ ] Fallback cached per session
- [ ] Tool errors returned to LLM, model self-corrects
- [ ] Multiple tool calls in single response handled sequentially
- [ ] SQL injection: sanitized table names with quotes/semicolons/paths

### Auto-profiling:
- [ ] Triggers on file attach (tool calling ON)
- [ ] Does NOT trigger (tool calling OFF)
- [ ] Works: single CSV, Parquet, XLSX, folder of mixed files
- [ ] Profile card: header, content, tool chips, action buttons all render
- [ ] Action buttons send follow-up messages
- [ ] Completes in < 15s for 100K row CSV

### Settings:
- [ ] Toggle persists across restarts
- [ ] Toggle OFF clears cache, disables auto-profile, restores previous skill
- [ ] Toggle ON re-enables everything

### Performance:
- [ ] Tool call IPC round-trip < 50ms
- [ ] Streaming text appears within 2s
- [ ] column_stats on 1M rows × 5 columns < 5s
