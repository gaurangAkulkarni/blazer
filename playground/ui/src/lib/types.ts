export type LeftTab = 'chat' | 'console' | 'history' | 'snippets' | 'schema'

export interface AttachedFile {
  path: string
  name: string
  ext: string
  /** Column names — populated after describe_tables or stats */
  columns?: string[]
  /** Column name → DuckDB type, populated after describe_tables */
  columnTypes?: Record<string, string>
  /** Total row count, populated after column_stats or a COUNT query */
  rowCount?: number
  /**
   * Overrides readExpr() when the actual content type was discovered at runtime.
   * Set when a folder is profiled and the real files inside (e.g. NDJSON) differ
   * from what the folder extension implies (e.g. would default to parquet).
   * Example: "read_json_auto('/path/trip_complete_data.ndjson')"
   */
  readerExpr?: string
  /**
   * Short DuckDB view name created automatically when the file is attached.
   * The LLM is instructed to query `FROM alias` instead of typing the full path,
   * eliminating path hallucination errors (e.g. gaurangkulatorani vs gaurangkulkarani).
   * Example: "tracker" → SELECT * FROM tracker
   */
  alias?: string
}

export interface QueryResult {
  success: boolean
  error?: string
  data: Record<string, unknown>[]
  columns: string[]
  shape: [number, number]
  duration_ms: number
  /** Short human-readable ID linking this result to the chat QueryBlock, e.g. "Q3" */
  queryId?: string
  /** How many times this specific QueryBlock has been run (1-based) */
  runNumber?: number
  /** Optional human-readable title extracted from the first SQL comment (-- ...) */
  title?: string
}

/** Persisted record of an executed query — stored in localStorage. */
export interface QueryHistoryEntry {
  id: string
  engine: 'blazer' | 'duckdb'
  query: string
  timestamp: number  // ms epoch
  success: boolean
  duration_ms: number
  rows: number
  cols: number
  error?: string
}

export interface ToolCallRecord {
  id: string
  name: string
  arguments: Record<string, unknown>
  result?: unknown
  duration_ms?: number
  status: 'running' | 'success' | 'error'
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  timestamp: number
  attachedFiles?: AttachedFile[]
  queryResults?: QueryResult[]
  /** Assistant messages only — wall-clock ms from first request to stream end */
  duration_ms?: number
  /** Prompt (input) token count from provider */
  tokens_in?: number
  /** Completion (output) token count from provider */
  tokens_out?: number
  /** AI-generated follow-up suggestion chips (assistant only) */
  suggestions?: string[]
  /** Full LLM context sent with this user message — for debugging */
  sentContext?: { role: string; content: string }[]
  /** Internal agentic loop continuation message — hidden from chat UI */
  agenticContinuation?: boolean
  /** UUID grouping messages from one agentic run */
  agenticRunId?: string
  /** Plan steps — stored only on the first assistant message of each run */
  agenticPlanSteps?: string[]
  /** Tool calls made during this assistant message */
  toolCalls?: ToolCallRecord[]
  /** Whether this message is an auto-profile response */
  isAutoProfile?: boolean
}

export interface CustomSkill {
  id: string
  name: string
  description: string
  prompt: string
}

/** How to authenticate against Azure Storage for Delta Lake / Iceberg on ADLS Gen2 */
export type AzureAuthMethod = 'none' | 'service_principal' | 'account_key' | 'sas' | 'azure_cli'

export interface ConnectionAlias {
  id: string
  name: string           // user-given label, e.g. "prod-postgres"
  ext_type: string       // DuckDB extension: "postgres", "mysql", "sqlite", "delta", "iceberg", etc.
  connection_string: string  // table path for delta/iceberg; connection URI for DB extensions
  description?: string
  // ── Azure credentials (delta / iceberg on ADLS Gen2) ──────────────────────
  azure_auth?: AzureAuthMethod
  /** Service principal fields */
  azure_tenant_id?: string
  azure_client_id?: string
  azure_client_secret?: string
  /** Account key — used to build azure_storage_connection_string */
  azure_account_key?: string
  /** Full Azure storage connection string or SAS URL (account_key or sas auth) */
  azure_storage_connection_string?: string
}

export interface ProviderSettings {
  api_key: string
  model: string
  temperature: number
  /** Optional custom base URL — useful for OpenAI-compatible proxies / Azure / local endpoints */
  base_url?: string
}

export interface OllamaSettings {
  base_url: string
  model: string
  temperature: number
}

export interface AppSettings {
  active_provider: 'openai' | 'claude' | 'ollama'
  openai: ProviderSettings
  claude: ProviderSettings
  ollama: OllamaSettings
  active_skills: string[]
  custom_skills: CustomSkill[]
  /** Whether to show AI follow-up suggestion chips below AI responses */
  show_follow_up_chips?: boolean
  /** Max number of previous messages to include in context (0 = all) */
  context_history_limit?: number
  /** Max output tokens per LLM response (undefined = provider default: 4096 for Claude, no cap for OpenAI/Ollama) */
  max_output_tokens?: number
  /** Named database/extension connections available for use in queries */
  connections?: ConnectionAlias[]
  /** Enable LLM tool calling (run_sql, describe_tables, etc.). Default: true. */
  tool_calling_enabled?: boolean
}

export interface SnippetGroup {
  id: string
  name: string
  createdAt: number
}

export interface QuerySnippet {
  id: string
  name: string
  description?: string
  query: string
  engine: 'blazer' | 'duckdb'
  createdAt: number
  /** undefined = belongs to Default group (ungrouped) */
  groupId?: string
}

export const DEFAULT_SETTINGS: AppSettings = {
  active_provider: 'openai',
  openai: { api_key: '', model: 'gpt-4o', temperature: 0.3, base_url: '' },
  claude: { api_key: '', model: 'claude-sonnet-4-20250514', temperature: 0.3 },
  ollama: { base_url: 'http://localhost:11434', model: 'llama3.2', temperature: 0.3 },
  active_skills: ['blazer-engine'],
  custom_skills: [],
  show_follow_up_chips: true,
  context_history_limit: 20, // 0 = all
  connections: [],
}
