export type LeftTab = 'chat' | 'console' | 'history' | 'snippets' | 'schema'

export interface AttachedFile {
  path: string
  name: string
  ext: string
  columns?: string[]
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
  /** Named database/extension connections available for use in queries */
  connections?: ConnectionAlias[]
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
