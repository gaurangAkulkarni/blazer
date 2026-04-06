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
}

export interface CustomSkill {
  id: string
  name: string
  description: string
  prompt: string
}

export interface ConnectionAlias {
  id: string
  name: string           // user-given label, e.g. "prod-postgres"
  ext_type: string       // DuckDB extension: "postgres", "mysql", "sqlite", "httpfs", "spatial", etc.
  connection_string: string  // e.g. "postgresql://user:pass@host/db" — empty for non-DB extensions
  description?: string
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
