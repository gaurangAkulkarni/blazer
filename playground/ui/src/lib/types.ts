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
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  timestamp: number
  attachedFiles?: AttachedFile[]
  queryResults?: QueryResult[]
}

export interface CustomSkill {
  id: string
  name: string
  description: string
  prompt: string
}

export interface ProviderSettings {
  api_key: string
  model: string
  temperature: number
}

export interface AppSettings {
  active_provider: 'openai' | 'claude'
  openai: ProviderSettings
  claude: ProviderSettings
  active_skills: string[]
  custom_skills: CustomSkill[]
}

export const DEFAULT_SETTINGS: AppSettings = {
  active_provider: 'openai',
  openai: { api_key: '', model: 'gpt-4o', temperature: 0.3 },
  claude: { api_key: '', model: 'claude-sonnet-4-20250514', temperature: 0.3 },
  active_skills: ['blazer-engine'],
  custom_skills: [],
}
