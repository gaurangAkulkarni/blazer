export interface AttachedFile {
  path: string
  name: string
  ext: string
  columns?: string[]   // header row read at attach time (CSV/TSV only)
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  timestamp: number
  attachedFiles?: AttachedFile[]
  executionResults?: ExecutionResult[]
}

export interface ExecutionResult {
  success: boolean
  stdout: string
  stderr: string
  durationMs: number
  dataframes: Array<{
    data: Record<string, unknown>[]
    columns: string[]
    shape: [number, number]
  }>
}

// onAddResult returns the new result's ID so callers can scroll to it
export type AddResultFn = (label: string, code: string, result: ExecutionResult) => string

export interface ResultEntry {
  id: string
  label: string
  code: string
  result: ExecutionResult
  timestamp: number
}

export interface CustomSkill {
  id: string
  name: string
  description: string
  prompt: string
}

export interface AppSettings {
  activeProvider: 'openai' | 'claude'
  openai: { apiKey: string; model: string; temperature: number }
  claude: { apiKey: string; model: string; temperature: number }
  execution: { timeoutMs: number; preferredLanguage: 'javascript' | 'python' }
  activeSkills: string[]
  customSkills: CustomSkill[]
}

export interface LLMSettings {
  provider: 'openai' | 'claude'
  apiKey: string
  model: string
  temperature: number
}

// Extend Window for the preload bridge
declare global {
  interface Window {
    blazerAPI: {
      sendMessage: (messages: { role: string; content: string }[], settings: LLMSettings) => Promise<string>
      onStreamChunk: (callback: (chunk: string) => void) => void
      onStreamEnd: (callback: () => void) => void
      removeStreamListeners: () => void
      executeCode: (code: string, language: string, loadedFiles?: { path: string; ext: string }[]) => Promise<ExecutionResult>
      getSettings: () => Promise<AppSettings>
      setSettings: (settings: Partial<AppSettings>) => Promise<void>
      openFileDialog: () => Promise<{ path: string; columns?: string[] }[]>
      openFolderDialog: () => Promise<{ path: string; name: string; ext: string } | null>
      convertToParquet: (csvPath: string) => Promise<string>
      toggleDevTools: () => Promise<void>
    }
  }
}
