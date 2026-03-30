import { contextBridge, ipcRenderer } from 'electron'

export interface ChatMessage {
  role: 'user' | 'assistant' | 'system'
  content: string
}

export interface LLMSettings {
  provider: 'openai' | 'claude'
  apiKey: string
  model: string
  temperature: number
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

export interface AppSettings {
  activeProvider: 'openai' | 'claude'
  openai: { apiKey: string; model: string; temperature: number }
  claude: { apiKey: string; model: string; temperature: number }
  execution: { timeoutMs: number; preferredLanguage: 'javascript' | 'python' }
}

contextBridge.exposeInMainWorld('blazerAPI', {
  // LLM
  sendMessage: (messages: ChatMessage[], settings: LLMSettings): Promise<string> =>
    ipcRenderer.invoke('llm:send', messages, settings),
  onStreamChunk: (callback: (chunk: string) => void) => {
    ipcRenderer.on('llm:chunk', (_event, chunk: string) => callback(chunk))
  },
  onStreamEnd: (callback: () => void) => {
    ipcRenderer.on('llm:end', () => callback())
  },
  removeStreamListeners: () => {
    ipcRenderer.removeAllListeners('llm:chunk')
    ipcRenderer.removeAllListeners('llm:end')
  },

  // Code execution
  executeCode: (
    code: string,
    language: string,
    loadedFiles?: { path: string; ext: string }[],
  ): Promise<ExecutionResult> =>
    ipcRenderer.invoke('executor:run', code, language, loadedFiles),

  // Settings
  getSettings: (): Promise<AppSettings> => ipcRenderer.invoke('settings:get'),
  setSettings: (settings: Partial<AppSettings>): Promise<void> =>
    ipcRenderer.invoke('settings:set', settings),

  // File picker
  openFileDialog: (): Promise<{ path: string; columns?: string[] }[]> =>
    ipcRenderer.invoke('dialog:openFiles'),

  // Folder picker (partitioned Parquet)
  openFolderDialog: (): Promise<{ path: string; name: string; ext: string } | null> =>
    ipcRenderer.invoke('dialog:openFolder'),

  // Convert CSV to Parquet, returns output path
  convertToParquet: (csvPath: string): Promise<string> =>
    ipcRenderer.invoke('file:convertToParquet', csvPath),

  // DevTools toggle
  toggleDevTools: (): Promise<void> => ipcRenderer.invoke('devtools:toggle'),
})
