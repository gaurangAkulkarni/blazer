import { ipcMain } from 'electron'
import Store from 'electron-store'

interface AppSettings {
  activeProvider: 'openai' | 'claude'
  openai: { apiKey: string; model: string; temperature: number }
  claude: { apiKey: string; model: string; temperature: number }
  execution: { timeoutMs: number; preferredLanguage: 'javascript' | 'python' }
  activeSkills: string[]
  customSkills: { id: string; name: string; description: string; prompt: string }[]
}

const defaults: AppSettings = {
  activeProvider: 'openai',
  openai: { apiKey: '', model: 'gpt-4o', temperature: 0.3 },
  claude: { apiKey: '', model: 'claude-sonnet-4-20250514', temperature: 0.3 },
  execution: { timeoutMs: 30000, preferredLanguage: 'javascript' },
  activeSkills: ['blazer-engine'],
  customSkills: [],
}

const store = new Store<AppSettings>({
  name: 'blazer-playground-settings',
  defaults,
  encryptionKey: 'blazer-playground-v1',
})

export function registerSettingsHandlers() {
  ipcMain.handle('settings:get', () => {
    return store.store
  })

  ipcMain.handle('settings:set', (_event, partial: Partial<AppSettings>) => {
    for (const [key, value] of Object.entries(partial)) {
      if (typeof value === 'object' && value !== null) {
        const existing = store.get(key as keyof AppSettings)
        store.set(key as keyof AppSettings, { ...existing, ...value } as any)
      } else {
        store.set(key as keyof AppSettings, value as any)
      }
    }
  })
}
