import { useState, useEffect, useCallback } from 'react'
import type { AppSettings } from '../lib/types'

const defaultSettings: AppSettings = {
  activeProvider: 'openai',
  openai: { apiKey: '', model: 'gpt-4o', temperature: 0.3 },
  claude: { apiKey: '', model: 'claude-sonnet-4-20250514', temperature: 0.3 },
  execution: { timeoutMs: 30000, preferredLanguage: 'javascript' },
  activeSkills: ['blazer-engine'],
  customSkills: [],
}

export function useSettings() {
  const [settings, setSettingsState] = useState<AppSettings>(defaultSettings)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    window.blazerAPI.getSettings().then((s) => {
      setSettingsState(s)
      setLoaded(true)
    })
  }, [])

  const updateSettings = useCallback(async (partial: Partial<AppSettings>) => {
    await window.blazerAPI.setSettings(partial)
    const updated = await window.blazerAPI.getSettings()
    setSettingsState(updated)
  }, [])

  return { settings, updateSettings, loaded }
}
