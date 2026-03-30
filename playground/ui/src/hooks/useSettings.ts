import { useState, useEffect, useCallback } from 'react'
import { invoke } from '@tauri-apps/api/core'
import type { AppSettings } from '../lib/types'
import { DEFAULT_SETTINGS } from '../lib/types'

export function useSettings() {
  const [settings, setSettingsState] = useState<AppSettings>(DEFAULT_SETTINGS)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    invoke<AppSettings>('load_settings')
      .then((s) => {
        setSettingsState({ ...DEFAULT_SETTINGS, ...s })
        setLoaded(true)
      })
      .catch(() => setLoaded(true))
  }, [])

  const updateSettings = useCallback(async (partial: Partial<AppSettings>) => {
    const updated = { ...settings, ...partial }
    setSettingsState(updated)
    await invoke('save_settings', { settings: updated }).catch(console.error)
  }, [settings])

  return { settings, updateSettings, loaded }
}
