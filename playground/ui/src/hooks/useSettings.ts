import { useState, useEffect, useCallback } from 'react'
import type { AppSettings } from '../lib/types'
import { DEFAULT_SETTINGS } from '../lib/types'
import { dbGetAppState, dbSetAppState } from '../lib/db'

export function useSettings() {
  const [settings, setSettingsState] = useState<AppSettings>(DEFAULT_SETTINGS)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    dbGetAppState<AppSettings>('blazer_settings', DEFAULT_SETTINGS)
      .then((s) => {
        setSettingsState({ ...DEFAULT_SETTINGS, ...s })
        setLoaded(true)
      })
      .catch(() => setLoaded(true))
  }, [])

  const updateSettings = useCallback(
    async (partial: Partial<AppSettings>) => {
      const updated = { ...settings, ...partial }
      setSettingsState(updated)
      await dbSetAppState('blazer_settings', updated).catch(console.error)
    },
    [settings],
  )

  return { settings, updateSettings, loaded }
}
