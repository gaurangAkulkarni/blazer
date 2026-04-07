import { useState, useEffect, useCallback } from 'react'
import { dbGetAppState, dbSetAppState } from '../lib/db'

export type ThemePreference = 'light' | 'dark' | 'system'

function getSystemDark(): boolean {
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

function applyDark(dark: boolean) {
  document.documentElement.classList.toggle('dark', dark)
}

export function useDarkMode() {
  const [preference, setPreference] = useState<ThemePreference>('system')
  const [isDark, setIsDark] = useState(getSystemDark)

  // Load saved preference from SQLite on mount
  useEffect(() => {
    dbGetAppState<ThemePreference>('blazer_theme', 'system')
      .then((pref) => setPreference(pref))
      .catch(console.error)
  }, [])

  // Apply theme whenever preference changes
  useEffect(() => {
    const mq = window.matchMedia('(prefers-color-scheme: dark)')
    const update = () => {
      const dark = preference === 'dark' || (preference === 'system' && mq.matches)
      setIsDark(dark)
      applyDark(dark)
    }
    update()
    if (preference === 'system') {
      mq.addEventListener('change', update)
      return () => mq.removeEventListener('change', update)
    }
  }, [preference])

  const setTheme = useCallback((pref: ThemePreference) => {
    setPreference(pref)
    dbSetAppState('blazer_theme', pref).catch(console.error)
  }, [])

  const toggleTheme = useCallback(() => {
    setTheme(
      preference === 'light'  ? 'dark'   :
      preference === 'dark'   ? 'system' : 'light',
    )
  }, [preference, setTheme])

  return { isDark, preference, setTheme, toggleTheme }
}
