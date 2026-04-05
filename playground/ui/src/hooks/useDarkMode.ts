import { useState, useEffect, useCallback } from 'react'

export type ThemePreference = 'light' | 'dark' | 'system'

const STORAGE_KEY = 'blazer_theme'

function getSystemDark(): boolean {
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

function applyDark(dark: boolean) {
  document.documentElement.classList.toggle('dark', dark)
}

export function useDarkMode() {
  const [preference, setPreference] = useState<ThemePreference>(() => {
    return (localStorage.getItem(STORAGE_KEY) as ThemePreference) ?? 'system'
  })

  const [isDark, setIsDark] = useState(() => {
    const pref = (localStorage.getItem(STORAGE_KEY) as ThemePreference) ?? 'system'
    return pref === 'dark' || (pref === 'system' && getSystemDark())
  })

  // Keep the <html class="dark"> in sync and watch the system media query
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
    localStorage.setItem(STORAGE_KEY, pref)
    setPreference(pref)
  }, [])

  /** Cycle: light → dark → system */
  const toggleTheme = useCallback(() => {
    setTheme(
      preference === 'light'  ? 'dark'   :
      preference === 'dark'   ? 'system' : 'light',
    )
  }, [preference, setTheme])

  return { isDark, preference, setTheme, toggleTheme }
}
