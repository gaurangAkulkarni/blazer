const sessionCache = new Map<string, boolean>()

export function shouldSendTools(provider: string, model: string): boolean {
  // Claude uses its own tool format — handled separately in a future phase
  if (provider === 'claude') return false
  const key = `${provider}::${model}`
  if (sessionCache.has(key)) return sessionCache.get(key)!
  return true // optimistic first attempt
}

export function cacheToolCallSupport(provider: string, model: string, supported: boolean) {
  sessionCache.set(`${provider}::${model}`, supported)
}

export function clearToolCallCache() {
  sessionCache.clear()
}
