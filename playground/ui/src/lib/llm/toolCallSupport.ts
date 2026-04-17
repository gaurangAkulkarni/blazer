const sessionCache = new Map<string, boolean>()

// OpenAI o-series reasoning models (o1, o3, o4, …) DO support tool calling via the
// API but their internal "thinking" phase can cause them to write SQL in code blocks
// and say they "cannot run it in a thought block."  They're still worth trying with
// tools — the prompt now explicitly tells them to use run_sql — but mark them here
// as a reminder that their behaviour can be erratic.
const REASONING_MODEL_PREFIXES = ['o1', 'o3', 'o4']

export function isReasoningModel(model: string): boolean {
  const m = model.toLowerCase()
  return REASONING_MODEL_PREFIXES.some((p) => m.startsWith(p) || m.includes(`-${p}-`) || m.includes(`/${p}-`))
}

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
