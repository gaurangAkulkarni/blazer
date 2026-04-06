import React, { useState, useCallback, useRef, useEffect } from 'react'
import { getCurrentWindow } from '@tauri-apps/api/window'
import { MessageList } from './components/Chat/MessageList'
import { InputBar } from './components/Chat/InputBar'
import { AgenticTimeline } from './components/Chat/AgenticTimeline'
import { SettingsPanel } from './components/Settings/SettingsPanel'
import { ResultPane } from './components/ResultPane/ResultPane'
import { ConsoleEditor } from './components/Console/ConsoleEditor'
import { QueryHistory } from './components/Console/QueryHistory'
import { SnippetsLibrary } from './components/Console/SnippetsLibrary'
import { CommandPalette } from './components/CommandPalette'
import { useSettings } from './hooks/useSettings'
import { useChat } from './hooks/useChat'
import { useQueryHistory } from './hooks/useQueryHistory'
import { useSnippets } from './hooks/useSnippets'
import { useSchema } from './hooks/useSchema'
import { useProfiler } from './hooks/useProfiler'
import { useDarkMode } from './hooks/useDarkMode'
import { useKeyboardShortcuts } from './hooks/useKeyboardShortcuts'
import { SchemaExplorer } from './components/SchemaExplorer/SchemaExplorer'
import type { Engine } from './hooks/useChat'
import type { QueryResult, LeftTab, AttachedFile } from './lib/types'
import type { ReplayRequest } from './components/Console/ConsoleEditor'
import { BUILT_IN_SKILLS } from './lib/skills'
import type { Skill } from './lib/skills'
import { ConnectionsContext } from './lib/ConnectionsContext'
import type { ConnectionAlias } from './lib/types'

// ── Agentic result context builder ───────────────────────────────────────────
// Builds a rich markdown representation of query results to send back to the
// LLM so it can reason from actual data — not just a row count.
function buildAgenticResultContext(
  pending: { result: QueryResult; query: string; engine: 'blazer' | 'duckdb' }[],
): string {
  const parts: string[] = []

  for (const { result } of pending) {
    const { data = [], columns = [], shape } = result
    const [totalRows, totalCols] = shape ?? [data.length, columns.length]

    if (totalRows === 0) {
      parts.push('Query returned **0 rows**.')
      continue
    }

    // Cap columns to avoid bloating context (show first 12; LLM can ask for more)
    const MAX_COLS = 12
    const displayCols = columns.slice(0, MAX_COLS)
    const hiddenCols = columns.length - displayCols.length

    // For small result sets send everything; for large ones send a meaningful head
    const MAX_ROWS = totalRows <= 40 ? totalRows : 25
    const displayData = data.slice(0, MAX_ROWS)

    // Truncate long cell values so one wide column doesn't dominate
    const fmt = (v: unknown): string => {
      const s = String(v ?? '')
      return s.length > 60 ? s.slice(0, 57) + '…' : s
    }

    const header  = '| ' + displayCols.join(' | ') + ' |'
    const divider = '| ' + displayCols.map(() => '---').join(' | ') + ' |'
    const rows    = displayData.map(row =>
      '| ' + displayCols.map(c => fmt(row[c])).join(' | ') + ' |',
    )

    const meta: string[] = [`**${totalRows.toLocaleString()} rows × ${totalCols} columns**`]
    if (MAX_ROWS < totalRows) meta.push(`first ${MAX_ROWS} rows shown`)
    if (hiddenCols > 0)       meta.push(`${hiddenCols} columns omitted`)

    parts.push([meta.join(' — '), '', header, divider, ...rows].join('\n'))
  }

  return parts.join('\n\n')
}

// ── localStorage helpers ──────────────────────────────────────────────────────
function persist<T>(key: string, value: T) {
  try { localStorage.setItem(key, JSON.stringify(value)) } catch { /* quota */ }
}
function restore<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key)
    return raw ? (JSON.parse(raw) as T) : fallback
  } catch { return fallback }
}

export default function App() {
  const { isDark, preference, toggleTheme } = useDarkMode()
  const { settings, updateSettings, loaded } = useSettings()

  // Persist engine selections across restarts
  const [chatEngine, setChatEngineState] = useState<Engine>(
    () => restore<Engine>('blazer_chat_engine', 'blazer'),
  )
  const [consoleEngine, setConsoleEngineState] = useState<Engine>(
    () => restore<Engine>('blazer_console_engine', 'blazer'),
  )
  const setChatEngine = useCallback((e: Engine) => {
    setChatEngineState(e); persist('blazer_chat_engine', e)
  }, [])
  const setConsoleEngine = useCallback((e: Engine) => {
    setConsoleEngineState(e); persist('blazer_console_engine', e)
  }, [])

  const { messages, sendMessage, isStreaming, addQueryResult, clearMessages, loadedFiles, replaceFile, removeFile } = useChat(settings, chatEngine)

  const [activeConnections, setActiveConnections] = useState<ConnectionAlias[]>([])

  const addConnection = useCallback((conn: ConnectionAlias) => {
    setActiveConnections((prev) => prev.some((c) => c.id === conn.id) ? prev : [...prev, conn])
  }, [])

  const removeConnection = useCallback((id: string) => {
    setActiveConnections((prev) => prev.filter((c) => c.id !== id))
  }, [])

  const [settingsOpen, setSettingsOpen] = useState(false)
  const [paletteOpen, setPaletteOpen] = useState(false)

  // Persist result history (max 30, strip data rows to save space)
  const [resultHistory, setResultHistoryState] = useState<QueryResult[]>(
    () => restore<QueryResult[]>('blazer_result_history', []),
  )
  const setResultHistory = useCallback((updater: QueryResult[] | ((prev: QueryResult[]) => QueryResult[])) => {
    setResultHistoryState((prev) => {
      const next = typeof updater === 'function' ? updater(prev) : updater
      const capped = next.slice(0, 30)
      persist('blazer_result_history', capped)
      return capped
    })
  }, [])

  const [resultPaneOpen, setResultPaneOpen] = useState(true)
  const [leftTab, setLeftTab] = useState<LeftTab>('chat')
  const [aiPrefill, setAiPrefill] = useState('')
  const [autoRun, setAutoRunState] = useState<boolean>(
    () => restore<boolean>('blazer_autorun', false),
  )
  const toggleAutoRun = useCallback(() => {
    setAutoRunState((v) => { persist('blazer_autorun', !v); return !v })
  }, [])

  // ── Agentic mode ─────────────────────────────────────────────────────────────
  const [agenticMode, setAgenticModeState] = useState<boolean>(
    () => restore<boolean>('blazer_agentic_mode', false),
  )
  const toggleAgenticMode = useCallback(() => {
    setAgenticModeState((v) => { persist('blazer_agentic_mode', !v); return !v })
  }, [])

  // Reactive state for UI
  const [agenticActive, setAgenticActive] = useState(false)
  const [agenticCurrentStep, setAgenticCurrentStep] = useState(0)
  const [agenticPlanSteps, setAgenticPlanSteps] = useState<string[]>([])
  const [agenticStepError, setAgenticStepError] = useState(false)
  const [agenticIteration, setAgenticIteration] = useState(0)

  // Refs — stable values accessible inside callbacks without stale closures
  const agenticActiveRef = useRef(false)
  const agenticIterationRef = useRef(0)
  const agenticCurrentStepRef = useRef(0)
  const agenticPlanStepsRef = useRef<string[]>([])
  const agenticDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const pendingAgenticResultsRef = useRef<{ result: QueryResult; query: string; engine: 'blazer' | 'duckdb' }[]>([])
  // Set to true when at least one query ran in the current streaming turn.
  // The 1200ms text-only fallback checks this so it doesn't double-fire after
  // the 800ms query debounce already sent a continuation.
  const agenticQueryRanRef = useRef(false)
  // Stable ref to the latest sendMessage so debounce callbacks don't capture stale closures
  const sendMessageRef = useRef(sendMessage)
  sendMessageRef.current = sendMessage

  const MAX_AGENTIC_ITER = 10

  const stopAgenticLoop = useCallback(() => {
    agenticActiveRef.current = false
    setAgenticActive(false)
    if (agenticDebounceRef.current) {
      clearTimeout(agenticDebounceRef.current)
      agenticDebounceRef.current = null
    }
    pendingAgenticResultsRef.current = []
  }, [])

  function parsePlanSteps(content: string): string[] {
    const match = content.match(/```plan\n([\s\S]*?)```/)
    if (!match) return []
    const steps = match[1]
      .split('\n')
      .map((l) => l.replace(/^\s*\d+\.\s*/, '').replace(/^\s*[-*]\s*/, '').trim())
      .filter(Boolean)

    // Always guarantee a final "assessment" step so the plan visually maps to DONE.
    // If the LLM already included one, keep it; otherwise inject a standard label.
    const FINAL_KEYWORDS = /\b(assess|assessment|summary|summarize|synthesize|synthesis|conclusion|findings|report|final|wrap.?up|present)\b/i
    const hasFinishStep = steps.length > 0 && FINAL_KEYWORDS.test(steps[steps.length - 1])
    if (!hasFinishStep) {
      steps.push('Synthesize findings & provide final assessment')
    }
    return steps
  }

  const handleSendToAI = useCallback((text: string) => {
    setLeftTab('chat')
    setAiPrefill(text)
  }, [])

  const handleScrollToQuery = useCallback((queryId: string) => {
    setLeftTab('chat')
    // Give the tab a moment to become visible before scrolling
    setTimeout(() => {
      const el = document.getElementById(`qblock-${queryId}`)
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' })
        // Brief highlight flash
        el.style.transition = 'outline 0s, box-shadow 0.15s'
        el.style.boxShadow = '0 0 0 2px #6366f1'
        setTimeout(() => { el.style.boxShadow = '' }, 1000)
      }
    }, 80)
  }, [])
  const [replayRequest, setReplayRequest] = useState<ReplayRequest | undefined>(undefined)

  const { history: queryHistory, addEntry: addHistoryEntry, removeEntry: removeHistoryEntry, clearHistory } = useQueryHistory()
  const { snippets, addSnippet, updateSnippet, removeSnippet, clearSnippets, groups: snippetGroups, addGroup, renameGroup, removeGroup } = useSnippets()
  const { schemas, fetchSchema, fetchAll } = useSchema()
  const { profiles, profileFile } = useProfiler()

  const handleSaveSnippet = useCallback(
    (query: string, engine: 'blazer' | 'duckdb', name: string, groupId?: string) => {
      addSnippet({ name, query, engine, groupId })
    },
    [addSnippet],
  )

  const handleSendSnippetToChat = useCallback(
    (text: string) => {
      setLeftTab('chat')
      setAiPrefill(text)
    },
    [],
  )

  // Fix: chat queries now pass query text + engine, so we can record them in history
  const handleQueryResult = useCallback(
    (messageId: string, result: QueryResult, query: string, engine: 'blazer' | 'duckdb') => {
      addQueryResult(messageId, result)
      setResultHistory((prev) => [result, ...prev])
      setResultPaneOpen(true)
      addHistoryEntry({
        engine,
        query,
        timestamp: Date.now(),
        success: result.success,
        duration_ms: result.duration_ms,
        rows: result.shape[0],
        cols: result.shape[1],
        error: result.error,
      })

      // ── Agentic loop continuation ──────────────────────────────────────────
      if (!agenticActiveRef.current) return

      // Mark that a query ran during this streaming turn — prevents the 1200ms
      // text-only fallback from double-firing a second continuation.
      agenticQueryRanRef.current = true

      // Collect results from this LLM turn (debounced for multi-block responses)
      pendingAgenticResultsRef.current.push({ result, query, engine })

      if (agenticDebounceRef.current) clearTimeout(agenticDebounceRef.current)
      agenticDebounceRef.current = setTimeout(() => {
        agenticDebounceRef.current = null
        if (!agenticActiveRef.current) return

        const pending = pendingAgenticResultsRef.current
        pendingAgenticResultsRef.current = []

        const failedItem = pending.find((p) => !p.result.success)

        agenticIterationRef.current += 1
        setAgenticIteration(agenticIterationRef.current)
        if (agenticIterationRef.current >= MAX_AGENTIC_ITER) {
          stopAgenticLoop()
          return
        }

        if (failedItem) {
          setAgenticStepError(true)
          const lang = failedItem.engine === 'duckdb' ? 'sql' : 'json'
          sendMessageRef.current(
            `The query returned an error:\n\n\`\`\`${lang}\n${failedItem.query}\n\`\`\`\n\nError:\n\`\`\`\n${failedItem.result.error ?? 'Unknown error'}\n\`\`\`\n\nDiagnose the error, fix the SQL, and output the corrected query.`,
            undefined, undefined, { agenticContinuation: true },
          )
        } else {
          setAgenticStepError(false)
          // Cap at steps.length - 1 so the final "assessment" step stays as
          // "running" (amber) while the LLM writes the assessment.
          // Only explicit DONE detection advances currentStep to steps.length (all done).
          const totalSteps = agenticPlanStepsRef.current.length
          const nextStep = Math.min(agenticCurrentStepRef.current + 1, Math.max(totalSteps - 1, 0))
          agenticCurrentStepRef.current = nextStep
          setAgenticCurrentStep(nextStep)

          // Build a rich result context so the LLM can reason from actual data
          const resultContext = buildAgenticResultContext(pending)

          sendMessageRef.current(
            `${resultContext}\n\nContinue toward the goal. When ALL steps are fully complete and the objective is achieved, respond with only: DONE`,
            undefined, undefined, { agenticContinuation: true },
          )
        }
      }, 800)
    },
    [addQueryResult, addHistoryEntry, setResultHistory, stopAgenticLoop],
  )

  // ── Agentic DONE detection + plan parsing + text-only step advancement ───────
  // Runs after every render to detect the isStreaming true→false transition.
  const prevStreamingRef = useRef(isStreaming)
  useEffect(() => {
    const wasStreaming = prevStreamingRef.current
    prevStreamingRef.current = isStreaming
    // Only act on true → false transition
    if (!wasStreaming || isStreaming) return

    // Capture per-turn query flag BEFORE resetting so text-only path can check it
    const queryRanThisTurn = agenticQueryRanRef.current
    agenticQueryRanRef.current = false

    const lastMsg = messages[messages.length - 1]
    if (!lastMsg || lastMsg.role !== 'assistant') return

    // Parse plan from first agentic response (only once)
    if (agenticActiveRef.current && agenticPlanStepsRef.current.length === 0) {
      const steps = parsePlanSteps(lastMsg.content)
      if (steps.length > 0) {
        agenticPlanStepsRef.current = steps
        setAgenticPlanSteps(steps)
      }
    }

    // ── DONE detection ────────────────────────────────────────────────────────
    // Match DONE in any of the forms the LLM commonly produces:
    //   - "DONE" on its own line (strict)          ← original
    //   - "**DONE**" or "*DONE*" (markdown bold)
    //   - "Done", "done" (case variants)
    //   - "DONE" at the very end of the message
    const DONE_RE = /(?:^\s*\*{0,2}DONE\*{0,2}\s*$|\*{0,2}DONE\*{0,2}\s*$)/im
    if (agenticActiveRef.current && DONE_RE.test(lastMsg.content)) {
      const beforeDone = lastMsg.content.replace(/\n*\s*\*{0,2}DONE\*{0,2}\s*$/i, '').trim()
      const hasAssessment = beforeDone.length > 150

      if (!hasAssessment) {
        // LLM said DONE without writing the assessment — ask for it
        agenticIterationRef.current += 1
        setAgenticIteration(agenticIterationRef.current)
        if (agenticIterationRef.current >= MAX_AGENTIC_ITER) { stopAgenticLoop(); return }
        sendMessageRef.current(
          `You responded with only "DONE" but haven't provided the final assessment yet. Please write your complete analysis and key findings from all the data you gathered, then end with DONE.`,
          undefined, undefined, { agenticContinuation: true },
        )
        return
      }

      setAgenticCurrentStep(agenticPlanStepsRef.current.length)
      agenticCurrentStepRef.current = agenticPlanStepsRef.current.length
      stopAgenticLoop()
      return
    }

    // ── Auto-stop: final step OR comprehensive final-report detected ─────────
    // Stops the loop if:
    //   A) We are at/past the final plan step and the response has substantial
    //      prose (no SQL) — the LLM just forgot to write DONE.
    //   B) The LLM jumped ahead and delivered a complete final-assessment
    //      response at any step. Detected by: long prose + section headings +
    //      final-report language ("Final", "Assessment", "Report", "Summary",
    //      "Recommendations") — regardless of current step index.
    if (agenticActiveRef.current) {
      const hasSql = /```sql/i.test(lastMsg.content)
      const content = lastMsg.content.trim()
      const atFinalStep = agenticCurrentStepRef.current >= agenticPlanStepsRef.current.length - 1

      // Heuristic: does this look like a full final-assessment report?
      const isFinalReport =
        !hasSql &&
        content.length > 600 &&
        // has numbered or ## section headings
        (/^#{1,3}\s/m.test(content) || /^\d+\.\s+\*{0,2}[A-Z]/m.test(content)) &&
        // contains final-report language
        /\b(final|comprehensive|summary|assessment|report|recommendation|conclusion|findings)\b/i.test(content)

      if (!hasSql && (atFinalStep || isFinalReport)) {
        setAgenticCurrentStep(agenticPlanStepsRef.current.length)
        agenticCurrentStepRef.current = agenticPlanStepsRef.current.length
        stopAgenticLoop()
        return
      }
    }

    // ── Text-only step advancement ────────────────────────────────────────────
    // If the LLM's response has no SQL block AND no query already ran this turn,
    // no QueryBlock will fire — send the continuation directly.
    if (agenticActiveRef.current) {
      const hasSql = /```sql/i.test(lastMsg.content)
      if (!hasSql && !queryRanThisTurn) {
        agenticIterationRef.current += 1
        setAgenticIteration(agenticIterationRef.current)
        if (agenticIterationRef.current >= MAX_AGENTIC_ITER) { stopAgenticLoop(); return }

        const totalSteps = agenticPlanStepsRef.current.length
        const nextStep = Math.min(agenticCurrentStepRef.current + 1, Math.max(totalSteps - 1, 0))
        agenticCurrentStepRef.current = nextStep
        setAgenticCurrentStep(nextStep)
        setAgenticStepError(false)

        sendMessageRef.current(
          `Continue toward the goal. When ALL steps are fully complete and the objective is achieved, respond with only: DONE`,
          undefined, undefined, { agenticContinuation: true },
        )
      }
      // If SQL is present OR a query already ran — handleQueryResult drives the next turn.
    }
  })

  const handleConsoleResult = useCallback(
    (result: QueryResult, query: string, engine: Engine) => {
      setResultHistory((prev) => [result, ...prev])
      setResultPaneOpen(true)
      addHistoryEntry({
        engine,
        query,
        timestamp: Date.now(),
        success: result.success,
        duration_ms: result.duration_ms,
        rows: result.shape[0],
        cols: result.shape[1],
        error: result.error,
      })
    },
    [addHistoryEntry],
  )

  const handleReplay = useCallback(
    (engine: Engine, query: string) => {
      setReplayRequest((prev) => ({ engine, query, seq: (prev?.seq ?? 0) + 1 }))
      setLeftTab('console')
      setConsoleEngine(engine)
    },
    [],
  )

  // ── Agentic send wrapper ──────────────────────────────────────────────────────
  // Wraps sendMessage so that when agenticMode is on, we initialise the loop state
  // before sending and inject the agentic instruction into the system prompt.
  const handleSend = useCallback(
    (content: string, attachments?: AttachedFile[], perMessageSkillIds?: string[]) => {
      if (agenticMode) {
        agenticActiveRef.current = true
        agenticIterationRef.current = 0
        agenticCurrentStepRef.current = 0
        agenticPlanStepsRef.current = []
        setAgenticActive(true)
        setAgenticCurrentStep(0)
        setAgenticPlanSteps([])
        setAgenticStepError(false)
        setAgenticIteration(0)
        sendMessage(content, attachments, perMessageSkillIds, { agenticMode: true, activeConnections })
      } else {
        sendMessage(content, attachments, perMessageSkillIds, { activeConnections })
      }
    },
    [agenticMode, sendMessage, activeConnections],
  )

  // ── Resizable split pane ────────────────────────────────────────────────────
  const [splitPct, setSplitPct] = useState<number>(
    () => restore<number>('blazer_split_pct', 52),
  )
  const splitContainerRef = useRef<HTMLDivElement>(null)
  const isDraggingRef = useRef(false)

  const handleDividerMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    isDraggingRef.current = true
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'

    const onMove = (ev: MouseEvent) => {
      if (!isDraggingRef.current || !splitContainerRef.current) return
      const { left, width } = splitContainerRef.current.getBoundingClientRect()
      const pct = Math.min(75, Math.max(25, ((ev.clientX - left) / width) * 100))
      setSplitPct(pct)
    }

    const onUp = () => {
      isDraggingRef.current = false
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
      // Persist after drag ends
      setSplitPct((prev) => { persist('blazer_split_pct', prev); return prev })
    }

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }, [])

  // ── Global keyboard shortcuts ───────────────────────────────────────────────
  useKeyboardShortcuts({
    onGoToTab: setLeftTab,
    onToggleSettings: () => setSettingsOpen(v => !v),
    onToggleResultPane: () => setResultPaneOpen(v => !v),
    onToggleTheme: toggleTheme,
    onToggleAutoRun: toggleAutoRun,
    onClearMessages: clearMessages,
    onOpenPalette: () => setPaletteOpen(v => !v),
    isSettingsOpen: settingsOpen,
    isPaletteOpen: paletteOpen,
  })

  if (!loaded) {
    return (
      <div className="h-screen flex items-center justify-center bg-white dark:bg-gray-900">
        <div className="text-gray-400 text-sm">Loading…</div>
      </div>
    )
  }

  const allSkills: Skill[] = [
    ...BUILT_IN_SKILLS,
    ...(settings.custom_skills ?? []).map((s) => ({ ...s, builtIn: false as const })),
  ]

  const isOllama = settings.active_provider === 'ollama'
  const hasApiKey = isOllama || settings[settings.active_provider].api_key.length > 0
  const providerLabel = settings.active_provider === 'openai' ? 'OpenAI' : settings.active_provider === 'claude' ? 'Claude' : 'Ollama'
  const activeModel = isOllama ? settings.ollama?.model : settings[settings.active_provider]?.model

  return (
    <ConnectionsContext.Provider value={activeConnections}>
    <div className="h-screen flex flex-col bg-white dark:bg-gray-900 overflow-hidden">
      {/* Title bar — draggable */}
      <header
        data-tauri-drag-region
        onDoubleClick={() => getCurrentWindow().toggleMaximize()}
        className="flex items-center justify-between pl-20 pr-4 py-2.5 border-b border-gray-200 dark:border-gray-700 bg-white/80 dark:bg-gray-900/80 backdrop-blur-xl shrink-0 select-none"
      >
        <div className="flex items-center gap-3">
          <h1 className="text-sm font-semibold tracking-tight">
            <span className="text-gray-900 dark:text-gray-100">blazer</span>
            <span className="text-gray-400 dark:text-gray-500 font-normal ml-1">playground</span>
          </h1>
          <span className="text-xs px-2.5 py-0.5 rounded-full font-medium bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400 border border-gray-200 dark:border-gray-700">
            {providerLabel} · {activeModel}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          {!hasApiKey && (
            <span className="text-xs text-orange-500 mr-2">Set API key in settings</span>
          )}
          {/* Dark mode toggle */}
          <button
            onClick={toggleTheme}
            className="text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-800 transition"
            title={preference === 'light' ? 'Light mode — click for dark' : preference === 'dark' ? 'Dark mode — click for system' : 'System mode — click for light'}
          >
            {preference === 'dark' ? (
              /* Moon icon */
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
              </svg>
            ) : preference === 'system' ? (
              /* Monitor icon */
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/>
              </svg>
            ) : (
              /* Sun icon */
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
              </svg>
            )}
          </button>
          {/* Command palette */}
          <button
            onClick={() => setPaletteOpen(v => !v)}
            className="text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-800 transition"
            title={`Command palette (${/Mac/.test(navigator.userAgent) ? '⌘P' : 'Ctrl+P'})`}
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M18 3a3 3 0 0 0-3 3v12a3 3 0 0 0 3 3 3 3 0 0 0 3-3 3 3 0 0 0-3-3H6a3 3 0 0 0-3 3 3 3 0 0 0 3 3 3 3 0 0 0 3-3V6a3 3 0 0 0-3-3 3 3 0 0 0-3 3 3 3 0 0 0 3 3h12a3 3 0 0 0 3-3 3 3 0 0 0-3-3z"/>
            </svg>
          </button>
          {/* Toggle result pane */}
          <button
            onClick={() => setResultPaneOpen((v) => !v)}
            className={`text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-800 transition ${resultPaneOpen ? 'bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-100' : ''}`}
            title={`${resultPaneOpen ? 'Hide' : 'Show'} result pane (${/Mac/.test(navigator.userAgent) ? '⌘\\' : 'Ctrl+\\'})`}
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
              <line x1="12" y1="3" x2="12" y2="21"/>
            </svg>
          </button>
          <button
            onClick={() => setSettingsOpen(true)}
            className="text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-800 transition"
            title={`Settings (${/Mac/.test(navigator.userAgent) ? '⌘,' : 'Ctrl+,'})`}
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="3"/>
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/>
            </svg>
          </button>
        </div>
      </header>

      {/* Main content — split pane */}
      <div ref={splitContainerRef} className="flex-1 flex min-h-0">
        {/* Left: Chat / Console */}
        <div
          className="flex flex-col min-h-0 min-w-0 shrink-0"
          style={resultPaneOpen ? { width: `${splitPct}%` } : { flex: '1 1 0' }}
        >
          {/* Tab bar */}
          <div className="shrink-0 flex items-center gap-0.5 px-3 pt-2 pb-0 border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900" style={{ minHeight: 36 }}>
            <button
              onClick={() => setLeftTab('chat')}
              className={`flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded-t-md border-b-2 transition-colors
                ${leftTab === 'chat'
                  ? 'border-gray-900 dark:border-gray-100 text-gray-900 dark:text-gray-100 bg-white dark:bg-gray-900'
                  : 'border-transparent text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
              </svg>
              AI Chat
            </button>
            <button
              onClick={() => setLeftTab('console')}
              className={`flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded-t-md border-b-2 transition-colors
                ${leftTab === 'console'
                  ? 'border-gray-900 dark:border-gray-100 text-gray-900 dark:text-gray-100 bg-white dark:bg-gray-900'
                  : 'border-transparent text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="4 17 10 11 4 5"/>
                <line x1="12" y1="19" x2="20" y2="19"/>
              </svg>
              Console
            </button>
            <button
              onClick={() => setLeftTab('history')}
              className={`flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded-t-md border-b-2 transition-colors
                ${leftTab === 'history'
                  ? 'border-gray-900 dark:border-gray-100 text-gray-900 dark:text-gray-100 bg-white dark:bg-gray-900'
                  : 'border-transparent text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="10"/>
                <polyline points="12 6 12 12 16 14"/>
              </svg>
              History
              {queryHistory.length > 0 && (
                <span className="ml-0.5 text-[10px] bg-gray-200 dark:bg-gray-700 text-gray-600 dark:text-gray-400 rounded-full px-1.5 py-0 font-semibold">
                  {queryHistory.length}
                </span>
              )}
            </button>
            <button
              onClick={() => setLeftTab('snippets')}
              className={`flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded-t-md border-b-2 transition-colors
                ${leftTab === 'snippets'
                  ? 'border-gray-900 dark:border-gray-100 text-gray-900 dark:text-gray-100 bg-white dark:bg-gray-900'
                  : 'border-transparent text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>
              </svg>
              Snippets
              {snippets.length > 0 && (
                <span className="ml-0.5 text-[10px] bg-gray-200 dark:bg-gray-700 text-gray-600 dark:text-gray-400 rounded-full px-1.5 py-0 font-semibold">
                  {snippets.length}
                </span>
              )}
            </button>
            <button
              onClick={() => setLeftTab('schema')}
              className={`flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded-t-md border-b-2 transition-colors
                ${leftTab === 'schema'
                  ? 'border-gray-900 dark:border-gray-100 text-gray-900 dark:text-gray-100 bg-white dark:bg-gray-900'
                  : 'border-transparent text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <ellipse cx="12" cy="5" rx="9" ry="3"/>
                <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
                <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
              </svg>
              Schema
              {loadedFiles.length > 0 && (
                <span className="ml-0.5 text-[10px] bg-gray-200 dark:bg-gray-700 text-gray-600 dark:text-gray-400 rounded-full px-1.5 py-0 font-semibold">
                  {loadedFiles.length}
                </span>
              )}
            </button>

            {/* Toggles — right side: icon-only pill group */}
            <div className="ml-auto flex items-center gap-1.5 pb-1 pr-1 select-none">
              {/* Agentic loop indicator — only while a step is in progress */}
              {agenticActive && agenticPlanSteps.length > 0 && agenticCurrentStep < agenticPlanSteps.length && (
                <span className="flex items-center gap-1 text-[10px] text-indigo-500 font-medium tabular-nums mr-0.5">
                  <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="animate-spin">
                    <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
                  </svg>
                  {agenticCurrentStep + 1}/{agenticPlanSteps.length}
                </span>
              )}

              {/* Icon-only pill group */}
              <div className="flex items-center rounded-md border border-gray-200 dark:border-gray-700 overflow-hidden">
                {/* Agentic button */}
                <button
                  onClick={toggleAgenticMode}
                  title={agenticMode ? 'Agentic on — AI plans and executes steps automatically' : 'Agentic off — single-turn mode'}
                  className={`flex items-center justify-center w-7 h-6 transition-colors ${
                    agenticMode
                      ? 'bg-indigo-500 text-white'
                      : 'bg-white dark:bg-gray-900 text-gray-400 dark:text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-800'
                  }`}
                >
                  {/* Robot/agent icon */}
                  <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <rect x="3" y="11" width="18" height="10" rx="2"/>
                    <circle cx="12" cy="5" r="2"/>
                    <path d="M12 7v4"/>
                    <line x1="8" y1="16" x2="8" y2="16" strokeWidth="2.5" strokeLinecap="round"/>
                    <line x1="16" y1="16" x2="16" y2="16" strokeWidth="2.5" strokeLinecap="round"/>
                  </svg>
                </button>

                {/* Divider */}
                <div className="w-px h-4 bg-gray-200 dark:bg-gray-700" />

                {/* Autorun button */}
                <button
                  onClick={toggleAutoRun}
                  title={autoRun ? 'Autorun on — queries run automatically after AI responds' : 'Autorun off — click Run Query manually'}
                  className={`flex items-center justify-center w-7 h-6 transition-colors ${
                    autoRun
                      ? 'bg-blue-500 text-white'
                      : 'bg-white dark:bg-gray-900 text-gray-400 dark:text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-800'
                  }`}
                >
                  {/* Zap/lightning icon — auto-execute */}
                  <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                    <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
                  </svg>
                </button>
              </div>
            </div>
          </div>

          {/* Tab content — both always mounted, inactive hidden to preserve state */}
          <div className={`${leftTab === 'chat' ? 'flex flex-col flex-1 min-h-0' : 'hidden'}`}>
            {/* Chat engine toolbar */}
            <div className="shrink-0 flex items-center gap-3 px-3 py-2 border-b border-gray-100 dark:border-gray-800 bg-gray-50/60 dark:bg-gray-800/60">
              <div className="flex items-center bg-gray-200/70 dark:bg-gray-700/70 rounded-lg p-0.5 gap-0.5">
                {(['blazer', 'duckdb'] as Engine[]).map((e) => (
                  <button
                    key={e}
                    onClick={() => setChatEngine(e)}
                    className={`text-[11px] font-semibold px-2.5 py-1 rounded-md transition-all ${
                      chatEngine === e
                        ? 'bg-white dark:bg-gray-600 text-gray-900 dark:text-gray-100 shadow-sm'
                        : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'
                    }`}
                  >
                    {e === 'blazer' ? 'Blazer' : 'DuckDB'}
                  </button>
                ))}
              </div>
              <span className="text-xs text-gray-400 dark:text-gray-500">
                AI will generate{' '}
                <span className="font-medium text-gray-600 dark:text-gray-400">
                  {chatEngine === 'blazer' ? 'Blazer JSON queries' : 'DuckDB SQL'}
                </span>
              </span>
            </div>

            {/* Chat body: optional agentic timeline + messages/input */}
            <div className="flex flex-1 min-h-0">
              {agenticPlanSteps.length > 0 && (
                <AgenticTimeline
                  steps={agenticPlanSteps}
                  currentStep={agenticCurrentStep}
                  active={agenticActive}
                  stepError={agenticStepError}
                  iteration={agenticIteration}
                  maxIterations={MAX_AGENTIC_ITER}
                  onClear={() => {
                    stopAgenticLoop()
                    setAgenticPlanSteps([])
                    agenticPlanStepsRef.current = []
                    setAgenticCurrentStep(0)
                    agenticCurrentStepRef.current = 0
                  }}
                />
              )}
              <div className="flex flex-col flex-1 min-h-0 min-w-0">
                <MessageList
                  messages={messages}
                  isStreaming={isStreaming}
                  onQueryResult={handleQueryResult}
                  onSend={(text) => handleSend(text)}
                  onAppendToChat={handleSendToAI}
                  autoRun={autoRun}
                  onSaveSnippet={handleSaveSnippet}
                  snippetGroups={snippetGroups}
                  agenticMode={agenticMode}
                  agenticActive={agenticActive}
                  agenticCurrentStep={agenticCurrentStep}
                  agenticPlanSteps={agenticPlanSteps}
                  agenticStepError={agenticStepError}
                />
                <InputBar
                  onSend={handleSend}
                  onClear={clearMessages}
                  disabled={isStreaming}
                  loadedFiles={loadedFiles}
                  onRemoveFile={removeFile}
                  onReplaceFile={replaceFile}
                  prefill={aiPrefill}
                  onPrefillConsumed={() => setAiPrefill('')}
                  availableSkills={allSkills}
                  availableConnections={settings.connections ?? []}
                  activeConnections={activeConnections}
                  onAddConnection={addConnection}
                  onRemoveConnection={removeConnection}
                />
              </div>
            </div>
          </div>
          <div className={`${leftTab === 'console' ? 'flex flex-col flex-1 min-h-0' : 'hidden'}`}>
            <ConsoleEditor
              onResult={handleConsoleResult}
              engine={consoleEngine}
              onEngineChange={setConsoleEngine}
              replayRequest={replayRequest}
              loadedFiles={loadedFiles}
              schemas={schemas}
              onSaveSnippet={handleSaveSnippet}
              snippetGroups={snippetGroups}
            />
          </div>
          <div className={`${leftTab === 'history' ? 'flex flex-col flex-1 min-h-0' : 'hidden'}`}>
            <QueryHistory
              history={queryHistory}
              onRemove={removeHistoryEntry}
              onClear={clearHistory}
              onReplay={handleReplay}
            />
          </div>
          <div className={`${leftTab === 'snippets' ? 'flex flex-col flex-1 min-h-0' : 'hidden'}`}>
            <SnippetsLibrary
              snippets={snippets}
              groups={snippetGroups}
              onRemove={removeSnippet}
              onUpdate={updateSnippet}
              onClear={clearSnippets}
              onAddGroup={addGroup}
              onRenameGroup={renameGroup}
              onRemoveGroup={removeGroup}
              onLoadToConsole={handleReplay}
              onSendToChat={handleSendSnippetToChat}
            />
          </div>
          <div className={`${leftTab === 'schema' ? 'flex flex-col flex-1 min-h-0' : 'hidden'}`}>
            <SchemaExplorer
              loadedFiles={loadedFiles}
              schemas={schemas}
              profiles={profiles}
              onFetch={fetchSchema}
              onFetchAll={fetchAll}
              onProfile={profileFile}
            />
          </div>
        </div>

        {/* Drag handle */}
        {resultPaneOpen && (
          <div
            onMouseDown={handleDividerMouseDown}
            className="w-1 shrink-0 cursor-col-resize relative group"
            title="Drag to resize"
          >
            {/* Visible line — brightens on hover/drag */}
            <div className="absolute inset-y-0 left-0 w-px bg-gray-200 dark:bg-gray-700 group-hover:bg-blue-400 transition-colors" />
            {/* Wider invisible grab area */}
            <div className="absolute inset-y-0 -left-1.5 -right-1.5" />
          </div>
        )}

        {/* Right: Result Pane */}
        {resultPaneOpen && (
          <div className="flex flex-col min-h-0 flex-1 min-w-0">
            {/* Result pane header */}
            <div className="shrink-0 flex items-center justify-between px-3 py-2 border-b border-gray-100 dark:border-gray-800 bg-white dark:bg-gray-900">
              <span className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Results</span>
              <div className="flex items-center gap-2">
                {resultHistory.length > 0 && (
                  <span className="text-xs text-gray-400 dark:text-gray-500">{resultHistory.length} result{resultHistory.length > 1 ? 's' : ''}</span>
                )}
                {resultHistory.length > 0 && (
                  <button
                    onClick={() => setResultHistory([])}
                    className="text-xs text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 px-2 py-0.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 transition"
                  >
                    Clear all
                  </button>
                )}
              </div>
            </div>
            <div className="flex-1 min-h-0 overflow-y-auto">
              <ResultPane results={resultHistory} onDismiss={(i) => setResultHistory((prev) => prev.filter((_, idx) => idx !== i))} onSendToAI={handleSendToAI} onScrollToQuery={handleScrollToQuery} />
            </div>
          </div>
        )}
      </div>

      {settingsOpen && (
        <SettingsPanel
          settings={settings}
          onUpdate={updateSettings}
          onClose={() => setSettingsOpen(false)}
        />
      )}

      {paletteOpen && (
        <CommandPalette
          onClose={() => setPaletteOpen(false)}
          onGoToTab={setLeftTab}
          onToggleSettings={() => { setSettingsOpen(v => !v) }}
          onToggleResultPane={() => setResultPaneOpen(v => !v)}
          onToggleTheme={toggleTheme}
          onToggleAutoRun={toggleAutoRun}
          onClearMessages={clearMessages}
          resultPaneOpen={resultPaneOpen}
          autoRun={autoRun}
          currentTab={leftTab}
          preference={preference}
        />
      )}
    </div>
    </ConnectionsContext.Provider>
  )
}
