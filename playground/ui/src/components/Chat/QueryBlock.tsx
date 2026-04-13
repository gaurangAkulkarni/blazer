import React, { useState, useRef, useEffect, useContext, useMemo } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { ChatStreamContext } from './ChatStreamContext'
import { ConnectionsContext } from '../../lib/ConnectionsContext'
import type { QueryResult, SnippetGroup } from '../../lib/types'

// ── Mini save-snippet popover (with group picker) ─────────────────────────────
interface SaveSnippetPopoverProps {
  groups: SnippetGroup[]
  onSave: (name: string, groupId?: string) => void
  onCancel: () => void
}
function SaveSnippetPopover({ groups, onSave, onCancel }: SaveSnippetPopoverProps) {
  const [name, setName] = useState('')
  const [groupId, setGroupId] = useState<string>('')

  const commit = () => { if (name.trim()) onSave(name.trim(), groupId || undefined) }

  return (
    <div
      className="flex items-center gap-1.5 px-2 py-1 bg-white border border-indigo-200 rounded-md shadow-sm"
      onClick={(e) => e.stopPropagation()}
    >
      <input
        autoFocus
        value={name}
        onChange={(e) => setName(e.target.value)}
        onKeyDown={(e) => { if (e.key === 'Enter') commit(); if (e.key === 'Escape') onCancel() }}
        placeholder="Snippet name…"
        className="text-xs w-32 border border-gray-200 rounded px-1.5 py-0.5 focus:outline-none focus:border-indigo-400 text-gray-700"
      />
      <select
        value={groupId}
        onChange={(e) => setGroupId(e.target.value)}
        className="text-xs border border-gray-200 rounded px-1 py-0.5 focus:outline-none focus:border-indigo-400 text-gray-600 bg-white max-w-[90px]"
      >
        <option value="">Default</option>
        {groups.map((g) => (
          <option key={g.id} value={g.id}>{g.name}</option>
        ))}
      </select>
      <button
        onClick={commit}
        disabled={!name.trim()}
        className="text-[10px] font-medium bg-indigo-600 text-white px-1.5 py-0.5 rounded hover:bg-indigo-700 disabled:opacity-40 transition"
      >
        Save
      </button>
      <button
        onClick={onCancel}
        className="text-[10px] text-gray-500 hover:text-gray-700 px-1 py-0.5 rounded hover:bg-gray-100 transition"
      >
        ✕
      </button>
    </div>
  )
}

// ── Stable query ID: derived from messageId + index + code so it survives restarts ──
// Including `index` ensures two identical SQL blocks in the same message get different IDs.
// djb2 hash → base36, 6 chars, prefixed with 'Q'
function stableQueryId(messageId: string, index: number, code: string): string {
  const input = `${messageId}|${index}|${code}`
  let h = 5381
  for (let i = 0; i < input.length; i++) {
    h = ((h << 5) + h + input.charCodeAt(i)) | 0
  }
  return 'Q' + Math.abs(h).toString(36).slice(0, 6).toUpperCase()
}

// Extract a human-readable title from the first SQL line comment (-- ...)
function extractSqlTitle(sql: string): string | undefined {
  const match = sql.match(/^\s*--\s*(.+)/m)
  return match ? match[1].trim() : undefined
}

interface Props {
  code: string
  language: string
  /** Position of this block within the parent message (0-based). Makes IDs unique even for identical SQL. */
  index?: number
  /** Position among sql-only blocks in this message (-1 for non-sql). Used for DDL preamble batching. */
  sqlIndex?: number
  /** Called with result, original query text, and which engine ran it. */
  onQueryResult?: (result: QueryResult, query: string, engine: 'blazer' | 'duckdb') => void
}

export function QueryBlock({ code, language, index = 0, sqlIndex = -1, onQueryResult }: Props) {
  // isStreaming, autoRun, messageId, existingResults, onSaveSnippet, onSendToChat, agenticMode come from context
  const { isStreaming, autoRun, agenticMode, messageId, existingResults, onSaveSnippet, snippetGroups = [], onSendToChat, sqlBlocksRef } = useContext(ChatStreamContext)
  const activeConnections = useContext(ConnectionsContext)

  // ── Stable queryId: computed once from (messageId + index + code), same across restarts ──
  const queryIdRef = useRef<string | null>(null)
  if (queryIdRef.current === null) {
    queryIdRef.current = stableQueryId(messageId, index, code)
  }
  const queryId = queryIdRef.current

  // ── Restore state from persisted queryResults (survives app restart) ──────────
  // Pick the result with the highest runNumber for this queryId
  const restoredResult = useMemo(() => {
    const matches = existingResults.filter((r) => r.queryId === queryId)
    if (matches.length === 0) return null
    return matches.reduce((best, r) => ((r.runNumber ?? 0) > (best.runNumber ?? 0) ? r : best))
  }, []) // eslint-disable-line react-hooks/exhaustive-deps — intentionally runs only on mount

  const [savingSnippet, setSavingSnippet] = useState(false)
  const [snippetSaved, setSnippetSaved] = useState(false)

  const [status, setStatus] = useState<'idle' | 'running' | 'success' | 'error'>(() => {
    if (!restoredResult) return 'idle'
    return restoredResult.success ? 'success' : 'error'
  })
  const [result, setResult] = useState<QueryResult | null>(() => restoredResult)
  const [copied, setCopied] = useState(false)
  const [elapsed, setElapsed] = useState(0)

  // runCountRef tracks the latest run number; restored from persisted result on mount
  const runCountRef = useRef(restoredResult?.runNumber ?? 0)
  const cancelledRef = useRef(false)
  const startTimeRef = useRef<number>(0)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Track previous isStreaming to detect the true → false transition
  const prevStreamingRef = useRef(isStreaming)

  useEffect(() => {
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [])

  const startTimer = () => {
    startTimeRef.current = Date.now()
    setElapsed(0)
    timerRef.current = setInterval(() => {
      setElapsed(Date.now() - startTimeRef.current)
    }, 100)
  }

  const stopTimer = () => {
    if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null }
  }

  const formatElapsed = (ms: number) => {
    if (ms < 1000) return `${ms}ms`
    const s = Math.floor(ms / 1000)
    const m = Math.floor(s / 60)
    const h = Math.floor(m / 60)
    if (h > 0) return `${h}h ${m % 60}m ${s % 60}s`
    if (m > 0) return `${m}m ${s % 60}s`
    const dec = String(ms).slice(-3, -1)
    return `${s}.${dec}s`
  }

  const isJsonQuery = language === 'json' && isValidQueryJson(code)
  const isSqlQuery = language === 'sql' && code.trim().length > 0
  const isRunnable = isJsonQuery || isSqlQuery

  // Clean SQL for both display and execution — strips hallucinated fence/DONE lines
  const displayCode = isSqlQuery
    ? code
        .split('\n')
        .filter(line => !/^\s*`{3,}/.test(line))
        .filter(line => !/^\s*DONE\s*$/i.test(line))
        .filter(line => !/^\s*sql\s*$/i.test(line))
        .join('\n')
        .trim()
    : code

  const handleRun = async () => {
    if (!isRunnable) return
    const runNumber = ++runCountRef.current
    cancelledRef.current = false
    setStatus('running')
    setResult(null)
    startTimer()
    try {
      let r: QueryResult
      if (isSqlQuery) {
        // Prepend only DDL blocks (CREATE, INSERT, DROP, ALTER, etc.) from earlier sql blocks
        // in the same message so they run in the same connection. Pure SELECTs are skipped —
        // they produce no persistent state and re-running them would be wasteful.
        const DDL_RE = /^\s*(CREATE|DROP|ALTER|INSERT|UPDATE|DELETE|COPY\b)/i
        const allSqlsInMessage = sqlBlocksRef?.current ?? []
        const precedingSqls = sqlIndex > 0
          ? allSqlsInMessage.slice(0, sqlIndex).filter(s => DDL_RE.test(s))
          : []
        if (precedingSqls.length > 0 && activeConnections.length === 0) {
          const allSqls = [...precedingSqls, displayCode]
          const results = await invoke<QueryResult[]>('run_duckdb_batch', { sqls: allSqls })
          // Use the last result (this block's result); stop early on first error
          const lastResult = results[results.length - 1]
          const firstError = results.find((res) => !res.success)
          r = firstError ?? lastResult
        } else {
          r = activeConnections.length > 0
            ? await invoke<QueryResult>('run_duckdb_query_with_connections', { sql: displayCode, connections: activeConnections })
            : await invoke<QueryResult>('run_duckdb_query', { sql: displayCode })
        }
      } else {
        const parsed = JSON.parse(code)
        r = await invoke<QueryResult>('run_query', { query: parsed })
      }
      stopTimer()
      if (cancelledRef.current) return
      const enriched: QueryResult = { ...r, queryId, runNumber, title: extractSqlTitle(code) }
      setResult(enriched)
      setStatus(r.success ? 'success' : 'error')
      if (onQueryResult) onQueryResult(enriched, code, isSqlQuery ? 'duckdb' : 'blazer')
    } catch (e: any) {
      stopTimer()
      if (cancelledRef.current) return
      const errResult: QueryResult = { success: false, error: e.message, data: [], columns: [], shape: [0, 0], duration_ms: 0, queryId, runNumber }
      setResult(errResult)
      setStatus('error')
    }
  }

  const scrollToResult = () => {
    const runNumber = runCountRef.current
    const el = document.getElementById(`result-${queryId}-${runNumber}`)
    if (!el) return

    // Tell the ResultCard to expand if it's collapsed
    el.dispatchEvent(new CustomEvent('blazer:expand-result', { bubbles: false }))

    el.scrollIntoView({ behavior: 'smooth', block: 'nearest' })

    // Prominent flash: indigo ring + tinted background, holds 600ms then fades 1.2s
    el.style.transition = 'none'
    el.style.boxShadow = '0 0 0 3px #6366f1, 0 0 16px 2px rgba(99,102,241,0.25)'
    el.style.backgroundColor = 'rgba(99, 102, 241, 0.08)'
    el.style.borderRadius = '8px'

    // Force repaint so initial state is committed before transition starts
    void el.offsetHeight

    setTimeout(() => {
      el.style.transition = 'box-shadow 1.2s ease-out, background-color 1.2s ease-out'
      el.style.boxShadow = '0 0 0 0px rgba(99,102,241,0)'
      el.style.backgroundColor = 'rgba(99,102,241,0)'
    }, 600)

    setTimeout(() => {
      el.style.transition = ''
      el.style.boxShadow = ''
      el.style.backgroundColor = ''
      el.style.borderRadius = ''
    }, 2000)
  }

  // ── Autorun: fires when streaming transitions true → false ────────────────
  useEffect(() => {
    const wasStreaming = prevStreamingRef.current
    prevStreamingRef.current = isStreaming
    if (wasStreaming && !isStreaming && (autoRun || agenticMode) && isRunnable && status === 'idle') {
      handleRun()
    }
  }) // no dep array: runs after every render to catch the exact transition

  const handleStop = () => {
    stopTimer()
    cancelledRef.current = true
    setStatus('idle')
    setResult(null)
  }

  const handleCopy = () => {
    navigator.clipboard.writeText(displayCode)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const langLabel = isJsonQuery ? 'blazer query' : isSqlQuery ? 'duckdb query' : language

  const handleSaveSnippet = (name: string, groupId?: string) => {
    const engine: 'blazer' | 'duckdb' = isSqlQuery ? 'duckdb' : 'blazer'
    onSaveSnippet?.(code, engine, name, groupId)
    setSavingSnippet(false)
    setSnippetSaved(true)
    setTimeout(() => setSnippetSaved(false), 2000)
  }

  return (
    <div id={`qblock-${queryId}`} className="my-2 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-1.5 bg-gray-100 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 text-xs text-gray-500 dark:text-gray-400">
        <span className="font-medium flex items-center gap-1.5">
          {isRunnable && (
            <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="text-blue-500">
              <polygon points="5 3 19 12 5 21 5 3"/>
            </svg>
          )}
          {langLabel}
          {/* Query ID badge */}
          <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded bg-gray-200 dark:bg-gray-700 text-gray-500 dark:text-gray-400 font-mono font-semibold tracking-tight text-[10px]">
            {queryId}
          </span>
          {/* View result link — only after at least one run */}
          {status !== 'idle' && status !== 'running' && (
            <button
              onClick={scrollToResult}
              className="flex items-center gap-0.5 text-indigo-400 hover:text-indigo-600 transition"
              title="Scroll to result"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/>
              </svg>
              result
            </button>
          )}
        </span>
        <div className="flex items-center gap-1.5">
          {/* Save as Snippet */}
          {isRunnable && !isStreaming && onSaveSnippet && (
            savingSnippet ? (
              <SaveSnippetPopover
                groups={snippetGroups}
                onSave={handleSaveSnippet}
                onCancel={() => setSavingSnippet(false)}
              />
            ) : snippetSaved ? (
              <span className="flex items-center gap-1 text-[10px] text-green-600 font-medium px-1.5">
                <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
                Saved!
              </span>
            ) : (
              <button
                onClick={() => setSavingSnippet(true)}
                title="Save as snippet"
                className="p-1 rounded hover:bg-gray-200 dark:hover:bg-gray-700 transition text-gray-400 dark:text-gray-500 hover:text-indigo-500"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>
                </svg>
              </button>
            )
          )}
          {!isStreaming && (
            <button onClick={handleCopy} className="px-2 py-0.5 rounded hover:bg-gray-200 dark:hover:bg-gray-700 transition text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200">
              {copied ? 'Copied!' : 'Copy'}
            </button>
          )}
          {isRunnable && !isStreaming && (
            <div className="flex items-center gap-1">
              {status === 'running' && (
                <button
                  onClick={handleStop}
                  className="px-2.5 py-0.5 rounded font-medium transition text-xs bg-red-100 text-red-600 hover:bg-red-200 flex items-center gap-1.5"
                  title="Stop query"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="currentColor" stroke="none">
                    <rect x="4" y="4" width="16" height="16" rx="2"/>
                  </svg>
                  Stop
                </button>
              )}
              <button
                onClick={handleRun}
                disabled={status === 'running'}
                className={`px-2.5 py-0.5 rounded font-medium transition text-xs ${
                  status === 'running' ? 'bg-gray-200 dark:bg-gray-700 text-gray-400 cursor-wait'
                  : status === 'success' ? 'bg-green-600 text-white hover:bg-green-700'
                  : status === 'error' ? 'bg-red-500 text-white hover:bg-red-600'
                  : 'bg-blue-600 text-white hover:bg-blue-700'
                }`}
              >
                {status === 'running' ? (
                  <span className="flex items-center gap-1.5">
                    <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="animate-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
                    <span className="tabular-nums">{formatElapsed(elapsed)}</span>
                  </span>
                ) : status === 'success' ? (
                  <span className="flex items-center gap-1.5">
                    <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
                    Done
                  </span>
                ) : status === 'error' ? (
                  <span className="flex items-center gap-1.5">
                    <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                    Error
                  </span>
                ) : (
                  <span className="flex items-center gap-1.5">
                    <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="currentColor" stroke="none"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                    Run Query
                  </span>
                )}
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Code — use displayCode so fence/DONE artifacts are stripped from view too */}
      <pre className="m-0 p-3 bg-gray-50 dark:bg-gray-900 text-xs font-mono overflow-x-auto text-gray-700 dark:text-gray-300 leading-relaxed max-h-64 overflow-y-auto">
        {displayCode}
      </pre>

      {/* Inline error only — success results go to the Result Pane */}
      {result && !result.success && (
        <div className="border-t border-gray-200 dark:border-gray-700 p-3 bg-white dark:bg-gray-900">
          <div className="relative group/err">
            <div className="text-xs text-red-600 font-mono bg-red-50 rounded p-2 border border-red-100 whitespace-pre-wrap break-words overflow-x-auto max-h-48 overflow-y-auto pr-8">
              {result.error}
            </div>
            {onSendToChat && (
              <button
                onClick={() => {
                  const lang = isSqlQuery ? 'sql' : 'json'
                  const text = `I got this error, can you help fix it?\n\n\`\`\`${lang}\n${displayCode}\n\`\`\`\n\nError:\n\`\`\`\n${result.error}\n\`\`\``
                  onSendToChat(text)
                }}
                title="Send query + error to chat"
                className="absolute top-1.5 right-1.5 p-1 rounded text-red-300 hover:text-red-600 hover:bg-red-100 transition opacity-0 group-hover/err:opacity-100 focus:opacity-100"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
                </svg>
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function isValidQueryJson(code: string): boolean {
  try {
    const obj = JSON.parse(code)
    return obj && typeof obj === 'object' && 'source' in obj
  } catch {
    return false
  }
}
