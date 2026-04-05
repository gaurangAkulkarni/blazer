import React, { useState, useRef, useCallback, useEffect, useMemo } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { format as formatSqlLib } from 'sql-formatter'
import type { QueryResult, AttachedFile, SnippetGroup } from '../../lib/types'
import type { Engine } from '../../hooks/useChat'
import type { FileSchema } from '../../hooks/useSchema'
import { CodeEditor } from './CodeEditor'
import type { CodeEditorRef, SqlSchema } from './CodeEditor'

/** When set, the editor should load this query text (and engine) and focus. */
export interface ReplayRequest {
  engine: Engine
  query: string
  /** Increment to trigger a new replay even if engine+query are the same. */
  seq: number
}

interface Props {
  onResult: (result: QueryResult, query: string, engine: Engine) => void
  engine: Engine
  onEngineChange: (e: Engine) => void
  /** Optional replay request — populated by Query History "Replay" button. */
  replayRequest?: ReplayRequest
  /** Loaded files + their schemas — used to build SQL autocomplete. */
  loadedFiles?: AttachedFile[]
  schemas?: Record<string, FileSchema>
  /** Called when user saves the current query as a named snippet. */
  onSaveSnippet?: (query: string, engine: Engine, name: string, groupId?: string) => void
  snippetGroups?: SnippetGroup[]
}

const BLAZER_PLACEHOLDER = `{
  "source": { "type": "parquet_dir", "path": "/path/to/data" },
  "ops": [
    {
      "op": "with_column",
      "name": "year",
      "expr": { "year": { "col": "tpep_pickup_datetime" } }
    },
    {
      "op": "group_by",
      "keys": ["VendorID", "year"],
      "aggs": [
        { "func": "sum", "col": "fare_amount", "alias": "total_fare" },
        { "func": "count", "col": "VendorID", "alias": "trip_count" }
      ]
    },
    { "op": "sort", "by": "year", "desc": false }
  ]
}`

const DUCKDB_PLACEHOLDER = `-- DuckDB SQL
SELECT
  VendorID,
  year(tpep_pickup_datetime) AS year,
  sum(fare_amount)           AS total_fare,
  count(*)                   AS trip_count
FROM read_parquet('/path/to/data/*.parquet')
GROUP BY 1, 2
ORDER BY 2`

function formatElapsed(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const s = Math.floor(ms / 1000)
  const m = Math.floor(s / 60)
  if (m > 0) return `${m}m ${s % 60}s`
  return `${s}s`
}

export function ConsoleEditor({ onResult, engine, onEngineChange, replayRequest, loadedFiles, schemas, onSaveSnippet, snippetGroups = [] }: Props) {
  // ── Per-engine independent state ─────────────────────────────────────────
  // Each engine keeps its own query text (persisted to localStorage so it
  // survives tab-switching and app restarts), its own last elapsed time, and
  // its own parse-error banner.  Only `running` is shared because you cannot
  // execute both engines simultaneously.
  const [texts, setTexts] = useState<Record<Engine, string>>(() => ({
    blazer: localStorage.getItem('console_text_blazer') ?? '',
    duckdb: localStorage.getItem('console_text_duckdb') ?? '',
  }))
  const [elapseds, setElapseds] = useState<Record<Engine, number>>({ blazer: 0, duckdb: 0 })
  const [parseErrors, setParseErrors] = useState<Record<Engine, string | null>>({ blazer: null, duckdb: null })
  const [running, setRunning] = useState(false)

  // Convenience accessors for the active engine
  const text = texts[engine]
  const elapsed = elapseds[engine]
  const parseError = parseErrors[engine]

  const setText = useCallback((v: string) => {
    setTexts(prev => ({ ...prev, [engine]: v }))
    localStorage.setItem(`console_text_${engine}`, v)
  }, [engine])

  const setElapsed = useCallback((v: number) => {
    setElapseds(prev => ({ ...prev, [engine]: v }))
  }, [engine])

  const setParseError = useCallback((v: string | null) => {
    setParseErrors(prev => ({ ...prev, [engine]: v }))
  }, [engine])

  const [savingSnippet, setSavingSnippet] = useState(false)
  const [snippetSavedName, setSnippetSavedName] = useState<string | null>(null)
  const [snippetNameInput, setSnippetNameInput] = useState('')
  const [snippetGroupInput, setSnippetGroupInput] = useState('')

  const editorRef    = useRef<CodeEditorRef>(null)
  const startTimeRef = useRef<number>(0)
  const timerRef     = useRef<ReturnType<typeof setInterval> | null>(null)

  // ── Build SQL schema from loaded files for autocomplete ─────────────────
  const sqlSchema = useMemo<SqlSchema>(() => {
    if (engine !== 'duckdb') return {}
    const schema: SqlSchema = {}
    for (const file of loadedFiles ?? []) {
      const cols = schemas?.[file.path]?.columns.map(c => c.name)
        ?? file.columns
        ?? []
      if (cols.length === 0) continue

      // Short name (file without extension, safe for SQL identifier)
      const shortName = file.name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_')
      schema[shortName] = cols

      // Full reader expression — so user gets column hints after typing the full path
      const ext = file.ext.toLowerCase()
      const p   = file.path.replace(/'/g, "''")
      if (ext === 'csv' || ext === 'tsv') {
        schema[`read_csv_auto('${p}')`] = cols
      } else if (!ext) {
        schema[`read_parquet('${p}/**/*.parquet')`] = cols
      } else {
        schema[`read_parquet('${p}')`] = cols
      }
    }
    return schema
  }, [loadedFiles, schemas, engine])

  // Live JSON validation (Blazer only)
  useEffect(() => {
    if (engine !== 'blazer') { setParseError(null); return }
    if (!text.trim()) { setParseError(null); return }
    try {
      JSON.parse(text)
      setParseError(null)
    } catch (e: unknown) {
      setParseError(e instanceof Error ? e.message : 'Invalid JSON')
    }
  }, [text, engine])

  // Cleanup timer on unmount
  useEffect(() => () => { if (timerRef.current) clearInterval(timerRef.current) }, [])

  // Apply external replay request (from Query History)
  useEffect(() => {
    if (!replayRequest) return
    onEngineChange(replayRequest.engine)
    setTexts(prev => ({ ...prev, [replayRequest.engine]: replayRequest.query }))
    localStorage.setItem(`console_text_${replayRequest.engine}`, replayRequest.query)
    // Focus the editor after a tick so the engine switch has rendered
    setTimeout(() => editorRef.current?.focus(), 50)
  }, [replayRequest?.seq]) // eslint-disable-line react-hooks/exhaustive-deps

  const startTimer = useCallback(() => {
    setElapsed(0)
    startTimeRef.current = Date.now()
    timerRef.current = setInterval(() => {
      setElapsed(Date.now() - startTimeRef.current)
    }, 100)
  }, [setElapsed])

  const stopTimer = useCallback(() => {
    if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null }
    setElapsed(Date.now() - startTimeRef.current)
  }, [setElapsed])

  const runQuery = useCallback(async () => {
    const trimmed = text.trim()
    if (!trimmed || running) return

    if (engine === 'blazer') {
      let parsed: unknown
      try {
        parsed = JSON.parse(trimmed)
      } catch (e: unknown) {
        setParseError(e instanceof Error ? e.message : 'Invalid JSON')
        return
      }

      startTimer()
      setRunning(true)
      try {
        const result = await invoke<QueryResult>('run_query', { query: parsed })
        onResult(result, trimmed, engine)
      } catch (err) {
        onResult({
          success: false,
          error: String(err),
          data: [],
          columns: [],
          shape: [0, 0],
          duration_ms: Date.now() - startTimeRef.current,
        }, trimmed, engine)
      } finally {
        stopTimer()
        setRunning(false)
      }
    } else {
      // DuckDB mode
      startTimer()
      setRunning(true)
      try {
        const result = await invoke<QueryResult>('run_duckdb_query', { sql: trimmed })
        onResult(result, trimmed, engine)
      } catch (err) {
        onResult({
          success: false,
          error: String(err),
          data: [],
          columns: [],
          shape: [0, 0],
          duration_ms: Date.now() - startTimeRef.current,
        }, trimmed, engine)
      } finally {
        stopTimer()
        setRunning(false)
      }
    }
  }, [text, running, onResult, engine, startTimer, stopTimer])

  const formatJson = useCallback(() => {
    if (engine !== 'blazer') return
    try {
      setText(JSON.stringify(JSON.parse(text), null, 2))
    } catch { /* keep as-is */ }
  }, [text, engine])

  const formatSql = useCallback(() => {
    if (engine !== 'duckdb') return
    try {
      setText(formatSqlLib(text, {
        language: 'sql',
        tabWidth: 2,
        keywordCase: 'upper',
        indentStyle: 'standard',
        linesBetweenQueries: 1,
      }))
    } catch { /* keep as-is if parse fails */ }
  }, [text, engine])

  const isValid = engine === 'duckdb'
    ? text.trim().length > 0
    : !parseError && text.trim().length > 0

  const switchEngine = useCallback((next: Engine) => {
    onEngineChange(next)
    // No state reset needed — each engine already owns its text, elapsed, and
    // parseError independently.  Switching just changes which engine is active.
  }, [onEngineChange])

  return (
    <div className="flex flex-col h-full min-h-0">
      {/* Toolbar */}
      <div className="shrink-0 flex items-center justify-between px-3 py-2 border-b border-gray-100 dark:border-gray-800 bg-gray-50/60 dark:bg-gray-800/60">
        {/* Engine toggle + status */}
        <div className="flex items-center gap-3 min-w-0">
          {/* Segmented control */}
          <div className="flex items-center bg-gray-200/70 dark:bg-gray-700/70 rounded-lg p-0.5 gap-0.5">
            {(['blazer', 'duckdb'] as Engine[]).map((e) => (
              <button
                key={e}
                onClick={() => switchEngine(e)}
                className={`text-[11px] font-semibold px-2.5 py-1 rounded-md transition-all ${
                  engine === e
                    ? 'bg-white dark:bg-gray-600 text-gray-900 dark:text-gray-100 shadow-sm'
                    : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'
                }`}
              >
                {e === 'blazer' ? 'Blazer' : 'DuckDB'}
              </button>
            ))}
          </div>

          {/* Status */}
          {running ? (
            <span className="flex items-center gap-1.5 text-xs text-blue-600 font-medium">
              <span className="inline-block w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse" />
              Running… {formatElapsed(elapsed)}
            </span>
          ) : elapsed > 0 && !parseError ? (
            <span className="flex items-center gap-1.5 text-xs text-gray-500">
              <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
              Done in {formatElapsed(elapsed)}
            </span>
          ) : parseError ? (
            <span className="flex items-center gap-1.5 text-xs text-red-500 font-medium truncate max-w-xs" title={parseError}>
              <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
              {parseError}
            </span>
          ) : isValid ? (
            <span className="flex items-center gap-1.5 text-xs text-green-600 font-medium">
              <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
              {engine === 'blazer' ? 'Valid JSON' : 'Ready'}
            </span>
          ) : (
            <span className="text-xs text-gray-400">
              {engine === 'blazer' ? 'Paste a query JSON and press Run' : 'Type SQL and press Run'}
            </span>
          )}
        </div>

        {/* Actions */}
        <div className="flex items-center gap-1 shrink-0">
          <button
            onClick={engine === 'blazer' ? formatJson : formatSql}
            disabled={!text.trim() || running}
            title={engine === 'blazer' ? 'Format JSON (pretty-print)' : 'Format SQL (auto-indent + uppercase keywords)'}
            className="text-xs text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 px-2 py-1 rounded hover:bg-gray-200 dark:hover:bg-gray-700 disabled:opacity-30 transition font-mono"
          >
            {engine === 'blazer' ? '{ }' : 'Format'}
          </button>
          {/* Save snippet */}
          {onSaveSnippet && (
            savingSnippet ? (
              <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
                <input
                  autoFocus
                  value={snippetNameInput}
                  onChange={(e) => setSnippetNameInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && snippetNameInput.trim()) {
                      onSaveSnippet(text.trim(), engine, snippetNameInput.trim(), snippetGroupInput || undefined)
                      setSnippetSavedName(snippetNameInput.trim())
                      setSnippetNameInput(''); setSnippetGroupInput('')
                      setSavingSnippet(false)
                      setTimeout(() => setSnippetSavedName(null), 2000)
                    }
                    if (e.key === 'Escape') { setSavingSnippet(false); setSnippetNameInput(''); setSnippetGroupInput('') }
                  }}
                  placeholder="Snippet name…"
                  className="text-xs w-28 border border-gray-200 dark:border-gray-600 rounded px-1.5 py-0.5 focus:outline-none focus:border-indigo-400 text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800"
                />
                <select
                  value={snippetGroupInput}
                  onChange={(e) => setSnippetGroupInput(e.target.value)}
                  className="text-xs border border-gray-200 dark:border-gray-600 rounded px-1 py-0.5 focus:outline-none focus:border-indigo-400 text-gray-600 dark:text-gray-400 bg-white dark:bg-gray-800 max-w-[80px]"
                >
                  <option value="">Default</option>
                  {snippetGroups.map((g) => (
                    <option key={g.id} value={g.id}>{g.name}</option>
                  ))}
                </select>
                <button
                  onClick={() => {
                    if (snippetNameInput.trim()) {
                      onSaveSnippet(text.trim(), engine, snippetNameInput.trim(), snippetGroupInput || undefined)
                      setSnippetSavedName(snippetNameInput.trim())
                      setSnippetNameInput(''); setSnippetGroupInput('')
                      setSavingSnippet(false)
                      setTimeout(() => setSnippetSavedName(null), 2000)
                    }
                  }}
                  disabled={!snippetNameInput.trim()}
                  className="text-[10px] font-medium bg-indigo-600 text-white px-1.5 py-0.5 rounded hover:bg-indigo-700 disabled:opacity-40 transition"
                >
                  Save
                </button>
                <button
                  onClick={() => { setSavingSnippet(false); setSnippetNameInput(''); setSnippetGroupInput('') }}
                  className="text-[10px] text-gray-400 hover:text-gray-600 px-1 py-0.5 rounded hover:bg-gray-200 transition"
                >
                  ✕
                </button>
              </div>
            ) : snippetSavedName ? (
              <span className="flex items-center gap-1 text-[10px] text-green-600 font-medium px-1">
                <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
                Saved!
              </span>
            ) : (
              <button
                onClick={() => { if (text.trim()) setSavingSnippet(true) }}
                disabled={!text.trim() || running}
                title="Save as snippet"
                className="text-gray-400 dark:text-gray-500 hover:text-indigo-500 p-1.5 rounded hover:bg-gray-200 dark:hover:bg-gray-700 disabled:opacity-30 transition"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>
                </svg>
              </button>
            )
          )}
          <button
            onClick={() => { setText(''); setElapsed(0); setParseError(null); editorRef.current?.focus() }}
            disabled={!text.trim() || running}
            title="Clear"
            className="text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 p-1.5 rounded hover:bg-gray-200 dark:hover:bg-gray-700 disabled:opacity-30 transition"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/></svg>
          </button>
          <button
            onClick={runQuery}
            disabled={!isValid || running}
            title="Run (⌘↵)"
            className="flex items-center gap-1.5 text-xs font-semibold px-3 py-1.5 rounded-md transition
              bg-gray-900 dark:bg-white text-white dark:text-gray-900 hover:bg-gray-700 dark:hover:bg-gray-100
              disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {running ? (
              <>
                <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="animate-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
                {formatElapsed(elapsed)}
              </>
            ) : (
              <>
                <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                Run
                <kbd className="ml-0.5 text-gray-400 font-normal text-[10px]">⌘↵</kbd>
              </>
            )}
          </button>
        </div>
      </div>

      {/* Editor */}
      <div className="flex-1 min-h-0 overflow-hidden">
        <CodeEditor
          ref={editorRef}
          value={text}
          onChange={setText}
          language={engine === 'duckdb' ? 'sql' : 'json'}
          sqlSchema={sqlSchema}
          onRun={runQuery}
          placeholder={engine === 'blazer' ? BLAZER_PLACEHOLDER : DUCKDB_PLACEHOLDER}
        />
      </div>
    </div>
  )
}
