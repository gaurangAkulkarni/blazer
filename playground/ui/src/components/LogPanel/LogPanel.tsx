import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { appLog } from '../../lib/appLog'
import type { LogEntry, LogCategory, LogLevel } from '../../lib/appLog'
import { X, Search, Trash2 } from 'lucide-react'

// ── Category display config ───────────────────────────────────────────────────

const CATEGORY_LABEL: Record<LogCategory, string> = {
  llm:     'LLM ',
  sql:     'SQL ',
  tool:    'TOOL',
  file:    'FILE',
  agentic: 'AGNT',
  app:     'APP ',
}

const CATEGORY_COLOR: Record<LogCategory, string> = {
  llm:     'text-blue-400',
  sql:     'text-cyan-400',
  tool:    'text-purple-400',
  file:    'text-orange-400',
  agentic: 'text-indigo-400',
  app:     'text-gray-400',
}

const LEVEL_COLOR: Record<LogLevel, string> = {
  debug: 'text-gray-500',
  info:  'text-green-400',
  warn:  'text-yellow-400',
  error: 'text-red-500',
}

// ── Category filter options ───────────────────────────────────────────────────

type FilterCategory = LogCategory | 'errors'

const FILTER_OPTIONS: { id: FilterCategory; label: string }[] = [
  { id: 'llm',     label: 'LLM'     },
  { id: 'sql',     label: 'SQL'     },
  { id: 'tool',    label: 'Tool'    },
  { id: 'file',    label: 'File'    },
  { id: 'agentic', label: 'Agentic' },
  { id: 'app',     label: 'App'     },
  { id: 'errors',  label: 'Errors'  },
]

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatTs(ts: number): string {
  const d = new Date(ts)
  const hh = String(d.getHours()).padStart(2, '0')
  const mm = String(d.getMinutes()).padStart(2, '0')
  const ss = String(d.getSeconds()).padStart(2, '0')
  const ms = String(d.getMilliseconds()).padStart(3, '0')
  return `${hh}:${mm}:${ss}.${ms}`
}

function prettyData(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

// ── Log row ───────────────────────────────────────────────────────────────────

interface LogRowProps {
  entry: LogEntry
  stripe: boolean
}

const LogRow = React.memo(function LogRow({ entry, stripe }: LogRowProps) {
  const [expanded, setExpanded] = useState(false)

  const isError = entry.level === 'error'
  const rowBg = isError
    ? 'bg-red-950/30'
    : stripe
      ? 'bg-gray-900'
      : 'bg-gray-950'

  return (
    <div className={`${rowBg} px-2`}>
      <div className="flex items-center gap-2 py-0.5 min-h-[20px]">
        {/* Timestamp */}
        <span className="shrink-0 text-gray-500 tabular-nums select-none w-[88px]">
          {formatTs(entry.ts)}
        </span>

        {/* Category badge — whitespace-nowrap prevents ] wrapping to second line */}
        <span className={`shrink-0 whitespace-nowrap font-mono font-semibold text-[10px] ${CATEGORY_COLOR[entry.category]}`}>
          [{CATEGORY_LABEL[entry.category]}]
        </span>

        {/* Level color dot (only for non-info) */}
        {entry.level !== 'info' && (
          <span className={`shrink-0 text-[10px] font-semibold ${LEVEL_COLOR[entry.level]} uppercase`}>
            {entry.level}
          </span>
        )}

        {/* Message */}
        <span className={`flex-1 min-w-0 truncate ${LEVEL_COLOR[entry.level]}`}>
          {entry.message}
        </span>

        {/* Expand data button */}
        {entry.data && (
          <button
            onClick={() => setExpanded((v) => !v)}
            className="shrink-0 text-gray-600 hover:text-gray-400 transition text-[10px] font-mono px-1 rounded hover:bg-gray-800"
            title={expanded ? 'Collapse' : 'Expand data'}
          >
            {expanded ? 'hide' : '···'}
          </button>
        )}
      </div>

      {/* Expanded data */}
      {expanded && entry.data && (
        <div className="pb-1 pl-[136px] pr-2">
          <pre className="text-[10px] text-gray-400 bg-gray-900 rounded p-2 overflow-x-auto whitespace-pre-wrap break-words border border-gray-800 leading-relaxed">
            {prettyData(entry.data)}
          </pre>
        </div>
      )}
    </div>
  )
})

// ── Main component ────────────────────────────────────────────────────────────

export function LogPanel() {
  const [entries, setEntries] = useState<LogEntry[]>(() => appLog.getAll())
  const [activeFilters, setActiveFilters] = useState<Set<FilterCategory>>(new Set())
  const [searchRaw, setSearchRaw] = useState('')
  const [searchDebounced, setSearchDebounced] = useState('')

  const scrollRef = useRef<HTMLDivElement>(null)
  const isNearBottomRef = useRef(true)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Subscribe to appLog updates
  useEffect(() => {
    const unsub = appLog.subscribe((all) => {
      setEntries(all)
    })
    return unsub
  }, [])

  // Debounce search 150ms
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      setSearchDebounced(searchRaw.trim().toLowerCase())
    }, 150)
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [searchRaw])

  // Track scroll position for auto-scroll logic
  const handleScroll = useCallback(() => {
    const el = scrollRef.current
    if (!el) return
    const distFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight
    isNearBottomRef.current = distFromBottom <= 120
  }, [])

  // Scroll to bottom helper — two rAF passes: first ensures layout is computed,
  // second fires after the browser has painted the new rows.
  const scrollToBottom = useCallback(() => {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        const el = scrollRef.current
        if (el) el.scrollTop = el.scrollHeight
      })
    })
  }, [])

  // Scroll to bottom on initial mount (unconditional)
  useEffect(() => {
    scrollToBottom()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-scroll when a new entry arrives and user hasn't scrolled up.
  // Track last entry ID rather than length so this fires even when the
  // 500-entry ring buffer wraps (length stays at 500 but content changes).
  const lastEntryId = entries[entries.length - 1]?.id
  useEffect(() => {
    if (isNearBottomRef.current) scrollToBottom()
  }, [lastEntryId]) // eslint-disable-line react-hooks/exhaustive-deps

  // Filter + search (memoized)
  const filtered = useMemo(() => {
    let result = entries

    if (activeFilters.size > 0) {
      result = result.filter((e) => {
        if (activeFilters.has('errors') && e.level === 'error') return true
        if (activeFilters.has(e.category as FilterCategory)) return true
        // If 'errors' is the only filter, don't show non-errors even if category matches
        if (activeFilters.has('errors') && !activeFilters.has(e.category as FilterCategory)) return e.level === 'error'
        return false
      })
    }

    if (searchDebounced) {
      result = result.filter((e) => {
        const inMsg  = e.message.toLowerCase().includes(searchDebounced)
        const inData = e.data?.toLowerCase().includes(searchDebounced) ?? false
        return inMsg || inData
      })
    }

    return result
  }, [entries, activeFilters, searchDebounced])

  const toggleFilter = useCallback((id: FilterCategory) => {
    setActiveFilters((prev) => {
      const next = new Set(prev)
      if (next.has(id)) {
        next.delete(id)
      } else {
        next.add(id)
      }
      return next
    })
  }, [])

  const handleClear = useCallback(() => {
    appLog.clear()
  }, [])

  return (
    <div className="flex flex-col flex-1 min-h-0 bg-gray-950 text-gray-200 font-mono text-[11px] leading-5">
      {/* Header */}
      <div className="shrink-0 flex items-center gap-2 px-3 py-2 border-b border-gray-800 bg-gray-900 flex-wrap">
        {/* Title */}
        <span className="text-gray-300 font-semibold text-[11px] uppercase tracking-widest shrink-0">
          App Log
        </span>

        {/* Divider */}
        <div className="w-px h-4 bg-gray-700 shrink-0" />

        {/* Filter buttons */}
        <div className="flex items-center gap-1 flex-wrap">
          {FILTER_OPTIONS.map(({ id, label }) => (
            <button
              key={id}
              onClick={() => toggleFilter(id)}
              className={`text-[10px] px-2 py-0.5 rounded font-medium transition border ${
                activeFilters.has(id)
                  ? 'bg-gray-700 border-gray-500 text-gray-100'
                  : 'bg-transparent border-gray-700 text-gray-500 hover:text-gray-300 hover:border-gray-500'
              }`}
            >
              {label}
            </button>
          ))}
        </div>

        {/* Search */}
        <div className="flex items-center gap-1.5 flex-1 min-w-[120px]">
          <Search size={11} className="text-gray-600 shrink-0" />
          <input
            type="text"
            value={searchRaw}
            onChange={(e) => setSearchRaw(e.target.value)}
            placeholder="Search logs…"
            className="flex-1 bg-transparent text-[11px] text-gray-300 placeholder-gray-600 outline-none border-b border-gray-700 focus:border-gray-500 transition pb-px"
          />
          {searchRaw && (
            <button
              onClick={() => setSearchRaw('')}
              className="text-gray-600 hover:text-gray-400 transition"
            >
              <X size={11} />
            </button>
          )}
        </div>

        {/* Entry count */}
        <span className="text-gray-600 text-[10px] tabular-nums shrink-0">
          {filtered.length.toLocaleString()} / {entries.length.toLocaleString()}
        </span>

        {/* Clear button */}
        <button
          onClick={handleClear}
          title="Clear in-memory log"
          className="shrink-0 flex items-center gap-1 text-[10px] text-gray-600 hover:text-red-400 transition px-1.5 py-0.5 rounded hover:bg-gray-800 border border-transparent hover:border-gray-700"
        >
          <Trash2 size={10} />
          Clear
        </button>
      </div>

      {/* Log list */}
      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden"
      >
        {filtered.length === 0 ? (
          <div className="flex items-center justify-center h-full text-gray-700 text-[11px] select-none">
            {entries.length === 0 ? 'No log entries yet' : 'No entries match current filters'}
          </div>
        ) : (
          filtered.map((entry, i) => (
            <LogRow
              key={entry.id}
              entry={entry}
              stripe={i % 2 === 1}
            />
          ))
        )}
      </div>

      {/* Footer */}
      <div className="shrink-0 flex items-center justify-between px-3 py-1.5 border-t border-gray-800 bg-gray-900 text-[10px] text-gray-600">
        <span>
          {entries.length.toLocaleString()} {entries.length === 1 ? 'entry' : 'entries'} in memory
        </span>
        <span>Stored in SQLite · auto-flush 7d / 50MB</span>
      </div>
    </div>
  )
}
