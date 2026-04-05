import React, { useState, useCallback } from 'react'
import type { QueryHistoryEntry } from '../../lib/types'
import type { Engine } from '../../hooks/useChat'

interface Props {
  history: QueryHistoryEntry[]
  onRemove: (id: string) => void
  onClear: () => void
  onReplay: (engine: Engine, query: string) => void
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function timeAgo(ts: number): string {
  const diff = Date.now() - ts
  const s = Math.floor(diff / 1000)
  if (s < 5) return 'just now'
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  const d = Math.floor(h / 24)
  return `${d}d ago`
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const s = Math.floor(ms / 1000)
  const m = Math.floor(s / 60)
  if (m > 0) return `${m}m ${s % 60}s`
  return `${s}s`
}

/** Truncate a multi-line query to a single preview line. */
function queryPreview(query: string): string {
  const first = query.trim().split('\n')[0] ?? ''
  return first.length > 80 ? first.slice(0, 77) + '…' : first
}

// ── Component ─────────────────────────────────────────────────────────────────

export function QueryHistory({ history, onRemove, onClear, onReplay }: Props) {
  const [search, setSearch] = useState('')
  const [copiedId, setCopiedId] = useState<string | null>(null)

  const handleCopy = useCallback(
    (entry: QueryHistoryEntry) => {
      navigator.clipboard.writeText(entry.query).then(() => {
        setCopiedId(entry.id)
        setTimeout(() => setCopiedId(null), 1500)
      })
    },
    [],
  )

  const filtered = search.trim()
    ? history.filter((e) =>
        e.query.toLowerCase().includes(search.toLowerCase()),
      )
    : history

  if (history.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-center px-6 py-16 gap-3">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="28"
          height="28"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="text-gray-300"
        >
          <circle cx="12" cy="12" r="10" />
          <polyline points="12 6 12 12 16 14" />
        </svg>
        <p className="text-xs text-gray-400">
          No queries yet. Run something in the Console or AI Chat.
        </p>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-full min-h-0">
      {/* Toolbar */}
      <div className="shrink-0 flex items-center gap-2 px-3 py-2 border-b border-gray-100 dark:border-gray-800 bg-gray-50/60 dark:bg-gray-800/60">
        {/* Search */}
        <div className="flex-1 relative">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="11"
            height="11"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400"
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search queries…"
            className="w-full text-xs pl-6 pr-2 py-1 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 focus:outline-none focus:border-gray-400 dark:focus:border-gray-500 text-gray-700 dark:text-gray-300 placeholder:text-gray-300 dark:placeholder:text-gray-600"
          />
        </div>
        <span className="text-xs text-gray-400 shrink-0">
          {filtered.length}/{history.length}
        </span>
        <button
          onClick={onClear}
          className="text-xs text-gray-400 dark:text-gray-500 hover:text-red-500 px-2 py-1 rounded hover:bg-gray-200 dark:hover:bg-gray-700 transition shrink-0"
        >
          Clear
        </button>
      </div>

      {/* List */}
      <div className="flex-1 min-h-0 overflow-y-auto divide-y divide-gray-100 dark:divide-gray-800">
        {filtered.length === 0 ? (
          <p className="text-xs text-gray-400 text-center py-8">No matches</p>
        ) : (
          filtered.map((entry) => (
            <HistoryRow
              key={entry.id}
              entry={entry}
              isCopied={copiedId === entry.id}
              onRemove={onRemove}
              onReplay={onReplay}
              onCopy={handleCopy}
            />
          ))
        )}
      </div>
    </div>
  )
}

// ── Row ───────────────────────────────────────────────────────────────────────

interface RowProps {
  entry: QueryHistoryEntry
  isCopied: boolean
  onRemove: (id: string) => void
  onReplay: (engine: Engine, query: string) => void
  onCopy: (entry: QueryHistoryEntry) => void
}

function HistoryRow({ entry, isCopied, onRemove, onReplay, onCopy }: RowProps) {
  const [expanded, setExpanded] = useState(false)

  return (
    <div
      className="group px-3 py-2.5 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors cursor-pointer"
      onClick={() => setExpanded((v) => !v)}
    >
      {/* Top row: engine badge + status + time */}
      <div className="flex items-center gap-1.5 mb-1">
        <span
          className={`inline-flex items-center text-[10px] font-semibold px-1.5 py-0.5 rounded-sm ${
            entry.engine === 'blazer'
              ? 'bg-violet-100 text-violet-700'
              : 'bg-yellow-100 text-yellow-700'
          }`}
        >
          {entry.engine === 'blazer' ? 'Blazer' : 'DuckDB'}
        </span>

        {entry.success ? (
          <span className="flex items-center gap-0.5 text-[10px] text-green-600">
            <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="20 6 9 17 4 12"/>
            </svg>
            {entry.rows.toLocaleString()} rows
          </span>
        ) : (
          <span className="flex items-center gap-0.5 text-[10px] text-red-500">
            <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
            </svg>
            error
          </span>
        )}

        <span className="text-[10px] text-gray-400 ml-auto shrink-0">
          {formatDuration(entry.duration_ms)} · {timeAgo(entry.timestamp)}
        </span>
      </div>

      {/* Query preview / full query */}
      <div className="font-mono text-[11px] text-gray-600 dark:text-gray-400 leading-relaxed">
        {expanded ? (
          <pre className="whitespace-pre-wrap break-words bg-gray-50 dark:bg-gray-800 rounded p-2 mt-1 text-[10.5px]">
            {entry.query}
          </pre>
        ) : (
          <span className="text-gray-500">{queryPreview(entry.query)}</span>
        )}
      </div>

      {/* Error message */}
      {!entry.success && entry.error && (
        <p className="text-[10.5px] text-red-500 mt-1 truncate" title={entry.error}>
          {entry.error}
        </p>
      )}

      {/* Action buttons — always visible on hover */}
      <div
        className="flex items-center gap-1 mt-1.5 opacity-0 group-hover:opacity-100 transition-opacity"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={() => onReplay(entry.engine as Engine, entry.query)}
          title="Replay in Console"
          className="flex items-center gap-1 text-[10px] text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 px-1.5 py-0.5 rounded hover:bg-gray-200 dark:hover:bg-gray-700 transition"
        >
          <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <polygon points="5 3 19 12 5 21 5 3"/>
          </svg>
          Replay
        </button>
        <button
          onClick={() => onCopy(entry)}
          title="Copy query"
          className="flex items-center gap-1 text-[10px] text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 px-1.5 py-0.5 rounded hover:bg-gray-200 dark:hover:bg-gray-700 transition"
        >
          {isCopied ? (
            <>
              <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
              Copied
            </>
          ) : (
            <>
              <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
              Copy
            </>
          )}
        </button>
        <button
          onClick={() => onRemove(entry.id)}
          title="Remove"
          className="ml-auto text-[10px] text-gray-400 dark:text-gray-500 hover:text-red-500 px-1.5 py-0.5 rounded hover:bg-gray-200 dark:hover:bg-gray-700 transition"
        >
          Remove
        </button>
      </div>
    </div>
  )
}
