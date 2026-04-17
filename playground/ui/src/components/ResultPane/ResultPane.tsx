import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { save as dialogSave } from '@tauri-apps/plugin-dialog'
import type { QueryResult } from '../../lib/types'
import { ChartView } from './ChartView'
import { computeStats, ColumnStatsPopover } from './ColumnStatsPopover'
import type { ColumnStats } from './ColumnStatsPopover'

// ── shared CSV builder ────────────────────────────────────────────────────────
function buildCsv(result: QueryResult): string {
  const escape = (v: unknown) => {
    if (v === null || v === undefined) return ''
    const s = String(v)
    return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s
  }
  const cols = result.columns ?? []
  const data = result.data ?? []
  const header = cols.join(',')
  const rows = data.map((row) => cols.map((c) => escape(row[c])).join(','))
  return [header, ...rows].join('\n')
}

// ── JSON builder ─────────────────────────────────────────────────────────────
function buildJson(result: QueryResult): string {
  return JSON.stringify(result.data ?? [], null, 2)
}

// ── Export dropdown ───────────────────────────────────────────────────────────
interface ExportMenuProps {
  result: QueryResult
  label: string  // e.g. "result-1"
}

function ExportMenu({ result, label }: ExportMenuProps) {
  const [open, setOpen] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [exportError, setExportError] = useState<string | null>(null)
  const menuRef = useRef<HTMLDivElement>(null)

  // Close on outside click
  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const downloadBlob = (content: string, mime: string, filename: string) => {
    const blob = new Blob([content], { type: mime })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = filename; a.click()
    URL.revokeObjectURL(url)
    setOpen(false)
  }

  const exportCsv = () => {
    downloadBlob(buildCsv(result), 'text/csv', `${label}.csv`)
  }

  const exportJson = () => {
    downloadBlob(buildJson(result), 'application/json', `${label}.json`)
  }

  const exportParquet = async () => {
    setOpen(false)
    setExportError(null)
    try {
      const filePath = await dialogSave({
        title: 'Export as Parquet',
        defaultPath: `${label}.parquet`,
        filters: [{ name: 'Parquet files', extensions: ['parquet'] }],
      })
      if (!filePath) return  // user cancelled
      setExporting(true)
      await invoke('export_to_parquet', { data: result.data, path: filePath })
    } catch (e: any) {
      setExportError(String(e))
      setTimeout(() => setExportError(null), 4000)
    } finally {
      setExporting(false)
    }
  }

  return (
    <div className="relative" ref={menuRef}>
      {exportError && (
        <span className="absolute bottom-full right-0 mb-1 text-[10px] text-red-500 bg-white border border-red-200 rounded px-2 py-1 whitespace-nowrap shadow-sm z-30">
          {exportError}
        </span>
      )}
      <button
        onClick={() => setOpen((v) => !v)}
        disabled={exporting}
        title="Export results"
        className={`flex items-center gap-0.5 text-gray-400 hover:text-gray-600 px-1.5 py-1 rounded hover:bg-gray-200 transition text-xs font-medium ${exporting ? 'opacity-50' : ''}`}
      >
        {exporting ? (
          <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="animate-spin">
            <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
          </svg>
        ) : (
          <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
            <polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>
          </svg>
        )}
        <svg xmlns="http://www.w3.org/2000/svg" width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="6 9 12 15 18 9"/>
        </svg>
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 z-20 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg py-1 min-w-[110px]">
          <button
            onClick={exportCsv}
            className="w-full text-left flex items-center gap-2 px-3 py-1.5 text-xs text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition"
          >
            <span className="text-[10px] font-semibold text-gray-400 dark:text-gray-500 w-12">CSV</span>
            <span className="text-gray-400 dark:text-gray-500 text-[10px]">spreadsheet</span>
          </button>
          <button
            onClick={exportJson}
            className="w-full text-left flex items-center gap-2 px-3 py-1.5 text-xs text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition"
          >
            <span className="text-[10px] font-semibold text-gray-400 dark:text-gray-500 w-12">JSON</span>
            <span className="text-gray-400 dark:text-gray-500 text-[10px]">row array</span>
          </button>
          <div className="mx-2 my-1 border-t border-gray-100 dark:border-gray-700" />
          <button
            onClick={exportParquet}
            className="w-full text-left flex items-center gap-2 px-3 py-1.5 text-xs text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition"
          >
            <span className="text-[10px] font-semibold text-violet-500 w-12">Parquet</span>
            <span className="text-gray-400 text-[10px]">columnar</span>
          </button>
        </div>
      )}
    </div>
  )
}

// ── tiny reusable copy button ─────────────────────────────────────────────────
function CopyButton({ getText, title, className = '' }: { getText: () => string; title: string; className?: string }) {
  const [copied, setCopied] = useState(false)
  const handleClick = useCallback((e: React.MouseEvent) => {
    e.stopPropagation()
    navigator.clipboard.writeText(getText()).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }, [getText])

  return (
    <button
      onClick={handleClick}
      className={`flex items-center gap-1 transition ${className}`}
      title={title}
    >
      {copied ? (
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="text-green-500">
          <polyline points="20 6 9 17 4 12"/>
        </svg>
      ) : (
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
        </svg>
      )}
    </button>
  )
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const s = Math.floor(ms / 1000)
  const m = Math.floor(s / 60)
  const h = Math.floor(m / 60)
  if (h > 0) return `${h}h ${m % 60}m ${s % 60}s`
  if (m > 0) return `${m}m ${s % 60}s`
  return `${s}s`
}

interface Props {
  results: QueryResult[]
  onDismiss: (index: number) => void
  onSendToAI?: (text: string) => void
  onScrollToQuery?: (queryId: string) => void
}

export function ResultPane({ results, onDismiss, onSendToAI, onScrollToQuery }: Props) {
  if (results.length === 0) {
    return (
      <div className="h-full flex flex-col items-center justify-center text-gray-400 select-none gap-3">
        <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round" className="opacity-25">
          <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
          <line x1="3" y1="9" x2="21" y2="9"/>
          <line x1="3" y1="15" x2="21" y2="15"/>
          <line x1="9" y1="3" x2="9" y2="21"/>
          <line x1="15" y1="3" x2="15" y2="21"/>
        </svg>
        <p className="text-xs text-center">Run a query to see results</p>
      </div>
    )
  }

  return (
    <div className="flex flex-col divide-y divide-gray-100 dark:divide-gray-800">
      {results.map((result, i) => (
        <ResultCard key={i} result={result} index={i} total={results.length} onDismiss={() => onDismiss(i)} onSendToAI={onSendToAI} onScrollToQuery={onScrollToQuery} />
      ))}
    </div>
  )
}

function ResultCard({ result, index, total, onDismiss, onSendToAI, onScrollToQuery }: { result: QueryResult; index: number; total: number; onDismiss: () => void; onSendToAI?: (text: string) => void; onScrollToQuery?: (queryId: string) => void }) {
  const [page, setPage] = useState(0)
  const [collapsed, setCollapsed] = useState(index > 0)
  const [showChart, setShowChart] = useState(false)
  const cardRef = useRef<HTMLDivElement>(null)

  // Expand and scroll into view when the QueryBlock fires the highlight event
  useEffect(() => {
    const el = cardRef.current
    if (!el) return
    const handler = () => setCollapsed(false)
    el.addEventListener('blazer:expand-result', handler)
    return () => el.removeEventListener('blazer:expand-result', handler)
  }, [])

  // ── Column stats (precomputed once for all columns) ──────────────────────
  const allStats = useMemo<Record<string, ColumnStats>>(
    () => Object.fromEntries((result.columns ?? []).map(col => [col, computeStats(result.data ?? [], col)])),
    [result],
  )

  // ── Hover state for column stats popover ─────────────────────────────────
  const [hoveredCol, setHoveredCol]         = useState<string | null>(null)
  const [anchorRect, setAnchorRect]         = useState<DOMRect | null>(null)
  const hideTimerRef                        = useRef<ReturnType<typeof setTimeout> | null>(null)

  const showPopover = useCallback((col: string, e: React.MouseEvent<HTMLTableCellElement>) => {
    if (hideTimerRef.current) clearTimeout(hideTimerRef.current)
    setHoveredCol(col)
    setAnchorRect(e.currentTarget.getBoundingClientRect())
  }, [])

  const hidePopover = useCallback(() => {
    hideTimerRef.current = setTimeout(() => {
      setHoveredCol(null)
      setAnchorRect(null)
    }, 120)
  }, [])
  const PAGE_SIZE = 500

  if (!result.success) {
    return (
      <div ref={cardRef} id={result.queryId ? `result-${result.queryId}-${result.runNumber ?? 1}` : undefined} className="p-3">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs text-red-500 font-medium flex items-center gap-1.5 min-w-0">
            <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="shrink-0"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
            Query {total - index}
            {result.title && (
              <span className="text-red-400 font-normal truncate max-w-[180px]" title={result.title}>
                {result.title}
              </span>
            )}
            {result.queryId && (
              <button
                onClick={() => onScrollToQuery?.(result.queryId!)}
                className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded bg-red-100 text-red-400 hover:text-red-600 hover:bg-red-200 font-mono font-semibold text-[10px] transition"
                title="Scroll to query in chat"
              >
                {result.queryId}
                {result.runNumber !== undefined && result.runNumber > 1 && <span className="opacity-60"> ·{result.runNumber}</span>}
                <svg xmlns="http://www.w3.org/2000/svg" width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="ml-0.5">
                  <line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/>
                </svg>
              </button>
            )}
          </span>
          <div className="flex items-center gap-1">
            <CopyButton
              getText={() => result.error ?? ''}
              title="Copy error"
              className="text-red-300 hover:text-red-500 p-0.5 rounded hover:bg-red-50"
            />
            {onSendToAI && (
              <button
                onClick={() => onSendToAI(`I got this SQL error, can you help me fix it?\n\n\`\`\`\n${result.error}\n\`\`\``)}
                title="Send error to chat input"
                className="flex items-center gap-1 text-red-300 hover:text-indigo-500 p-0.5 rounded hover:bg-indigo-50 transition"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
                  <line x1="12" y1="8" x2="12" y2="13"/><line x1="9.5" y1="10.5" x2="12" y2="8"/><line x1="14.5" y1="10.5" x2="12" y2="8"/>
                </svg>
              </button>
            )}
            <button onClick={onDismiss} className="text-gray-300 hover:text-gray-500 p-0.5 rounded transition">
              <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
          </div>
        </div>
        <div className="text-xs text-red-600 font-mono bg-red-50 rounded p-2 border border-red-100 whitespace-pre-wrap break-words overflow-x-auto max-h-48 overflow-y-auto">
          {result.error}
        </div>
      </div>
    )
  }

  const [totalRows, totalCols] = result.shape ?? [result.data.length, result.columns.length]
  const start = page * PAGE_SIZE
  const end = Math.min(start + PAGE_SIZE, (result.data ?? []).length)
  const displayRows = (result.data ?? []).slice(start, end)
  const totalPages = Math.ceil((result.data ?? []).length / PAGE_SIZE)

  const exportLabel = `result-${total - index}`

  return (
    <div ref={cardRef} id={result.queryId ? `result-${result.queryId}-${result.runNumber ?? 1}` : undefined} className="flex flex-col">
      {/* Card header */}
      <div className="flex items-center justify-between px-3 py-2 bg-gray-50 dark:bg-gray-800 hover:bg-gray-100/80 dark:hover:bg-gray-700/80 transition cursor-pointer select-none"
        onClick={() => setCollapsed((v) => !v)}
      >
        <div className="flex items-center gap-2 text-xs">
          <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={`text-gray-400 transition-transform ${collapsed ? '-rotate-90' : ''}`}>
            <polyline points="6 9 12 15 18 9"/>
          </svg>
          <span className="font-semibold text-gray-700 dark:text-gray-300">Query {total - index}</span>
          {result.title && (
            <span className="text-gray-400 dark:text-gray-500 font-normal truncate max-w-[200px]" title={result.title}>
              {result.title}
            </span>
          )}
          {/* QueryId badge — click to scroll to the originating QueryBlock in chat */}
          {result.queryId && (
            <button
              onClick={(e) => { e.stopPropagation(); onScrollToQuery?.(result.queryId!) }}
              className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded bg-indigo-50 text-indigo-400 hover:text-indigo-700 hover:bg-indigo-100 font-mono font-semibold text-[10px] transition"
              title="Scroll to query in chat"
            >
              {result.queryId}
              {result.runNumber !== undefined && result.runNumber > 1 && <span className="opacity-60"> ·{result.runNumber}</span>}
              <svg xmlns="http://www.w3.org/2000/svg" width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="ml-0.5">
                <line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/>
              </svg>
            </button>
          )}
          <span className="text-gray-400 dark:text-gray-500">{totalRows.toLocaleString()} × {totalCols}</span>
          {result.duration_ms > 0 && <span className="text-gray-400 dark:text-gray-500">{formatDuration(result.duration_ms)}</span>}
        </div>
        <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
          {/* Chart toggle */}
          <button
            onClick={() => { setShowChart(v => !v); setCollapsed(false) }}
            title={showChart ? 'Hide chart' : 'Show chart'}
            className={`flex items-center gap-1 px-2 py-1 rounded text-xs font-medium transition-all ${
              showChart
                ? 'bg-indigo-100 dark:bg-indigo-900/40 text-indigo-700 dark:text-indigo-400 hover:bg-indigo-200'
                : 'text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-700'
            }`}
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none"
              stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/>
              <line x1="6" y1="20" x2="6" y2="14"/><line x1="3" y1="20" x2="21" y2="20"/>
            </svg>
            Chart
          </button>
          <CopyButton
            getText={() => buildCsv(result)}
            title="Copy as CSV"
            className="text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 p-1 rounded hover:bg-gray-200 dark:hover:bg-gray-700"
          />
          {onSendToAI && (
            <button
              onClick={() => {
                const cols = result.columns ?? []
                const data = result.data ?? []
                const preview = data.slice(0, 20)
                const csv = [cols.join(','), ...preview.map(row => cols.map(c => {
                  const v = row[c]; if (v === null || v === undefined) return ''
                  const s = String(v); return s.includes(',') || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s
                }).join(','))].join('\n')
                const truncNote = data.length > 20 ? `\n… (${data.length - 20} more rows not shown)` : ''
                onSendToAI(`Query result — ${totalRows.toLocaleString()} rows × ${totalCols} cols:\n\`\`\`csv\n${csv}${truncNote}\n\`\`\``)
              }}
              title="Send result to chat input"
              className="flex items-center gap-1 text-gray-400 dark:text-gray-500 hover:text-indigo-500 dark:hover:text-indigo-400 p-1 rounded hover:bg-indigo-50 dark:hover:bg-indigo-900/30 transition"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
              </svg>
            </button>
          )}
          <ExportMenu result={result} label={exportLabel} />
          <button onClick={onDismiss} className="text-gray-300 dark:text-gray-600 hover:text-gray-500 dark:hover:text-gray-400 p-1 rounded hover:bg-gray-200 dark:hover:bg-gray-700 transition" title="Dismiss">
            <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          </button>
        </div>
      </div>

      {/* Table — collapsible */}
      {!collapsed && (
        <>
          {/* Chart — above the table */}
          {showChart && <ChartView result={result} />}

          {/* Column stats popover — rendered outside <table> to keep valid HTML */}
          {hoveredCol && anchorRect && allStats[hoveredCol] && (
            <ColumnStatsPopover
              col={hoveredCol}
              stats={allStats[hoveredCol]}
              anchorRect={anchorRect}
            />
          )}

          <div className="data-table-scroll">
            <table className="min-w-full text-xs border-collapse">
              <thead className="sticky top-0 z-10 bg-white dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
                <tr>
                  <th className="px-2 py-2 text-left text-gray-300 dark:text-gray-600 font-medium w-8 select-none">#</th>
                  {result.columns.map((col) => (
                    <th
                      key={col}
                      onMouseEnter={(e) => showPopover(col, e)}
                      onMouseLeave={hidePopover}
                      className="px-3 py-2 text-left text-gray-600 dark:text-gray-400 font-semibold whitespace-nowrap
                        cursor-default select-none group/th hover:bg-blue-50/60 dark:hover:bg-blue-900/20 transition-colors"
                    >
                      <span className="flex items-center gap-1">
                        {col}
                        {/* Subtle indicator that stats are available */}
                        <svg
                          xmlns="http://www.w3.org/2000/svg" width="9" height="9"
                          viewBox="0 0 24 24" fill="none" stroke="currentColor"
                          strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
                          className="text-gray-300 opacity-0 group-hover/th:opacity-100 transition-opacity shrink-0"
                        >
                          <line x1="18" y1="20" x2="18" y2="10"/>
                          <line x1="12" y1="20" x2="12" y2="4"/>
                          <line x1="6" y1="20" x2="6" y2="14"/>
                          <line x1="3" y1="20" x2="21" y2="20"/>
                        </svg>
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {displayRows.map((row, i) => (
                  <tr key={start + i} className={`${(start + i) % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50/40 dark:bg-gray-800/40'} hover:bg-blue-50/40 dark:hover:bg-blue-900/10 transition-colors`}>
                    <td className="px-2 py-1.5 text-gray-300 dark:text-gray-600 border-b border-gray-100 dark:border-gray-800 select-none font-mono">{start + i}</td>
                    {result.columns.map((col) => {
                      const val = row[col]
                      const isNum = typeof val === 'number'
                      return (
                        <td key={col} className={`px-3 py-1.5 border-b border-gray-100 dark:border-gray-800 ${isNum ? 'text-right tabular-nums text-blue-700 dark:text-blue-400' : 'text-gray-700 dark:text-gray-300'} ${val === null ? 'text-gray-300 dark:text-gray-600 italic' : ''}`}>
                          {/* Wrapper div — overflow:hidden on <td> is ignored without table-layout:fixed */}
                          <div
                            className={`overflow-hidden text-ellipsis whitespace-nowrap ${
                              isNum ? 'min-w-[50px] text-right' : 'max-w-[200px]'
                            }`}
                            title={val === null ? undefined : String(val)}
                          >
                            {val === null
                              ? 'null'
                              : isNum
                                ? Number.isInteger(val)
                                  ? (val as number).toString()
                                  : (val as number).toLocaleString('en-US', { maximumFractionDigits: 4 })
                                : String(val)}
                          </div>
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {totalPages > 1 && (
            <div className="flex items-center justify-between px-3 py-2 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 dark:text-gray-400">
              <span>Rows {(start + 1).toLocaleString()}–{end.toLocaleString()} of {result.data.length.toLocaleString()}</span>
              <div className="flex items-center gap-1">
                <button onClick={() => setPage((p) => Math.max(0, p - 1))} disabled={page === 0} className="px-2 py-1 rounded hover:bg-gray-200 disabled:opacity-30 transition">
                  <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="15 18 9 12 15 6"/></svg>
                </button>
                <span className="px-1">{page + 1} / {totalPages}</span>
                <button onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1} className="px-2 py-1 rounded hover:bg-gray-200 disabled:opacity-30 transition">
                  <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="9 18 15 12 9 6"/></svg>
                </button>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
