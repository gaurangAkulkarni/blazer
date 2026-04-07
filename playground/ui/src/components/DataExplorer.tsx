import React, { useState, useEffect, useMemo, useRef } from 'react'
import { invoke } from '@tauri-apps/api/core'
import type { AttachedFile } from '../lib/types'

// ── Helpers ────────────────────────────────────────────────────────────────────

function sqlEscape(path: string) {
  return path.replace(/'/g, "''")
}

function readExpr(file: AttachedFile): string {
  const ext = file.ext.toLowerCase()
  const p = sqlEscape(file.path)
  if (ext === 'csv' || ext === 'tsv') return `read_csv_auto('${p}')`
  if (ext === 'xlsx') return `read_xlsx('${p}')`
  if (ext === 'xlsx_dir') return `read_xlsx('${p}/*.xlsx')`
  if (ext === 'csv_dir') return `read_csv_auto('${p}/*.csv')`
  if (!ext || ext === 'parquet_dir') return `read_parquet('${p}/**/*.parquet')`
  return `read_parquet('${p}')`
}

/** Build a WHERE clause that ILIKE-searches every column */
function buildWhere(columns: ColumnInfo[], term: string): string {
  // Escape single-quotes inside the search term; % and _ are treated as literals via ESCAPE
  const safe = term.replace(/'/g, "''").replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_')
  return columns
    .map(c => `TRY_CAST("${c.name.replace(/"/g, '""')}" AS VARCHAR) ILIKE '%${safe}%' ESCAPE '\\'`)
    .join('\n   OR ')
}

function typeBadgeColor(type: string): string {
  const t = type.toUpperCase()
  if (/INT|BIGINT|HUGEINT|TINYINT|SMALLINT/.test(t))
    return 'bg-blue-50 text-blue-600 border-blue-100 dark:bg-blue-900/30 dark:text-blue-400 dark:border-blue-800'
  if (/FLOAT|DOUBLE|DECIMAL|REAL|NUMERIC/.test(t))
    return 'bg-violet-50 text-violet-600 border-violet-100 dark:bg-violet-900/30 dark:text-violet-400 dark:border-violet-800'
  if (/VARCHAR|TEXT|STRING|CHAR/.test(t))
    return 'bg-gray-100 text-gray-500 border-gray-200 dark:bg-gray-700 dark:text-gray-400 dark:border-gray-600'
  if (/TIMESTAMP|DATE|TIME|INTERVAL/.test(t))
    return 'bg-orange-50 text-orange-600 border-orange-100 dark:bg-orange-900/30 dark:text-orange-400 dark:border-orange-800'
  if (/BOOL/.test(t))
    return 'bg-green-50 text-green-600 border-green-100 dark:bg-green-900/30 dark:text-green-400 dark:border-green-800'
  return 'bg-gray-100 text-gray-400 border-gray-200 dark:bg-gray-700 dark:text-gray-500 dark:border-gray-600'
}

function isNumericType(type: string) {
  return /INT|BIGINT|HUGEINT|FLOAT|DOUBLE|DECIMAL|REAL|NUMERIC|TINYINT|SMALLINT/i.test(type)
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return ''
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

// ── Types ──────────────────────────────────────────────────────────────────────

interface ColumnInfo { name: string; type: string }
type Tab = 'data' | 'schema'

interface Props {
  file: AttachedFile
  onClose: () => void
}

// ── Component ──────────────────────────────────────────────────────────────────

export function DataExplorer({ file, onClose }: Props) {
  const [tab, setTab] = useState<Tab>('data')
  const [columns, setColumns] = useState<ColumnInfo[]>([])
  const [baseRows, setBaseRows] = useState<Record<string, unknown>[]>([])  // top-100, no filter
  const [rowCount, setRowCount] = useState<number | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Search state
  const [search, setSearch] = useState('')
  const [searchRows, setSearchRows] = useState<Record<string, unknown>[] | null>(null)  // null = not searching
  const [searchTotal, setSearchTotal] = useState<number | null>(null)  // total matches in full dataset
  const [searchLoading, setSearchLoading] = useState(false)
  const [searchError, setSearchError] = useState<string | null>(null)

  const searchRef = useRef<HTMLInputElement>(null)

  // Displayed rows: searchRows when filter active, else baseRows
  const displayRows = search.trim() ? (searchRows ?? []) : baseRows

  // ── Initial load: schema + top 100 rows + row count ──────────────────────────
  useEffect(() => {
    setLoading(true)
    setError(null)
    setColumns([])
    setBaseRows([])
    setRowCount(null)
    setSearch('')
    setSearchRows(null)
    setSearchTotal(null)

    const expr = readExpr(file)

    async function load() {
      try {
        const [descRes, dataRes, cntRes] = await Promise.all([
          invoke<{ success: boolean; data: Record<string, unknown>[]; error?: string }>(
            'run_duckdb_query',
            { sql: `DESCRIBE SELECT * FROM ${expr}` },
          ),
          invoke<{ success: boolean; data: Record<string, unknown>[]; error?: string }>(
            'run_duckdb_query',
            { sql: `SELECT * FROM ${expr} LIMIT 100` },
          ),
          invoke<{ success: boolean; data: Record<string, unknown>[] }>(
            'run_duckdb_query',
            { sql: `SELECT COUNT(*) AS _cnt FROM ${expr}` },
          ).catch(() => null),
        ])

        if (!descRes.success) throw new Error(descRes.error ?? 'Failed to read schema')
        if (!dataRes.success) throw new Error(dataRes.error ?? 'Failed to read data')

        const cols: ColumnInfo[] = (descRes.data ?? []).map(row => ({
          name: String(row['column_name'] ?? row['Field'] ?? ''),
          type: String(row['column_type'] ?? row['Type'] ?? 'unknown'),
        }))
        setColumns(cols)
        setBaseRows(dataRes.data ?? [])

        if (cntRes?.success && cntRes.data?.[0]) {
          setRowCount(Number(cntRes.data[0]['_cnt']))
        }
      } catch (e) {
        setError(String(e))
      } finally {
        setLoading(false)
      }
    }

    load()
  }, [file.path]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Debounced server-side search across ALL rows ──────────────────────────────
  useEffect(() => {
    const term = search.trim()

    // Clear search results immediately when input is empty
    if (!term || columns.length === 0) {
      setSearchRows(null)
      setSearchTotal(null)
      setSearchLoading(false)
      setSearchError(null)
      return
    }

    setSearchLoading(true)
    setSearchError(null)

    const timer = setTimeout(async () => {
      const expr = readExpr(file)
      const where = buildWhere(columns, term)

      try {
        const [dataRes, cntRes] = await Promise.all([
          invoke<{ success: boolean; data: Record<string, unknown>[]; error?: string }>(
            'run_duckdb_query',
            { sql: `SELECT * FROM ${expr}\nWHERE ${where}\nLIMIT 100` },
          ),
          invoke<{ success: boolean; data: Record<string, unknown>[] }>(
            'run_duckdb_query',
            { sql: `SELECT COUNT(*) AS _cnt FROM ${expr}\nWHERE ${where}` },
          ).catch(() => null),
        ])

        if (!dataRes.success) throw new Error(dataRes.error ?? 'Search failed')

        setSearchRows(dataRes.data ?? [])
        if (cntRes?.success && cntRes.data?.[0]) {
          setSearchTotal(Number(cntRes.data[0]['_cnt']))
        }
      } catch (e) {
        setSearchError(String(e))
        setSearchRows([])
      } finally {
        setSearchLoading(false)
      }
    }, 350) // 350 ms debounce

    return () => clearTimeout(timer)
  }, [search, columns, file.path]) // eslint-disable-line react-hooks/exhaustive-deps

  // Focus search on mount
  useEffect(() => {
    setTimeout(() => searchRef.current?.focus(), 80)
  }, [])

  // Escape to close
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  // Schema tab: client-side filter (just a small column list)
  const filteredColumns = useMemo(() => {
    if (!search.trim()) return columns
    const q = search.toLowerCase()
    return columns.filter(c =>
      c.name.toLowerCase().includes(q) || c.type.toLowerCase().includes(q),
    )
  }, [columns, search])

  const colTypeMap = useMemo(() => {
    const m: Record<string, string> = {}
    columns.forEach(c => { m[c.name] = c.type })
    return m
  }, [columns])

  // Footer status string for the data tab
  const dataFooter = () => {
    const term = search.trim()
    if (!term) {
      if (rowCount !== null && rowCount > 100)
        return `Showing first 100 of ${rowCount.toLocaleString()} total rows`
      return `${baseRows.length} rows`
    }
    if (searchLoading) return 'Searching…'
    if (searchError) return `Search error`
    if (searchTotal !== null) {
      if (searchTotal === 0) return `No matches found`
      if (searchTotal > 100)
        return `Showing 100 of ${searchTotal.toLocaleString()} matching rows`
      return `${searchTotal.toLocaleString()} matching rows`
    }
    return `${displayRows.length} rows`
  }

  // ── Render ───────────────────────────────────────────────────────────────────

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4 sm:p-8"
      style={{ background: 'rgba(0,0,0,0.55)', backdropFilter: 'blur(2px)' }}
      onMouseDown={e => { if (e.target === e.currentTarget) onClose() }}
    >
      <div
        className="w-full max-w-6xl bg-white dark:bg-gray-900 rounded-2xl shadow-2xl
          border border-gray-200 dark:border-gray-700 flex flex-col overflow-hidden"
        style={{ height: '82vh' }}
        onMouseDown={e => e.stopPropagation()}
      >

        {/* ── Header ── */}
        <div className="shrink-0 flex items-center gap-3 px-5 py-3 border-b border-gray-100 dark:border-gray-800">

          {/* Title */}
          <div className="flex items-center gap-2 min-w-0 mr-auto">
            <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24"
              fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
              className="text-blue-500 shrink-0">
              <ellipse cx="12" cy="5" rx="9" ry="3"/>
              <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
              <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
            </svg>
            <span className="text-sm font-semibold text-gray-900 dark:text-gray-100 truncate">
              {file.name}
            </span>
            {!loading && !error && rowCount !== null && (
              <span className="text-xs text-gray-400 dark:text-gray-500 shrink-0 font-normal">
                {rowCount.toLocaleString()} rows · {columns.length} cols
              </span>
            )}
          </div>

          {/* Tab switcher */}
          <div className="shrink-0 flex items-center gap-0.5 bg-gray-100 dark:bg-gray-800 rounded-lg p-0.5">
            {(['data', 'schema'] as Tab[]).map(t => (
              <button
                key={t}
                onClick={() => { setTab(t); setSearch('') }}
                className={`px-3 py-1 rounded-md text-xs font-medium capitalize transition-all ${
                  tab === t
                    ? 'bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 shadow-sm'
                    : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'
                }`}
              >
                {t}
              </button>
            ))}
          </div>

          {/* Search — with inline spinner when searching */}
          <div className="relative shrink-0">
            {/* Left icon: spinner while searching, magnifier otherwise */}
            {searchLoading && tab === 'data' ? (
              <svg className="absolute left-2.5 top-1/2 -translate-y-1/2 animate-spin text-indigo-400 pointer-events-none"
                xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"
                fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
              </svg>
            ) : (
              <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"
                fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
                className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400 dark:text-gray-500 pointer-events-none">
                <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
              </svg>
            )}
            <input
              ref={searchRef}
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder={tab === 'data' ? 'Search all rows…' : 'Filter columns…'}
              className={`pl-7 pr-7 py-1.5 text-xs w-56 rounded-lg border transition
                bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100
                placeholder-gray-400 dark:placeholder-gray-500
                focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-400 ${
                  searchError
                    ? 'border-red-300 dark:border-red-700'
                    : 'border-gray-200 dark:border-gray-700'
                }`}
            />
            {search && !searchLoading && (
              <button
                onClick={() => setSearch('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400
                  hover:text-gray-600 dark:hover:text-gray-300"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24"
                  fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
                </svg>
              </button>
            )}
          </div>

          {/* Close */}
          <button
            onClick={onClose}
            className="shrink-0 text-gray-400 hover:text-gray-700 dark:hover:text-gray-200
              p-1.5 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition"
            title="Close (Esc)"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24"
              fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
            </svg>
          </button>
        </div>

        {/* ── Body ── */}
        <div className="flex-1 min-h-0 overflow-hidden relative">

          {/* Initial load spinner */}
          {loading && (
            <div className="flex flex-col items-center justify-center h-full gap-3">
              <svg className="animate-spin text-indigo-500" xmlns="http://www.w3.org/2000/svg"
                width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
              </svg>
              <span className="text-sm text-gray-400 dark:text-gray-500">Loading {file.name}…</span>
            </div>
          )}

          {/* Load error */}
          {!loading && error && (
            <div className="flex flex-col items-center justify-center h-full gap-3 px-10">
              <svg className="text-red-400" xmlns="http://www.w3.org/2000/svg"
                width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="10"/>
                <line x1="12" y1="8" x2="12" y2="12"/>
                <line x1="12" y1="16" x2="12.01" y2="16"/>
              </svg>
              <p className="text-sm text-red-500 text-center font-mono leading-relaxed">{error}</p>
            </div>
          )}

          {/* ── Schema tab ── */}
          {!loading && !error && tab === 'schema' && (
            <div className="h-full overflow-y-auto">
              <table className="w-full text-xs">
                <thead className="sticky top-0 z-10 bg-gray-50 dark:bg-gray-800/90
                  border-b border-gray-200 dark:border-gray-700 backdrop-blur-sm">
                  <tr>
                    <th className="px-5 py-2.5 text-left font-semibold text-gray-500 dark:text-gray-400 w-10 tabular-nums">#</th>
                    <th className="px-4 py-2.5 text-left font-semibold text-gray-600 dark:text-gray-300">Column</th>
                    <th className="px-4 py-2.5 text-left font-semibold text-gray-600 dark:text-gray-300">Type</th>
                    <th className="px-4 py-2.5 text-left font-semibold text-gray-600 dark:text-gray-300 hidden sm:table-cell">Kind</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50 dark:divide-gray-800">
                  {filteredColumns.map((col, i) => (
                    <tr key={col.name}
                      className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
                      <td className="px-5 py-2.5 text-gray-300 dark:text-gray-700 tabular-nums">{i + 1}</td>
                      <td className="px-4 py-2.5 font-mono text-gray-800 dark:text-gray-200 font-medium">{col.name}</td>
                      <td className="px-4 py-2.5">
                        <span className={`inline-flex items-center px-2 py-0.5 rounded border text-[10px] font-bold ${typeBadgeColor(col.type)}`}>
                          {col.type}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 text-gray-400 dark:text-gray-600 hidden sm:table-cell">
                        {isNumericType(col.type) ? 'numeric'
                          : /BOOL/i.test(col.type) ? 'boolean'
                          : /TIMESTAMP|DATE|TIME/i.test(col.type) ? 'temporal'
                          : 'text'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {filteredColumns.length === 0 && (
                <div className="flex items-center justify-center py-20 text-sm text-gray-400 dark:text-gray-500">
                  No columns match &ldquo;{search}&rdquo;
                </div>
              )}
            </div>
          )}

          {/* ── Data tab ── */}
          {!loading && !error && tab === 'data' && (
            <div className="h-full overflow-auto">

              {/* Search error banner */}
              {searchError && (
                <div className="px-5 py-2 bg-red-50 dark:bg-red-900/20 border-b border-red-100 dark:border-red-800
                  text-xs text-red-600 dark:text-red-400 font-mono">
                  Search error: {searchError}
                </div>
              )}

              <table className="text-xs border-collapse" style={{ minWidth: '100%' }}>
                <thead className="sticky top-0 z-10">
                  <tr className="bg-gray-50 dark:bg-gray-800/90 border-b border-gray-200 dark:border-gray-700 backdrop-blur-sm">
                    <th className="sticky left-0 z-20 bg-gray-50 dark:bg-gray-800 px-4 py-2.5
                      text-left font-semibold text-gray-400 dark:text-gray-600 w-10 tabular-nums
                      border-r border-gray-100 dark:border-gray-700">
                      #
                    </th>
                    {columns.map(col => (
                      <th key={col.name} className="px-4 py-2 text-left whitespace-nowrap">
                        <div className="flex flex-col gap-0.5">
                          <span className="font-semibold text-gray-700 dark:text-gray-300 font-mono text-[11px]">
                            {col.name}
                          </span>
                          <span className={`self-start inline-flex items-center px-1.5 py-px rounded border
                            text-[9px] font-bold leading-tight ${typeBadgeColor(col.type)}`}>
                            {col.type.length > 14 ? col.type.slice(0, 12) + '…' : col.type}
                          </span>
                        </div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {/* While searching, show a subtle overlay row */}
                  {searchLoading && (
                    <tr>
                      <td colSpan={columns.length + 1} className="py-0">
                        <div className="w-full h-0.5 bg-indigo-100 dark:bg-indigo-900/40 overflow-hidden">
                          <div className="h-full bg-indigo-400 dark:bg-indigo-500 animate-[shimmer_1.2s_ease-in-out_infinite]"
                            style={{ width: '40%', animation: 'pulse 1s ease-in-out infinite' }} />
                        </div>
                      </td>
                    </tr>
                  )}
                  {!searchLoading && displayRows.length === 0 && search.trim() ? (
                    <tr>
                      <td colSpan={columns.length + 1}
                        className="text-center py-20 text-sm text-gray-400 dark:text-gray-500">
                        No rows match &ldquo;{search}&rdquo; in {rowCount?.toLocaleString() ?? 'all'} rows
                      </td>
                    </tr>
                  ) : (
                    displayRows.map((row, i) => (
                      <tr key={i} className={`group transition-colors
                        hover:bg-indigo-50/50 dark:hover:bg-indigo-900/10 ${
                          i % 2 === 0
                            ? 'bg-white dark:bg-gray-900'
                            : 'bg-gray-50/50 dark:bg-gray-800/25'
                        }`}>
                        <td className="sticky left-0 bg-inherit px-4 py-2 text-right
                          text-gray-300 dark:text-gray-700 tabular-nums
                          border-r border-gray-100 dark:border-gray-800">
                          {i + 1}
                        </td>
                        {columns.map(col => {
                          const v = row[col.name]
                          const isNull = v === null || v === undefined
                          const isNum = isNumericType(colTypeMap[col.name] ?? '')
                          const cell = formatCell(v)
                          return (
                            <td key={col.name}
                              className={`px-4 py-2 whitespace-nowrap border-b border-gray-50
                                dark:border-gray-800/60 max-w-xs ${
                                  isNull
                                    ? 'text-gray-300 dark:text-gray-700 italic'
                                    : isNum
                                      ? 'text-right font-mono text-blue-700 dark:text-blue-400'
                                      : 'text-gray-700 dark:text-gray-300'
                                }`}>
                              {isNull
                                ? <span className="text-[10px]">null</span>
                                : cell.length > 64
                                  ? <span title={cell} className="cursor-help">{cell.slice(0, 62)}…</span>
                                  : cell
                              }
                            </td>
                          )
                        })}
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* ── Footer ── */}
        {!loading && !error && (
          <div className="shrink-0 flex items-center justify-between px-5 py-2
            border-t border-gray-100 dark:border-gray-800 bg-gray-50/50 dark:bg-gray-900/50">
            <span className={`text-[11px] ${
              searchError
                ? 'text-red-500 dark:text-red-400'
                : 'text-gray-400 dark:text-gray-600'
            }`}>
              {tab === 'data'
                ? dataFooter()
                : search
                  ? `${filteredColumns.length} of ${columns.length} columns`
                  : `${columns.length} columns`
              }
            </span>
            <span className="text-[11px] text-gray-300 dark:text-gray-700 truncate max-w-sm font-mono"
              title={file.path}>
              {file.path}
            </span>
          </div>
        )}
      </div>
    </div>
  )
}
