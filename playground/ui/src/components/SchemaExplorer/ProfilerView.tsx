import React, { useMemo } from 'react'
import type { FileProfile, ColumnProfile, TopValue } from '../../hooks/useProfiler'

// ── Helpers ───────────────────────────────────────────────────────────────────

function isNumericType(t: string) {
  return /INT|BIGINT|HUGEINT|FLOAT|DOUBLE|DECIMAL|REAL|NUMERIC|TINYINT|SMALLINT/i.test(t)
}

function fmt(n: number | null | undefined, decimals = 2): string {
  if (n === null || n === undefined) return '—'
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (Math.abs(n) >= 1_000)     return `${(n / 1_000).toFixed(1)}K`
  return n.toFixed(decimals).replace(/\.?0+$/, '')
}

function fmtStr(s: string | null): string {
  if (!s) return '—'
  if (s.length > 16) return s.slice(0, 14) + '…'
  return s
}

function typeBadge(type: string): string {
  const t = type.toUpperCase()
  if (/INT|BIGINT|HUGEINT|TINYINT|SMALLINT/.test(t)) return 'bg-blue-50 text-blue-600 border-blue-100'
  if (/FLOAT|DOUBLE|DECIMAL|REAL|NUMERIC/.test(t))    return 'bg-violet-50 text-violet-600 border-violet-100'
  if (/VARCHAR|TEXT|STRING|CHAR/.test(t))              return 'bg-gray-100 text-gray-500 border-gray-200'
  if (/TIMESTAMP|DATE|TIME|INTERVAL/.test(t))          return 'bg-orange-50 text-orange-600 border-orange-100'
  if (/BOOL/.test(t))                                  return 'bg-green-50 text-green-600 border-green-100'
  if (/LIST|STRUCT|MAP|JSON/.test(t))                  return 'bg-pink-50 text-pink-600 border-pink-100'
  return 'bg-gray-100 text-gray-400 border-gray-200'
}

function shortType(type: string): string {
  return type
    .replace('VARCHAR', 'STR').replace('DOUBLE', 'DBL').replace('BIGINT', 'INT64')
    .replace('INTEGER', 'INT32').replace('HUGEINT', 'INT128').replace('BOOLEAN', 'BOOL')
    .replace('FLOAT', 'FLT').replace('TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ')
    .replace('TIMESTAMP', 'TIME').replace('SMALLINT', 'INT16').replace('TINYINT', 'INT8')
}

// ── Box-and-whisker plot ──────────────────────────────────────────────────────

function BoxPlot({ col }: { col: ColumnProfile }) {
  const min  = parseFloat(col.min  ?? '')
  const q25  = parseFloat(col.q25  ?? '')
  const q50  = parseFloat(col.q50  ?? '')
  const q75  = parseFloat(col.q75  ?? '')
  const max  = parseFloat(col.max  ?? '')

  if ([min, q25, q50, q75, max].some(isNaN)) return null

  const range = max - min
  const W = 100
  const PAD = 6
  const inner = W - PAD * 2
  const scale = (v: number) => range === 0 ? W / 2 : PAD + ((v - min) / range) * inner

  const x1  = scale(min)
  const x25 = scale(q25)
  const x50 = scale(q50)
  const x75 = scale(q75)
  const x2  = scale(max)
  const cy  = 12

  return (
    <div className="mt-2">
      <svg width="100%" height="24" viewBox={`0 0 ${W} 24`} className="overflow-visible" preserveAspectRatio="none">
        {/* Whisker full line */}
        <line x1={x1} y1={cy} x2={x2} y2={cy} stroke="#d1d5db" strokeWidth="1.5" />
        {/* Min / Max caps */}
        <line x1={x1} y1={cy - 4} x2={x1} y2={cy + 4} stroke="#9ca3af" strokeWidth="1.5" strokeLinecap="round" />
        <line x1={x2} y1={cy - 4} x2={x2} y2={cy + 4} stroke="#9ca3af" strokeWidth="1.5" strokeLinecap="round" />
        {/* IQR box */}
        <rect x={x25} y={cy - 6} width={Math.max(x75 - x25, 1)} height={12}
          fill="#e0e7ff" stroke="#818cf8" strokeWidth="1" rx="2" />
        {/* Median line */}
        <line x1={x50} y1={cy - 6} x2={x50} y2={cy + 6}
          stroke="#4f46e5" strokeWidth="2.5" strokeLinecap="round" />
      </svg>
      {/* Labels */}
      <div className="flex justify-between text-[9px] text-gray-400 -mt-0.5 px-0.5">
        <span title={`min: ${col.min}`}>{fmtStr(col.min)}</span>
        <span title={`Q1: ${col.q25}`}>{fmtStr(col.q25)}</span>
        <span title={`median: ${col.q50}`} className="text-indigo-500 font-medium">{fmtStr(col.q50)}</span>
        <span title={`Q3: ${col.q75}`}>{fmtStr(col.q75)}</span>
        <span title={`max: ${col.max}`}>{fmtStr(col.max)}</span>
      </div>
    </div>
  )
}

// ── Top values bar chart ──────────────────────────────────────────────────────

function TopValuesBars({ values }: { values: TopValue[] }) {
  const maxPct = Math.max(...values.map(v => v.pct), 1)
  return (
    <div className="mt-2 space-y-1">
      {values.map(({ val, cnt, pct }) => (
        <div key={val} className="flex items-center gap-1.5">
          <span className="text-[10px] font-mono text-gray-600 dark:text-gray-400 w-20 truncate shrink-0" title={val}>
            {val === '' ? <em className="text-gray-400">empty</em> : val}
          </span>
          <div className="flex-1 h-1.5 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
            <div className="h-full bg-indigo-400 rounded-full transition-all"
              style={{ width: `${(pct / maxPct) * 100}%` }} />
          </div>
          <span className="text-[10px] text-gray-400 w-7 text-right shrink-0">
            {pct < 1 ? '<1' : pct.toFixed(0)}%
          </span>
          <span className="text-[10px] text-gray-300 w-12 text-right shrink-0 hidden xl:block">
            {cnt.toLocaleString()}
          </span>
        </div>
      ))}
    </div>
  )
}

// ── Per-column card ───────────────────────────────────────────────────────────

function ColumnCard({ col }: { col: ColumnProfile }) {
  const isNum    = isNumericType(col.column_type)
  const nullPct  = col.null_percentage ?? 0
  const nullColor = nullPct > 50 ? 'bg-red-400' : nullPct > 10 ? 'bg-yellow-400' : 'bg-emerald-400'
  const uniqueK   = col.approx_unique >= 1000
    ? `~${(col.approx_unique / 1000).toFixed(0)}K`
    : col.approx_unique.toLocaleString()

  return (
    <div className="border border-gray-100 dark:border-gray-700 rounded-xl p-3 bg-white dark:bg-gray-900 hover:border-gray-200 dark:hover:border-gray-600 hover:shadow-sm transition-all">
      {/* Header */}
      <div className="flex items-start justify-between gap-1 mb-2">
        <span className="text-xs font-semibold text-gray-800 dark:text-gray-200 font-mono truncate" title={col.column_name}>
          {col.column_name}
        </span>
        <span className={`text-[10px] font-bold px-1.5 py-0 rounded border shrink-0 ${typeBadge(col.column_type)}`}>
          {shortType(col.column_type)}
        </span>
      </div>

      {/* Null bar */}
      <div className="flex items-center gap-2 mb-2">
        <div className="flex-1 h-1.5 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full ${nullColor}`}
            style={{ width: nullPct > 0 ? `${Math.max(nullPct, 2)}%` : '0%' }}
          />
        </div>
        <span className="text-[10px] text-gray-400 shrink-0 w-14 text-right">
          {nullPct === 0 ? 'no nulls' : `${nullPct.toFixed(1)}% null`}
        </span>
      </div>

      {/* Stats grid */}
      {isNum ? (
        <div className="grid grid-cols-3 gap-x-2 gap-y-0.5 text-[10px] mb-1">
          {[
            { label: 'min',    val: fmtStr(col.min) },
            { label: 'avg',    val: col.avg != null ? fmt(col.avg) : '—' },
            { label: 'max',    val: fmtStr(col.max) },
            { label: 'std',    val: col.std != null ? fmt(col.std) : '—' },
            { label: 'median', val: fmtStr(col.q50) },
            { label: 'unique', val: uniqueK },
          ].map(({ label, val }) => (
            <div key={label} className="flex flex-col">
              <span className="text-gray-400 uppercase tracking-wide" style={{ fontSize: 9 }}>{label}</span>
              <span className="text-gray-700 dark:text-gray-300 font-mono font-medium truncate">{val}</span>
            </div>
          ))}
        </div>
      ) : (
        <div className="flex items-center gap-3 text-[10px] mb-1">
          <div>
            <span className="text-gray-400 uppercase tracking-wide" style={{ fontSize: 9 }}>unique</span>
            <div className="text-gray-700 dark:text-gray-300 font-mono font-medium">{uniqueK}</div>
          </div>
          <div>
            <span className="text-gray-400 uppercase tracking-wide" style={{ fontSize: 9 }}>count</span>
            <div className="text-gray-700 dark:text-gray-300 font-mono font-medium">{col.count.toLocaleString()}</div>
          </div>
        </div>
      )}

      {/* Distribution */}
      {isNum && <BoxPlot col={col} />}
      {!isNum && col.top_values && col.top_values.length > 0 && (
        <>
          <div className="text-[9px] uppercase tracking-wide text-gray-400 mt-2 mb-1">Top values</div>
          <TopValuesBars values={col.top_values} />
        </>
      )}
    </div>
  )
}

// ── Summary bar ───────────────────────────────────────────────────────────────

function SummaryBadge({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="flex flex-col items-center px-4 py-2 bg-gray-50 dark:bg-gray-800 rounded-xl border border-gray-100 dark:border-gray-700">
      <span className="text-base font-bold text-gray-800 dark:text-gray-200 tabular-nums">{value}</span>
      {sub && <span className="text-[10px] text-gray-400 dark:text-gray-500">{sub}</span>}
      <span className="text-[10px] text-gray-500 dark:text-gray-400 mt-0.5">{label}</span>
    </div>
  )
}

// ── Main view ─────────────────────────────────────────────────────────────────

interface Props {
  profile: FileProfile
  onBack: () => void
  onRefresh: () => void
}

export function ProfilerView({ profile, onBack, onRefresh }: Props) {
  const nullCols = useMemo(
    () => profile.columns.filter(c => c.null_percentage > 0).length,
    [profile.columns],
  )
  const numCols = useMemo(
    () => profile.columns.filter(c => isNumericType(c.column_type)).length,
    [profile.columns],
  )

  if (profile.loading) {
    return (
      <div className="flex flex-col flex-1 min-h-0">
        <ProfileHeader name={profile.name} onBack={onBack} onRefresh={onRefresh} />
        <div className="flex-1 flex flex-col items-center justify-center gap-3 text-gray-400 dark:text-gray-500">
          <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24"
            fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
            className="animate-spin text-indigo-500">
            <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
          </svg>
          <div className="text-center">
            <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Profiling {profile.name}…</p>
            <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">Running SUMMARIZE query</p>
          </div>
        </div>
      </div>
    )
  }

  if (profile.error) {
    return (
      <div className="flex flex-col flex-1 min-h-0">
        <ProfileHeader name={profile.name} onBack={onBack} onRefresh={onRefresh} />
        <div className="flex-1 flex flex-col items-center justify-center gap-3 px-6">
          <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24"
            fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
            className="text-red-400">
            <circle cx="12" cy="12" r="10"/>
            <line x1="12" y1="8" x2="12" y2="12"/>
            <line x1="12" y1="16" x2="12.01" y2="16"/>
          </svg>
          <p className="text-xs text-red-500 text-center font-mono">{profile.error}</p>
          <button onClick={onRefresh}
            className="text-xs text-indigo-600 hover:text-indigo-800 underline">
            Try again
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col flex-1 min-h-0">
      <ProfileHeader name={profile.name} onBack={onBack} onRefresh={onRefresh}
        duration_ms={profile.duration_ms} />

      {/* Summary badges */}
      <div className="shrink-0 flex items-center gap-2 px-3 py-2.5 border-b border-gray-100 dark:border-gray-800 overflow-x-auto">
        <SummaryBadge label="rows"    value={profile.total_rows >= 1_000_000
          ? `${(profile.total_rows / 1_000_000).toFixed(2)}M`
          : profile.total_rows.toLocaleString()} />
        <SummaryBadge label="columns" value={String(profile.columns.length)}
          sub={`${numCols} numeric`} />
        <SummaryBadge
          label="with nulls"
          value={String(nullCols)}
          sub={nullCols === 0 ? 'clean ✓' : `${profile.columns.length - nullCols} clean`}
        />
        {profile.duration_ms !== undefined && (
          <SummaryBadge label="profiled in"
            value={profile.duration_ms >= 1000
              ? `${(profile.duration_ms / 1000).toFixed(1)}s`
              : `${profile.duration_ms}ms`} />
        )}
      </div>

      {/* Column cards grid */}
      <div className="flex-1 min-h-0 overflow-y-auto px-3 py-3">
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-2.5">
          {profile.columns.map(col => (
            <ColumnCard key={col.column_name} col={col} />
          ))}
        </div>
      </div>
    </div>
  )
}

// ── Shared header ─────────────────────────────────────────────────────────────

function ProfileHeader({
  name, onBack, onRefresh, duration_ms,
}: {
  name: string; onBack: () => void; onRefresh: () => void; duration_ms?: number
}) {
  return (
    <div className="shrink-0 flex items-center gap-2 px-3 py-2 border-b border-gray-100 dark:border-gray-800 bg-white dark:bg-gray-900">
      <button onClick={onBack}
        className="flex items-center gap-1 text-xs text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 transition shrink-0">
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24"
          fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="15 18 9 12 15 6"/>
        </svg>
        Schema
      </button>
      <span className="text-gray-300 dark:text-gray-600">/</span>
      <span className="text-xs font-semibold text-gray-700 dark:text-gray-300 truncate flex-1">{name}</span>
      {duration_ms !== undefined && (
        <span className="text-[10px] text-gray-400 shrink-0">
          {duration_ms >= 1000 ? `${(duration_ms / 1000).toFixed(1)}s` : `${duration_ms}ms`}
        </span>
      )}
      <button onClick={onRefresh} title="Re-profile"
        className="text-gray-400 hover:text-indigo-600 p-1 rounded hover:bg-gray-100 dark:hover:bg-gray-800 transition shrink-0">
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24"
          fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="23 4 23 10 17 10"/>
          <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
        </svg>
      </button>
    </div>
  )
}
