import React, { useEffect, useRef, useState } from 'react'

// ── Stats computation ─────────────────────────────────────────────────────────

const NUM_BINS = 10

export interface NumericStats {
  kind: 'numeric'
  min: number; max: number; mean: number; median: number
  nullCount: number; nullPct: number
  uniqueCount: number
  bins: number[]      // histogram counts, length = NUM_BINS
  binMin: number      // left edge of first bin
  binWidth: number    // width of each bin
}

export interface CategoricalStats {
  kind: 'categorical'
  nullCount: number; nullPct: number
  uniqueCount: number
  topValues: { val: string; count: number; pct: number }[]
}

export type ColumnStats = NumericStats | CategoricalStats

export function computeStats(data: Record<string, unknown>[], col: string): ColumnStats {
  const total = data.length
  let nullCount = 0
  const nums: number[] = []
  const strCounts = new Map<string, number>()
  let isNumeric = true

  for (const row of data) {
    const v = row[col]
    if (v === null || v === undefined) {
      nullCount++
      continue
    }
    if (typeof v === 'number' && isFinite(v)) {
      nums.push(v)
      strCounts.set(String(v), (strCounts.get(String(v)) ?? 0) + 1)
    } else {
      isNumeric = false
      const s = String(v)
      strCounts.set(s, (strCounts.get(s) ?? 0) + 1)
    }
  }

  const nullPct = total > 0 ? (nullCount / total) * 100 : 0

  if (isNumeric && nums.length > 0) {
    nums.sort((a, b) => a - b)
    const min    = nums[0]
    const max    = nums[nums.length - 1]
    const mean   = nums.reduce((s, n) => s + n, 0) / nums.length
    const mid    = Math.floor(nums.length / 2)
    const median = nums.length % 2 === 0 ? (nums[mid - 1] + nums[mid]) / 2 : nums[mid]

    // Histogram bins
    const bins = new Array<number>(NUM_BINS).fill(0)
    const range = max - min
    if (range === 0) {
      bins[Math.floor(NUM_BINS / 2)] = nums.length
    } else {
      const binWidth = range / NUM_BINS
      for (const n of nums) {
        const idx = Math.min(Math.floor((n - min) / binWidth), NUM_BINS - 1)
        bins[idx]++
      }
    }

    return {
      kind: 'numeric', min, max, mean, median, nullCount, nullPct,
      uniqueCount: strCounts.size,
      bins, binMin: min, binWidth: range === 0 ? 1 : range / NUM_BINS,
    }
  }

  // Categorical
  const topValues = [...strCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([val, count]) => ({ val, count, pct: total > 0 ? (count / total) * 100 : 0 }))

  return {
    kind: 'categorical', nullCount, nullPct,
    uniqueCount: strCounts.size,
    topValues,
  }
}

// ── Formatting helpers ────────────────────────────────────────────────────────

function fmtNum(n: number): string {
  if (!isFinite(n)) return '—'
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (Math.abs(n) >= 1_000)     return `${(n / 1_000).toFixed(2)}K`
  // Show up to 4 sig figs, strip trailing zeros
  return parseFloat(n.toPrecision(4)).toString()
}

function fmtPct(p: number): string {
  return p < 0.1 ? '<0.1%' : `${p.toFixed(1)}%`
}

// ── Sparkline bar chart ───────────────────────────────────────────────────────

function Sparkline({ bins }: { bins: number[] }) {
  const W = 168  // svg viewBox width
  const H = 36
  const maxBin = Math.max(...bins, 1)
  const barW = (W / bins.length) - 2

  return (
    <svg
      width="100%" height={H} viewBox={`0 0 ${W} ${H}`}
      preserveAspectRatio="none"
      className="block"
    >
      {bins.map((count, i) => {
        const barH = Math.max((count / maxBin) * (H - 2), count > 0 ? 2 : 0)
        const x = i * (W / bins.length) + 1
        const y = H - barH
        return (
          <rect
            key={i} x={x} y={y} width={barW} height={barH}
            rx="1.5"
            fill={count === maxBin ? '#4f46e5' : '#a5b4fc'}
            opacity={count === 0 ? 0.15 : 1}
          />
        )
      })}
    </svg>
  )
}

// ── Top-values mini bar chart ─────────────────────────────────────────────────

function TopValueBars({ values }: { values: CategoricalStats['topValues'] }) {
  const maxPct = Math.max(...values.map(v => v.pct), 1)
  return (
    <div className="space-y-1 mt-1">
      {values.map(({ val, count, pct }) => (
        <div key={val} className="flex items-center gap-1.5">
          <span
            className="text-[10.5px] font-mono text-gray-600 truncate"
            style={{ width: 72, flexShrink: 0 }}
            title={val}
          >
            {val === '' ? <em className="text-gray-400">empty</em> : val}
          </span>
          <div className="flex-1 h-1.5 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full bg-indigo-400"
              style={{ width: `${(pct / maxPct) * 100}%` }}
            />
          </div>
          <span className="text-[10px] text-gray-400 tabular-nums" style={{ width: 28, flexShrink: 0, textAlign: 'right' }}>
            {pct < 1 ? '<1%' : `${Math.round(pct)}%`}
          </span>
          <span className="text-[10px] text-gray-300 tabular-nums hidden xl:block" style={{ width: 32, flexShrink: 0, textAlign: 'right' }}>
            {count.toLocaleString()}
          </span>
        </div>
      ))}
    </div>
  )
}

// ── Stat tile ─────────────────────────────────────────────────────────────────

function StatTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col items-center min-w-0 overflow-hidden">
      <span className="text-[9px] uppercase tracking-wider text-gray-400 mb-0.5 shrink-0">{label}</span>
      <span
        className="text-[11px] font-mono font-semibold text-gray-800 dark:text-gray-200 tabular-nums w-full text-center truncate"
        title={value}
      >
        {value}
      </span>
    </div>
  )
}

// ── Popover content ───────────────────────────────────────────────────────────

function PopoverContent({ col, stats }: { col: string; stats: ColumnStats }) {
  const typeBadge = stats.kind === 'numeric'
    ? 'bg-blue-50 text-blue-600 border-blue-100'
    : 'bg-gray-100 text-gray-500 border-gray-200'
  const typeLabel = stats.kind === 'numeric' ? 'NUM' : 'STR'

  return (
    <div className="w-full min-w-0">
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold text-gray-800 dark:text-gray-200 font-mono truncate">{col}</span>
        <span className={`text-[10px] font-bold px-1.5 py-0 rounded border ml-2 shrink-0 ${typeBadge}`}>
          {typeLabel}
        </span>
      </div>

      {stats.kind === 'numeric' ? (
        <>
          {/* Sparkline */}
          <div className="rounded-lg bg-gray-50 dark:bg-gray-800 border border-gray-100 dark:border-gray-700 overflow-hidden mb-2 px-1 pt-1.5 pb-0.5">
            <Sparkline bins={stats.bins} />
            <div className="flex justify-between text-[9px] text-gray-400 px-0.5 pb-1">
              <span>{fmtNum(stats.min)}</span>
              <span className="text-indigo-500">{fmtNum(stats.median)}</span>
              <span>{fmtNum(stats.max)}</span>
            </div>
          </div>

          {/* Stat tiles */}
          <div className="grid grid-cols-3 gap-1 mb-2">
            <StatTile label="min"    value={fmtNum(stats.min)} />
            <StatTile label="mean"   value={fmtNum(stats.mean)} />
            <StatTile label="max"    value={fmtNum(stats.max)} />
          </div>
        </>
      ) : (
        <>
          {/* Top values */}
          {stats.topValues.length > 0 && (
            <div className="mb-2">
              <div className="text-[9px] uppercase tracking-wider text-gray-400 mb-1">Top values</div>
              <TopValueBars values={stats.topValues} />
            </div>
          )}
        </>
      )}

      {/* Footer */}
      <div className="flex items-center justify-between pt-1.5 border-t border-gray-100 dark:border-gray-700 text-[10px] text-gray-400 dark:text-gray-500">
        <span>
          {stats.nullCount === 0
            ? <span className="text-emerald-500 font-medium">no nulls</span>
            : <>{stats.nullCount.toLocaleString()} null · <span className="text-red-400">{fmtPct(stats.nullPct)}</span></>
          }
        </span>
        <span>{stats.uniqueCount.toLocaleString()} unique</span>
      </div>
    </div>
  )
}

// ── Main exported popover ─────────────────────────────────────────────────────

interface Props {
  col: string
  stats: ColumnStats
  anchorRect: DOMRect
}

export function ColumnStatsPopover({ col, stats, anchorRect }: Props) {
  const popoverRef = useRef<HTMLDivElement>(null)
  const [pos, setPos] = useState({ top: 0, left: 0 })

  useEffect(() => {
    const popW = 252
    const popH = 220  // estimated height

    let left = anchorRect.left
    let top  = anchorRect.bottom + 6

    // Clamp right edge
    if (left + popW > window.innerWidth - 8) {
      left = Math.max(8, window.innerWidth - popW - 8)
    }
    // Flip above if too close to bottom
    if (top + popH > window.innerHeight - 8) {
      top = anchorRect.top - popH - 6
    }

    setPos({ top, left })
  }, [anchorRect])

  return (
    <div
      ref={popoverRef}
      className="fixed z-50 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 shadow-xl p-3 pointer-events-none select-none overflow-hidden"
      style={{ top: pos.top, left: pos.left, width: 252 }}
    >
      <PopoverContent col={col} stats={stats} />
    </div>
  )
}
