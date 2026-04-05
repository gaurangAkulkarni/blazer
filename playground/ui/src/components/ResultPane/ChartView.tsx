import React, { useState, useMemo, useEffect } from 'react'
import {
  BarChart, Bar,
  LineChart, Line,
  AreaChart, Area,
  ScatterChart, Scatter,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'
import type { QueryResult } from '../../lib/types'

type ChartType = 'bar' | 'line' | 'area' | 'scatter' | 'pie'

const PALETTE = [
  '#6366f1', '#f59e0b', '#10b981', '#ef4444',
  '#8b5cf6', '#06b6d4', '#f97316', '#84cc16',
  '#ec4899', '#14b8a6',
]

const MAX_POINTS = 1000

const CHART_TYPES: { type: ChartType; label: string; icon: string }[] = [
  { type: 'bar',     label: 'Bar',     icon: 'M3 3v18h18M7 16v-4M11 16V8M15 16v-6M19 16v-9' },
  { type: 'line',    label: 'Line',    icon: 'M3 3v18h18M7 14l4-6 4 4 4-5' },
  { type: 'area',    label: 'Area',    icon: 'M3 3v18h18M3 17l4-4 4 2 4-6 4-2v8H3z' },
  { type: 'scatter', label: 'Scatter', icon: 'M3 3v18h18M7 17l.01 0M12 12l.01 0M17 8l.01 0M9 14l.01 0M15 6l.01 0' },
  { type: 'pie',     label: 'Pie',     icon: 'M21.21 15.89A10 10 0 1 1 8 2.83M22 12A10 10 0 0 0 12 2v10z' },
]

interface Props {
  result: QueryResult
}

export function ChartView({ result }: Props) {
  // Classify columns
  const { numericCols, categoricalCols } = useMemo(() => {
    const numeric: string[] = []
    const categorical: string[] = []
    for (const col of result.columns) {
      const sample = result.data.find(r => r[col] !== null && r[col] !== undefined)
      if (sample && typeof sample[col] === 'number') numeric.push(col)
      else categorical.push(col)
    }
    return { numericCols: numeric, categoricalCols: categorical }
  }, [result])

  const [chartType, setChartType] = useState<ChartType>('bar')
  const [xCol, setXCol] = useState<string>(() => categoricalCols[0] ?? result.columns[0] ?? '')
  const [yCols, setYCols] = useState<string[]>(() => numericCols.slice(0, 3))
  const [isDark, setIsDark] = useState(() => document.documentElement.classList.contains('dark'))

  useEffect(() => {
    const obs = new MutationObserver(() => {
      setIsDark(document.documentElement.classList.contains('dark'))
    })
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
    return () => obs.disconnect()
  }, [])

  const chartData = useMemo(
    () => result.data.slice(0, MAX_POINTS),
    [result.data],
  )

  const toggleYCol = (col: string) => {
    setYCols(prev =>
      prev.includes(col) ? prev.filter(c => c !== col) : [...prev, col],
    )
  }

  const noNumeric = numericCols.length === 0
  const noY = yCols.length === 0

  // ── Renders ─────────────────────────────────────────────────────────────────

  const commonAxisProps = {
    tick: { fontSize: 11, fill: isDark ? '#6b7280' : '#9ca3af' },
    axisLine: { stroke: isDark ? '#374151' : '#e5e7eb' },
    tickLine: false,
  } as const

  const tooltipStyle = {
    fontSize: 11,
    borderRadius: 8,
    border: `1px solid ${isDark ? '#374151' : '#e5e7eb'}`,
    backgroundColor: isDark ? '#111827' : '#ffffff',
    color: isDark ? '#f3f4f6' : '#111827',
    boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
  }

  function renderChart() {
    if (noNumeric) {
      return (
        <div className="flex items-center justify-center h-full text-sm text-gray-400 dark:text-gray-500">
          No numeric columns to chart
        </div>
      )
    }
    if (noY) {
      return (
        <div className="flex items-center justify-center h-full text-sm text-gray-400 dark:text-gray-500">
          Select at least one Y column
        </div>
      )
    }

    if (chartType === 'pie') {
      const pieCol = yCols[0]
      const pieData = chartData.slice(0, 50).map(row => ({
        name: String(row[xCol] ?? ''),
        value: Number(row[pieCol] ?? 0),
      }))
      return (
        <ResponsiveContainer width="100%" height={300}>
          <PieChart>
            <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%"
              outerRadius={110} label={({ name, percent }: { name?: string; percent?: number }) => `${name ?? ''} ${((percent ?? 0) * 100).toFixed(0)}%`}
              labelLine={false}>
              {pieData.map((_, i) => (
                <Cell key={i} fill={PALETTE[i % PALETTE.length]} />
              ))}
            </Pie>
            <Tooltip contentStyle={tooltipStyle} />
            <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
          </PieChart>
        </ResponsiveContainer>
      )
    }

    if (chartType === 'scatter') {
      const [yCol] = yCols
      return (
        <ResponsiveContainer width="100%" height={300}>
          <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={isDark ? '#1f2937' : '#f0f0f0'} />
            <XAxis dataKey={xCol} type="number" name={xCol} {...commonAxisProps} />
            <YAxis dataKey={yCol} type="number" name={yCol} {...commonAxisProps} width={55} />
            <Tooltip contentStyle={tooltipStyle} cursor={{ strokeDasharray: '3 3' }} />
            <Scatter data={chartData} fill={PALETTE[0]} opacity={0.7} />
          </ScatterChart>
        </ResponsiveContainer>
      )
    }

    const ChartComponent = chartType === 'line' ? LineChart : chartType === 'area' ? AreaChart : BarChart
    const DataComponent = chartType === 'line' ? Line : chartType === 'area' ? Area : Bar

    return (
      <ResponsiveContainer width="100%" height={300}>
        <ChartComponent data={chartData} margin={{ top: 10, right: 20, bottom: 30, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={isDark ? '#1f2937' : '#f3f4f6'} vertical={false} />
          <XAxis
            dataKey={xCol}
            {...commonAxisProps}
            angle={-35}
            textAnchor="end"
            interval="preserveStartEnd"
          />
          <YAxis {...commonAxisProps} width={55} tickFormatter={(v: number) =>
            Math.abs(v) >= 1_000_000 ? `${(v/1_000_000).toFixed(1)}M`
            : Math.abs(v) >= 1_000 ? `${(v/1_000).toFixed(1)}K`
            : String(v)
          } />
          <Tooltip contentStyle={tooltipStyle} />
          {yCols.length > 1 && <Legend iconSize={10} wrapperStyle={{ fontSize: 11, paddingTop: 8 }} />}
          {yCols.map((col, i) =>
            chartType === 'area' ? (
              <Area key={col} type="monotone" dataKey={col}
                stroke={PALETTE[i % PALETTE.length]} fill={PALETTE[i % PALETTE.length]}
                fillOpacity={0.15} strokeWidth={2} dot={false} />
            ) : chartType === 'line' ? (
              <Line key={col} type="monotone" dataKey={col}
                stroke={PALETTE[i % PALETTE.length]} strokeWidth={2} dot={false} />
            ) : (
              <Bar key={col} dataKey={col} fill={PALETTE[i % PALETTE.length]}
                radius={[3, 3, 0, 0]} maxBarSize={40} />
            )
          )}
        </ChartComponent>
      </ResponsiveContainer>
    )
  }

  // ── UI ───────────────────────────────────────────────────────────────────────
  return (
    <div className="px-3 pt-2 pb-3 border-t border-gray-100 dark:border-gray-800 bg-white dark:bg-gray-900">
      {/* Controls row */}
      <div className="flex flex-wrap items-start gap-4 mb-3">

        {/* Chart type pills */}
        <div className="flex items-center gap-0.5 bg-gray-100 dark:bg-gray-800 rounded-lg p-0.5">
          {CHART_TYPES.map(({ type, label, icon }) => (
            <button
              key={type}
              onClick={() => setChartType(type)}
              title={label}
              className={`flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-medium transition-all ${
                chartType === type
                  ? 'bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 shadow-sm'
                  : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'
              }`}
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none"
                stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d={icon} />
              </svg>
              {label}
            </button>
          ))}
        </div>

        {/* X axis */}
        {chartType !== 'pie' && (
          <div className="flex items-center gap-1.5">
            <span className="text-[11px] text-gray-400 font-medium">X</span>
            <select
              value={xCol}
              onChange={e => setXCol(e.target.value)}
              className="text-xs border border-gray-200 dark:border-gray-600 rounded-md px-2 py-1 outline-none bg-white dark:bg-gray-800 dark:text-gray-200 focus:border-blue-400 transition"
            >
              {result.columns.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
        )}

        {/* Y columns (checkboxes for multi-series, single select for pie/scatter) */}
        {chartType === 'pie' || chartType === 'scatter' ? (
          <div className="flex items-center gap-1.5">
            <span className="text-[11px] text-gray-400 font-medium">Y</span>
            <select
              value={yCols[0] ?? ''}
              onChange={e => setYCols([e.target.value])}
              className="text-xs border border-gray-200 dark:border-gray-600 rounded-md px-2 py-1 outline-none bg-white dark:bg-gray-800 dark:text-gray-200 focus:border-blue-400 transition"
            >
              {numericCols.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
        ) : (
          <div className="flex items-start gap-1.5">
            <span className="text-[11px] text-gray-400 font-medium mt-1">Y</span>
            <div className="flex flex-wrap gap-1">
              {numericCols.map((col, i) => {
                const active = yCols.includes(col)
                return (
                  <button
                    key={col}
                    onClick={() => toggleYCol(col)}
                    className={`text-[11px] px-2 py-0.5 rounded-md border font-medium transition-all ${
                      active
                        ? 'border-transparent text-white'
                        : 'border-gray-200 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:border-gray-300 dark:hover:border-gray-500'
                    }`}
                    style={active ? { backgroundColor: PALETTE[i % PALETTE.length] } : {}}
                  >
                    {col}
                  </button>
                )
              })}
            </div>
          </div>
        )}

        {/* Point count hint */}
        {result.data.length > MAX_POINTS && (
          <span className="text-[11px] text-gray-400 dark:text-gray-500 ml-auto self-center">
            Showing first {MAX_POINTS.toLocaleString()} of {result.data.length.toLocaleString()} rows
          </span>
        )}
      </div>

      {/* Chart canvas */}
      {renderChart()}
    </div>
  )
}
