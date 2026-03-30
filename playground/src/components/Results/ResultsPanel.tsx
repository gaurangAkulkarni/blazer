import React, { useRef, useEffect } from 'react'

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const s = Math.floor(ms / 1000)
  const m = Math.floor(s / 60)
  const h = Math.floor(m / 60)
  const parts: string[] = []
  if (h > 0) parts.push(`${h}h`)
  if (m > 0) parts.push(`${m % 60}m`)
  parts.push(`${s % 60}s`)
  return parts.join(' ')
}
import { DataFrameTable } from '../Chat/DataFrameTable'
import type { ResultEntry } from '../../lib/types'

interface Props {
  results: ResultEntry[]
  onClear: () => void
  onHide: () => void
}

export function ResultsPanel({ results, onClear, onHide }: Props) {
  const endRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [results])

  return (
    <div className="flex flex-col h-full border-l border-gray-200 bg-gray-50/40">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-200 bg-white/90 shrink-0">
        <div className="flex items-center gap-2">
          <button
            onClick={onHide}
            className="text-gray-400 hover:text-gray-700 p-0.5 rounded hover:bg-gray-100 transition"
            title="Hide results panel"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </button>
          <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Results</span>
          {results.length > 0 && (
            <span className="text-xs bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded-full font-medium">
              {results.length}
            </span>
          )}
        </div>
        {results.length > 0 && (
          <button
            onClick={onClear}
            className="text-xs text-gray-400 hover:text-gray-700 transition px-2 py-0.5 rounded hover:bg-gray-100"
          >
            Clear all
          </button>
        )}
      </div>

      {/* Results list */}
      <div className="flex-1 overflow-y-auto p-3 space-y-3">
        {results.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center py-16">
            <div className="text-3xl mb-3 opacity-10">▶</div>
            <p className="text-xs text-gray-400">Run a code block to see results here</p>
          </div>
        ) : (
          results.map((entry) => (
            <div key={entry.id} id={`result-${entry.id}`} className="bg-white rounded-xl border border-gray-100 overflow-hidden shadow-sm">
              {/* Entry header */}
              <div className="flex items-center justify-between px-3 py-2 bg-gray-50 border-b border-gray-100">
                <code className="text-xs text-gray-600 truncate flex-1 font-mono">{entry.label}</code>
                <span
                  className={`text-xs ml-3 shrink-0 font-medium tabular-nums ${
                    entry.result.success ? 'text-green-600' : 'text-red-500'
                  }`}
                >
                  {entry.result.success ? '✓' : '✗'} {formatDuration(entry.result.durationMs)}
                </span>
              </div>

              {/* Entry content */}
              <div className="p-3 space-y-2">
                {entry.result.dataframes.length === 0 &&
                  !entry.result.stdout &&
                  !entry.result.stderr && (
                    <p className="text-xs text-gray-400 italic">No output</p>
                  )}
                {entry.result.dataframes.map((df, i) => (
                  <DataFrameTable key={i} data={df.data} columns={df.columns} shape={df.shape} />
                ))}
                {entry.result.stdout && (
                  <pre className="text-xs text-gray-700 bg-gray-50 rounded-lg p-2.5 overflow-x-auto whitespace-pre-wrap font-mono border border-gray-100">
                    {entry.result.stdout}
                  </pre>
                )}
                {entry.result.stderr && (
                  <pre className="text-xs text-red-600 bg-red-50 rounded-lg p-2.5 overflow-x-auto whitespace-pre-wrap font-mono border border-red-100">
                    {entry.result.stderr}
                  </pre>
                )}
              </div>
            </div>
          ))
        )}
        <div ref={endRef} />
      </div>
    </div>
  )
}
