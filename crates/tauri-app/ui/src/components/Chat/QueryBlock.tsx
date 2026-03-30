import React, { useState } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { DataFrameTable } from './DataFrameTable'
import type { QueryResult } from '../../lib/types'

interface Props {
  code: string
  language: string
  onQueryResult?: (result: QueryResult) => void
}

export function QueryBlock({ code, language, onQueryResult }: Props) {
  const [status, setStatus] = useState<'idle' | 'running' | 'success' | 'error'>('idle')
  const [result, setResult] = useState<QueryResult | null>(null)
  const [copied, setCopied] = useState(false)

  const isQuery = language === 'json'
  const isRunnable = isQuery && isValidQueryJson(code)

  const handleRun = async () => {
    if (!isRunnable) return
    setStatus('running')
    setResult(null)
    try {
      const parsed = JSON.parse(code)
      const r = await invoke<QueryResult>('run_query', { query: parsed })
      setResult(r)
      setStatus(r.success ? 'success' : 'error')
      if (r.success && onQueryResult) onQueryResult(r)
    } catch (e: any) {
      setResult({ success: false, error: e.message, data: [], columns: [], shape: [0, 0], duration_ms: 0 })
      setStatus('error')
    }
  }

  const handleCopy = () => {
    navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const langLabel = isRunnable ? 'blazer query' : language

  return (
    <div className="my-2 rounded-lg border border-gray-200 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-1.5 bg-gray-100 border-b border-gray-200 text-xs text-gray-500">
        <span className="font-medium flex items-center gap-1.5">
          {isRunnable && (
            <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="text-blue-500">
              <polygon points="5 3 19 12 5 21 5 3"/>
            </svg>
          )}
          {langLabel}
        </span>
        <div className="flex items-center gap-1.5">
          <button onClick={handleCopy} className="px-2 py-0.5 rounded hover:bg-gray-200 transition text-gray-500 hover:text-gray-700">
            {copied ? 'Copied!' : 'Copy'}
          </button>
          {isRunnable && (
            <button
              onClick={handleRun}
              disabled={status === 'running'}
              className={`px-2.5 py-0.5 rounded font-medium transition text-xs ${
                status === 'running' ? 'bg-gray-200 text-gray-400 cursor-wait'
                : status === 'success' ? 'bg-green-600 text-white hover:bg-green-700'
                : status === 'error' ? 'bg-red-500 text-white hover:bg-red-600'
                : 'bg-blue-600 text-white hover:bg-blue-700'
              }`}
            >
              {status === 'running' ? 'Running…' : status === 'success' ? '✓ Done' : status === 'error' ? '✗ Error' : '▶ Run Query'}
            </button>
          )}
        </div>
      </div>

      {/* Code */}
      <pre className="m-0 p-3 bg-gray-50 text-xs font-mono overflow-x-auto text-gray-700 leading-relaxed max-h-64 overflow-y-auto">
        {code}
      </pre>

      {/* Result */}
      {result && (
        <div className="border-t border-gray-200 p-3 bg-white">
          {result.success ? (
            <DataFrameTable
              data={result.data}
              columns={result.columns}
              shape={result.shape}
              durationMs={result.duration_ms}
            />
          ) : (
            <div className="text-xs text-red-600 font-mono bg-red-50 rounded p-2 border border-red-100">
              {result.error}
            </div>
          )}
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
