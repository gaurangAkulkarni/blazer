import React, { useState } from 'react'
import type { ToolCallRecord } from '../../lib/types'

const TOOL_ICONS: Record<string, string> = {
  run_sql: '⚡',
  describe_tables: '🗂️',
  get_sample_rows: '👁️',
  column_stats: '📊',
  export_result: '💾',
}

const TOOL_LABELS: Record<string, string> = {
  run_sql: 'SQL',
  describe_tables: 'Describe',
  get_sample_rows: 'Sample',
  column_stats: 'Stats',
  export_result: 'Export',
}

interface Props {
  toolCall: ToolCallRecord
}

export function ToolCallChip({ toolCall }: Props) {
  const [expanded, setExpanded] = useState(false)
  const icon = TOOL_ICONS[toolCall.name] ?? '🔧'
  const label = TOOL_LABELS[toolCall.name] ?? toolCall.name

  const statusColor = toolCall.status === 'running'
    ? 'bg-amber-50 dark:bg-amber-900/20 border-amber-200 dark:border-amber-700'
    : toolCall.status === 'error'
    ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-700'
    : 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-700'

  const statusText = toolCall.status === 'running' ? 'running…'
    : toolCall.status === 'error' ? '✗ error'
    : `✓ ${getResultSummary(toolCall)}`

  return (
    <div className={`my-1 rounded-lg border text-[11px] font-mono ${statusColor}`}>
      <button
        className="w-full flex items-center gap-2 px-3 py-1.5 text-left"
        onClick={() => setExpanded(e => !e)}
      >
        <span>{icon}</span>
        <span className="font-medium text-gray-700 dark:text-gray-300">{label}</span>
        <span className="text-gray-400 dark:text-gray-500 flex-1">{statusText}</span>
        {toolCall.duration_ms != null && (
          <span className="text-gray-400 dark:text-gray-500">{toolCall.duration_ms}ms</span>
        )}
        <span className="text-gray-400">{expanded ? '▲' : '▼'}</span>
      </button>
      {expanded && (
        <div className="border-t border-current/10 px-3 pb-2 pt-1">
          <div className="text-[10px] text-gray-500 dark:text-gray-400 mb-1">Arguments</div>
          <pre className="text-[10px] text-gray-700 dark:text-gray-300 max-h-32 overflow-y-auto whitespace-pre-wrap break-all">
            {JSON.stringify(toolCall.arguments, null, 2)}
          </pre>
          {toolCall.result != null && (
            <>
              <div className="text-[10px] text-gray-500 dark:text-gray-400 mb-1 mt-2">Result</div>
              <pre className="text-[10px] text-gray-700 dark:text-gray-300 max-h-48 overflow-y-auto whitespace-pre-wrap break-all">
                {JSON.stringify(toolCall.result, null, 2)}
              </pre>
            </>
          )}
        </div>
      )}
    </div>
  )
}

function getResultSummary(tc: ToolCallRecord): string {
  if (!tc.result || typeof tc.result !== 'object') return 'done'
  const r = tc.result as Record<string, unknown>
  if (tc.name === 'run_sql') {
    const rows = r['row_count'] as number | undefined
    return rows != null ? `${rows} rows` : 'done'
  }
  if (tc.name === 'describe_tables') {
    const tables = (r['tables'] as unknown[] | undefined)?.length
    return tables != null ? `${tables} tables` : 'done'
  }
  if (tc.name === 'get_sample_rows') {
    const rows = r['row_count'] as number | undefined
    return rows != null ? `${rows} rows` : 'done'
  }
  if (tc.name === 'column_stats') {
    const cols = (r['column_profiles'] as unknown[] | undefined)?.length
    return cols != null ? `${cols} columns` : 'done'
  }
  if (tc.name === 'export_result') {
    return (r['path'] as string | undefined)?.split('/').pop() ?? 'saved'
  }
  return 'done'
}
