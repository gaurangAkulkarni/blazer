import React, { useState, useEffect, useRef, useMemo } from 'react'
import {
  Zap,
  Table2,
  Eye,
  BarChart2,
  Download,
  Wrench,
  CheckCircle,
  XCircle,
  Loader2,
  ChevronDown,
  ChevronUp,
} from 'lucide-react'
import type { ToolCallRecord } from '../../lib/types'

const TOOL_ICONS: Record<string, React.ElementType> = {
  run_sql:         Zap,
  describe_tables: Table2,
  get_sample_rows: Eye,
  column_stats:    BarChart2,
  export_result:   Download,
}

const TOOL_LABELS: Record<string, string> = {
  run_sql:         'SQL',
  describe_tables: 'Describe',
  get_sample_rows: 'Sample',
  column_stats:    'Stats',
  export_result:   'Export',
}

interface Props {
  toolCall: ToolCallRecord
}

// React.memo prevents re-renders when toolCall reference hasn't changed.
// During LLM streaming, setMessages fires 30+ times/sec updating message.content,
// but the toolCalls array keeps the same object references via object spread —
// so completed chips are skipped entirely by the reconciler.
export const ToolCallChip = React.memo(function ToolCallChip({ toolCall }: Props) {
  const [expanded, setExpanded] = useState(false)
  // Live elapsed timer — ticks every 250 ms while the chip is running
  const [elapsed, setElapsed] = useState(0)
  const mountedAt = useRef(Date.now())

  useEffect(() => {
    if (toolCall.status !== 'running') return
    mountedAt.current = Date.now()
    setElapsed(0)
    const interval = setInterval(() => setElapsed(Date.now() - mountedAt.current), 250)
    return () => clearInterval(interval)
  }, [toolCall.status])

  const Icon = TOOL_ICONS[toolCall.name] ?? Wrench
  const label = TOOL_LABELS[toolCall.name] ?? toolCall.name

  const isRunning = toolCall.status === 'running'
  const isError   = toolCall.status === 'error'

  const containerColor = isRunning
    ? 'bg-amber-50 dark:bg-amber-900/20 border-amber-200 dark:border-amber-700'
    : isError
    ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-700'
    : 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-700'

  const iconColor = isRunning
    ? 'text-amber-500'
    : isError
    ? 'text-red-500'
    : 'text-green-500'

  const StatusIcon = isRunning
    ? Loader2
    : isError
    ? XCircle
    : CheckCircle

  const summaryText = isRunning
    ? 'running…'
    : isError
    ? 'error'
    : getResultSummary(toolCall)

  // Memoize JSON serialization — these only recompute when the actual data changes,
  // not on every parent re-render triggered by streaming content updates.
  const argsJson  = useMemo(() => JSON.stringify(toolCall.arguments, null, 2), [toolCall.arguments])
  const resultJson = useMemo(
    () => toolCall.result != null ? JSON.stringify(toolCall.result, null, 2) : null,
    [toolCall.result],
  )

  return (
    <div className={`my-1 rounded-lg border text-[11px] font-mono ${containerColor}`}>
      <button
        className="w-full flex items-center gap-2 px-3 py-1.5 text-left"
        onClick={() => setExpanded(e => !e)}
      >
        <Icon size={12} className="shrink-0 text-gray-500 dark:text-gray-400" />
        <span className="font-semibold text-gray-700 dark:text-gray-300">{label}</span>
        <span className="flex items-center gap-1 text-gray-500 dark:text-gray-400 flex-1">
          <StatusIcon
            size={11}
            className={`shrink-0 ${iconColor} ${isRunning ? 'animate-spin' : ''}`}
          />
          {summaryText}
        </span>
        <span className="text-gray-400 dark:text-gray-500 tabular-nums">
          {isRunning
            ? elapsed < 1000
              ? `${elapsed}ms`
              : `${(elapsed / 1000).toFixed(1)}s`
            : toolCall.duration_ms != null
              ? toolCall.duration_ms < 1000
                ? `${toolCall.duration_ms}ms`
                : `${(toolCall.duration_ms / 1000).toFixed(1)}s`
              : null}
        </span>
        {expanded
          ? <ChevronUp size={11} className="text-gray-400 shrink-0" />
          : <ChevronDown size={11} className="text-gray-400 shrink-0" />}
      </button>

      {expanded && (
        <div className="border-t border-current/10 px-3 pb-2 pt-1">
          <div className="text-[10px] text-gray-500 dark:text-gray-400 mb-1">Arguments</div>
          <pre className="text-[10px] text-gray-700 dark:text-gray-300 max-h-32 overflow-y-auto whitespace-pre-wrap break-all">
            {argsJson}
          </pre>
          {resultJson != null && (
            <>
              <div className="text-[10px] text-gray-500 dark:text-gray-400 mb-1 mt-2">Result</div>
              <pre className="text-[10px] text-gray-700 dark:text-gray-300 max-h-48 overflow-y-auto whitespace-pre-wrap break-all">
                {resultJson}
              </pre>
            </>
          )}
        </div>
      )}
    </div>
  )
})

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
