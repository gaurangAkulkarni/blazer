import React from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import type { ChatMessage } from '../../lib/types'
import { ToolCallChip } from './ToolCallChip'

interface Props {
  message: ChatMessage
  onSendToChat?: (text: string) => void
}

const FOLLOW_UP_ACTIONS = [
  { label: 'Explore distributions', prompt: 'Show distribution charts for the key numeric columns.' },
  { label: 'Find anomalies', prompt: 'Identify outliers and data quality issues in this dataset.' },
  { label: 'Show trends', prompt: 'Analyse trends over time if there is a date column.' },
]

export function AutoProfileCard({ message, onSendToChat }: Props) {
  return (
    <div className="rounded-xl border border-indigo-200 dark:border-indigo-700 bg-gradient-to-b from-indigo-50 to-white dark:from-indigo-900/20 dark:to-gray-900 overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-indigo-100 dark:border-indigo-800 bg-indigo-50 dark:bg-indigo-900/30">
        <span className="text-lg">✨</span>
        <span className="text-sm font-semibold text-indigo-700 dark:text-indigo-300">Data Profile</span>
      </div>

      {/* Tool call chips */}
      {message.toolCalls && message.toolCalls.length > 0 && (
        <div className="px-4 pt-3 space-y-1">
          {message.toolCalls.map(tc => (
            <ToolCallChip key={tc.id} toolCall={tc} />
          ))}
        </div>
      )}

      {/* Streamed content */}
      {message.content && (
        <div className="px-4 py-3 prose prose-sm dark:prose-invert max-w-none text-sm">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content}</ReactMarkdown>
        </div>
      )}

      {/* Action buttons */}
      {message.content && !message.content.includes('…') && onSendToChat && (
        <div className="flex flex-wrap gap-2 px-4 pb-3">
          {FOLLOW_UP_ACTIONS.map(a => (
            <button
              key={a.label}
              onClick={() => onSendToChat(a.prompt)}
              className="text-xs px-3 py-1 rounded-full border border-indigo-300 dark:border-indigo-600 text-indigo-600 dark:text-indigo-300 hover:bg-indigo-50 dark:hover:bg-indigo-900/30 transition-colors"
            >
              {a.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
