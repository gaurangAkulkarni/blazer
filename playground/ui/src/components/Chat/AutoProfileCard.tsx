import React from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import remarkMath from 'remark-math'
import rehypeKatex from 'rehype-katex'
import { Sparkles } from 'lucide-react'
import type { ChatMessage } from '../../lib/types'
import { ToolCallChip } from './ToolCallChip'
import { WithLeadingIcon } from '../../lib/markdownIcons'

const mdComponents = {
  h1: ({ children }: { children?: React.ReactNode }) => (
    <h1 className="text-base font-bold mt-3 mb-1 text-gray-900 dark:text-gray-100">
      <WithLeadingIcon iconSize={15}>{children}</WithLeadingIcon>
    </h1>
  ),
  h2: ({ children }: { children?: React.ReactNode }) => (
    <h2 className="text-sm font-semibold mt-2.5 mb-1 text-gray-800 dark:text-gray-200">
      <WithLeadingIcon iconSize={13}>{children}</WithLeadingIcon>
    </h2>
  ),
  h3: ({ children }: { children?: React.ReactNode }) => (
    <h3 className="text-sm font-medium mt-2 mb-0.5 text-gray-700 dark:text-gray-300">
      <WithLeadingIcon iconSize={12}>{children}</WithLeadingIcon>
    </h3>
  ),
  h4: ({ children }: { children?: React.ReactNode }) => (
    <h4 className="text-xs font-semibold mt-1.5 mb-0.5 text-gray-600 dark:text-gray-400 uppercase tracking-wide">
      <WithLeadingIcon iconSize={11}>{children}</WithLeadingIcon>
    </h4>
  ),
  li: ({ children }: { children?: React.ReactNode }) => (
    <li className="my-0.5"><WithLeadingIcon iconSize={11}>{children}</WithLeadingIcon></li>
  ),
  table: ({ children }: { children?: React.ReactNode }) => (
    <div className="overflow-x-auto my-2">
      <table className="min-w-full text-xs border-collapse">{children}</table>
    </div>
  ),
  th: ({ children }: { children?: React.ReactNode }) => (
    <th className="border border-gray-200 dark:border-gray-700 px-2 py-1 bg-gray-50 dark:bg-gray-800 text-left font-medium text-gray-700 dark:text-gray-300">{children}</th>
  ),
  td: ({ children }: { children?: React.ReactNode }) => (
    <td className="border border-gray-200 dark:border-gray-700 px-2 py-1 text-gray-600 dark:text-gray-400">{children}</td>
  ),
}

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
    <div className="w-full rounded-xl border border-indigo-200 dark:border-indigo-700 bg-gradient-to-b from-indigo-50 to-white dark:from-indigo-900/20 dark:to-gray-900 overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-indigo-100 dark:border-indigo-800 bg-indigo-50 dark:bg-indigo-900/30">
        <Sparkles size={14} className="text-indigo-500 dark:text-indigo-400 shrink-0" />
        <span className="text-sm font-semibold text-indigo-700 dark:text-indigo-300">Data Profile</span>
      </div>

      {/* Body — chips and content share one padded region so bottom spacing is always consistent */}
      <div className="px-4 pt-3 pb-3 space-y-3">
        {message.toolCalls && message.toolCalls.length > 0 && (
          <div className="space-y-1">
            {message.toolCalls.map(tc => (
              <ToolCallChip key={tc.id} toolCall={tc} />
            ))}
          </div>
        )}

        {message.content && (
          <div className="prose prose-sm dark:prose-invert max-w-none text-sm">
            <ReactMarkdown remarkPlugins={[remarkGfm, remarkMath]} rehypePlugins={[rehypeKatex]} components={mdComponents}>
              {message.content}
            </ReactMarkdown>
          </div>
        )}
      </div>

      {/* Action buttons — sit directly below body with a subtle separator */}
      {message.content && !message.content.includes('…') && onSendToChat && (
        <div className="flex flex-wrap gap-2 px-4 pb-3 border-t border-indigo-100 dark:border-indigo-800 pt-2">
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
