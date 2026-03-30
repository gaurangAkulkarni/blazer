import React, { useRef, useEffect } from 'react'
import { MessageBubble } from './MessageBubble'
import type { ChatMessage, AddResultFn, AttachedFile } from '../../lib/types'

interface Props {
  messages: ChatMessage[]
  isStreaming: boolean
  onAddResult: AddResultFn
  preferredLanguage: string
  loadedFiles: AttachedFile[]
}

export function MessageList({ messages, isStreaming, onAddResult, preferredLanguage, loadedFiles }: Props) {
  const endRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  if (messages.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center max-w-sm px-4">
          <div className="text-5xl mb-5 opacity-20">&#9889;</div>
          <h2 className="text-lg font-semibold text-gray-900 mb-2">Blazer Playground</h2>
          <p className="text-sm text-gray-500 leading-relaxed">
            Ask me to analyze data using the blazer DataFrame engine.
            I can read CSV/Parquet files, filter, aggregate, sort, and more.
          </p>
          <div className="mt-5 space-y-2">
            <p className="text-xs text-gray-400 bg-gray-50 rounded-lg px-3 py-2">"Read sales.parquet and show average revenue by region"</p>
            <p className="text-xs text-gray-400 bg-gray-50 rounded-lg px-3 py-2">"Load data.csv, filter rows where age &gt; 30, sort by salary"</p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-y-auto px-4 py-4 space-y-3">
      {messages.map((msg) => (
        <MessageBubble
          key={msg.id}
          message={msg}
          onAddResult={onAddResult}
          preferredLanguage={preferredLanguage}
          loadedFiles={loadedFiles}
        />
      ))}
      {isStreaming && (
        <div className="flex items-center gap-2 text-gray-400 text-sm pl-2">
          <div className="flex gap-1">
            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
          </div>
        </div>
      )}
      <div ref={endRef} />
    </div>
  )
}
