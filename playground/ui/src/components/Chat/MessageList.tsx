import React, { useEffect, useRef } from 'react'
import { MessageBubble } from './MessageBubble'
import type { ChatMessage, QueryResult } from '../../lib/types'

interface Props {
  messages: ChatMessage[]
  isStreaming: boolean
  onQueryResult: (messageId: string, result: QueryResult) => void
}

export function MessageList({ messages, isStreaming, onQueryResult }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages.length, isStreaming])

  return (
    <div className="flex-1 overflow-y-auto px-4 py-4 space-y-3 min-h-0">
      {messages.length === 0 && (
        <div className="h-full flex flex-col items-center justify-center text-gray-400 select-none">
          <svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round" className="mb-4 opacity-30">
            <ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
          </svg>
          <p className="text-sm font-medium text-gray-500">Attach a data file and start querying</p>
          <p className="text-xs text-gray-400 mt-1">Supports Parquet, CSV, and partitioned Parquet directories</p>
        </div>
      )}
      {messages.map((msg) => (
        <MessageBubble
          key={msg.id}
          message={msg}
          onQueryResult={onQueryResult}
        />
      ))}
      {isStreaming && messages[messages.length - 1]?.role === 'assistant' && messages[messages.length - 1]?.content === '' && (
        <div className="flex justify-start">
          <div className="bg-gray-50 border border-gray-100 rounded-2xl px-4 py-3">
            <div className="flex gap-1 items-center">
              <div className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: '0ms' }} />
              <div className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: '150ms' }} />
              <div className="w-1.5 h-1.5 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: '300ms' }} />
            </div>
          </div>
        </div>
      )}
      <div ref={bottomRef} />
    </div>
  )
}
