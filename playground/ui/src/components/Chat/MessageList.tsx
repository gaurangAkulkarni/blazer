import React, { useEffect, useRef } from 'react'
import { MessageBubble } from './MessageBubble'
import { StreamingIndicator, StreamingBar } from './StreamingIndicator'
import type { ChatMessage, QueryResult, SnippetGroup } from '../../lib/types'

interface Props {
  messages: ChatMessage[]
  isStreaming: boolean
  onQueryResult: (messageId: string, result: QueryResult, query: string, engine: 'blazer' | 'duckdb') => void
  onSend?: (text: string) => void
  /** Append text to the chat input without sending (for "copy to chat" actions) */
  onAppendToChat?: (text: string) => void
  autoRun?: boolean
  onSaveSnippet?: (query: string, engine: 'blazer' | 'duckdb', name: string, groupId?: string) => void
  snippetGroups?: SnippetGroup[]
  /** Agentic mode props */
  agenticMode?: boolean
  agenticActive?: boolean
  agenticCurrentStep?: number
  agenticPlanSteps?: string[]
  agenticStepError?: boolean
  /** Called with the runId when an agentic-plan message scrolls into view */
  onRunVisible?: (runId: string) => void
}

export function MessageList({ messages, isStreaming, onQueryResult, onSend, onAppendToChat, autoRun, onSaveSnippet, snippetGroups, agenticMode, agenticActive, agenticCurrentStep, agenticPlanSteps, agenticStepError, onRunVisible }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null)
  const observerRef = useRef<IntersectionObserver | null>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages.length, isStreaming])

  // IntersectionObserver: call onRunVisible when an agentic plan message enters the viewport
  useEffect(() => {
    if (!onRunVisible) return
    observerRef.current?.disconnect()
    observerRef.current = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            const runId = (entry.target as HTMLElement).dataset.runId
            if (runId) onRunVisible(runId)
          }
        }
      },
      { root: null, threshold: 0.2 },
    )
    // Observe all currently rendered plan-start elements
    document.querySelectorAll('[data-run-id]').forEach((el) => observerRef.current?.observe(el))
    return () => observerRef.current?.disconnect()
  }, [messages, onRunVisible])

  const lastMsg = messages[messages.length - 1]
  // Show full rotating indicator when we're waiting for the very first chunk
  const showIndicator = isStreaming && lastMsg?.role === 'assistant' && lastMsg?.content === ''
  // Show a slim status strip while content is already streaming in
  const showStreamingBar = isStreaming && lastMsg?.role === 'assistant' && (lastMsg?.content ?? '') !== ''

  return (
    <div className="flex-1 overflow-y-auto px-4 py-4 space-y-3 min-h-0">
      {messages.length === 0 && (
        <div className="h-full flex flex-col items-center justify-center text-gray-400 select-none">
          <svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round" className="mb-4 opacity-30">
            <ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
          </svg>
          <p className="chat-prose font-medium text-gray-500">Attach a data file and start querying</p>
          <p className="chat-prose text-gray-400 mt-1" style={{ fontSize: 13 }}>Supports Parquet, CSV, and partitioned Parquet directories</p>
        </div>
      )}

      {messages.map((msg, idx) => {
        // Skip the empty assistant placeholder — StreamingIndicator covers this state
        if (msg.role === 'assistant' && msg.content === '' && isStreaming && idx === messages.length - 1) return null
        // Hide completed assistant messages with no content (e.g. empty LLM responses)
        if (msg.role === 'assistant' && !msg.content.trim()) return null
        // Hide internal agentic loop continuation messages from the chat UI
        if (msg.agenticContinuation) return null
        const isRunStart = (msg.agenticPlanSteps?.length ?? 0) > 0 && msg.agenticRunId
        return (
          <div key={msg.id} data-run-id={isRunStart ? msg.agenticRunId : undefined}>
            <MessageBubble
              message={msg}
              onQueryResult={onQueryResult}
              onSend={onSend}
              onAppendToChat={onAppendToChat}
              isStreaming={isStreaming}
              isLastMessage={idx === messages.length - 1}
              autoRun={autoRun}
              onSaveSnippet={onSaveSnippet}
              snippetGroups={snippetGroups}
              agenticMode={agenticMode}
              agenticActive={agenticActive}
              agenticCurrentStep={agenticCurrentStep}
              agenticPlanSteps={agenticPlanSteps}
              agenticStepError={agenticStepError}
            />
          </div>
        )
      })}

      {/* Waiting for first chunk — full rotating bubble */}
      {showIndicator && <StreamingIndicator />}

      {/* Content already streaming in — slim rotating bar */}
      {showStreamingBar && <StreamingBar />}

      <div ref={bottomRef} />
    </div>
  )
}
