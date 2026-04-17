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
  /** Called with the runId of the bottommost visible message (null = non-agentic) */
  onRunVisible?: (runId: string | null) => void
}

export function MessageList({ messages, isStreaming, onQueryResult, onSend, onAppendToChat, autoRun, onSaveSnippet, snippetGroups, agenticMode, agenticActive, agenticCurrentStep, agenticPlanSteps, agenticStepError, onRunVisible }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null)
  const observerRef = useRef<IntersectionObserver | null>(null)
  // Tracks which elements are currently intersecting and their runIds
  const intersectingRef = useRef<Map<Element, string | null>>(new Map())

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages.length, isStreaming])

  // IntersectionObserver: observe every message; emit the runId of the
  // bottommost visible one (null when a non-agentic message is bottommost).
  useEffect(() => {
    if (!onRunVisible) return
    observerRef.current?.disconnect()
    intersectingRef.current.clear()

    observerRef.current = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const runId = ((entry.target as HTMLElement).dataset.msgRunId) || null
          if (entry.isIntersecting) {
            intersectingRef.current.set(entry.target, runId)
          } else {
            intersectingRef.current.delete(entry.target)
          }
        }
        // Pick the bottommost (highest top value) visible message
        let bestTop = -Infinity
        let bestRunId: string | null = null
        intersectingRef.current.forEach((runId, el) => {
          const top = el.getBoundingClientRect().top
          if (top > bestTop) {
            bestTop = top
            bestRunId = runId
          }
        })
        onRunVisible(bestRunId)
      },
      { root: null, threshold: 0.1 },
    )
    document.querySelectorAll('[data-msg-run-id]').forEach((el) => observerRef.current?.observe(el))
    return () => {
      observerRef.current?.disconnect()
      intersectingRef.current.clear()
    }
  }, [messages, onRunVisible])

  const lastMsg = messages[messages.length - 1]
  // During tool-calling turns, content is empty (all tokens stripped) but the
  // message is visible (chips rendering). Show the slim bar instead of the
  // full rotating indicator so we don't get a duplicate spinner below the chips.
  const lastMsgHasToolContent = !!(lastMsg?.toolCalls?.length || lastMsg?.isAutoProfile)
  // Show full rotating indicator only when truly waiting for first output and no chips yet
  const showIndicator = isStreaming && lastMsg?.role === 'assistant' && lastMsg?.content === '' && !lastMsgHasToolContent
  // Show a slim status strip while content is streaming in, OR during tool-calling turns
  const showStreamingBar = isStreaming && lastMsg?.role === 'assistant' && ((lastMsg?.content ?? '') !== '' || lastMsgHasToolContent)

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
        // A message with tool calls or auto-profile must always render (chips and card
        // are meaningful even when textual content is empty during tool calling turns).
        const hasToolContent = !!(msg.toolCalls?.length || msg.isAutoProfile)
        // Skip the empty assistant placeholder — StreamingIndicator covers this state.
        // But keep it if it has tool chips to show (content is empty because all LLM
        // output was tool-call tokens that got stripped from the display).
        if (msg.role === 'assistant' && msg.content === '' && !hasToolContent && isStreaming && idx === messages.length - 1) return null
        // Hide completed assistant messages with no content (e.g. empty LLM responses),
        // unless they carry tool call chips or an auto-profile card.
        if (msg.role === 'assistant' && !msg.content.trim() && !hasToolContent) return null
        // Hide internal agentic loop continuation messages from the chat UI
        if (msg.agenticContinuation) return null
        return (
          <div key={msg.id} data-msg-run-id={msg.agenticRunId ?? ''}>
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
