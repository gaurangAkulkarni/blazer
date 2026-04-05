import React, { useState, useRef, useMemo, useCallback } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { QueryBlock } from './QueryBlock'
import { ChatStreamContext } from './ChatStreamContext'
import type { ChatMessage, QueryResult, SnippetGroup } from '../../lib/types'

interface Props {
  message: ChatMessage
  onQueryResult?: (messageId: string, result: QueryResult, query: string, engine: 'blazer' | 'duckdb') => void
  onSend?: (text: string) => void
  /** Append text to chat input without sending (for "copy to chat" actions) */
  onAppendToChat?: (text: string) => void
  /** Global streaming flag */
  isStreaming?: boolean
  /** Whether this is the last message in the list (used to scope streaming context) */
  isLastMessage?: boolean
  autoRun?: boolean
  onSaveSnippet?: (query: string, engine: 'blazer' | 'duckdb', name: string, groupId?: string) => void
  snippetGroups?: SnippetGroup[]
  /** Agentic mode props */
  agenticMode?: boolean
  agenticActive?: boolean
  agenticCurrentStep?: number
  agenticPlanSteps?: string[]
  agenticStepError?: boolean
}

function CopyBtn({ text, light = false }: { text: string; light?: boolean }) {
  const [copied, setCopied] = useState(false)
  const handle = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }
  return (
    <button
      onClick={handle}
      title="Copy message"
      className={`p-1 rounded transition opacity-0 group-hover:opacity-100 focus:opacity-100
        ${light
          ? 'text-white/50 hover:text-white hover:bg-white/15'
          : 'text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-700'}`}
    >
      {copied ? (
        <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className={light ? 'text-green-300' : 'text-green-500'}>
          <polyline points="20 6 9 17 4 12"/>
        </svg>
      ) : (
        <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
        </svg>
      )}
    </button>
  )
}

// ── LLM Context Inspector modal ───────────────────────────────────────────────
const ROLE_COLORS: Record<string, { bg: string; text: string; border: string; label: string }> = {
  system: { bg: 'bg-amber-50',  text: 'text-amber-800', border: 'border-amber-200', label: 'system'    },
  user:   { bg: 'bg-blue-50',   text: 'text-blue-800',  border: 'border-blue-200',  label: 'user'      },
  assistant: { bg: 'bg-gray-50', text: 'text-gray-700', border: 'border-gray-200',  label: 'assistant' },
}

function ContextInspector({ ctx, onClose }: { ctx: { role: string; content: string }[]; onClose: () => void }) {
  const [copiedIdx, setCopiedIdx] = useState<number | null>(null)
  const [expandedIdx, setExpandedIdx] = useState<Set<number>>(new Set([ctx.length - 1]))

  const copyAll = () => {
    const text = ctx.map((m) => `[${m.role.toUpperCase()}]\n${m.content}`).join('\n\n---\n\n')
    navigator.clipboard.writeText(text)
  }

  const toggleExpand = (i: number) =>
    setExpandedIdx((prev) => { const s = new Set(prev); s.has(i) ? s.delete(i) : s.add(i); return s })

  const copyMsg = (i: number, content: string) => {
    navigator.clipboard.writeText(content)
    setCopiedIdx(i)
    setTimeout(() => setCopiedIdx(null), 1500)
  }

  const totalChars = ctx.reduce((n, m) => n + m.content.length, 0)

  return (
    <div className="fixed inset-0 z-[80] flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-white dark:bg-gray-900 rounded-2xl shadow-2xl w-full max-w-[760px] max-h-[85vh] flex flex-col overflow-hidden">
        {/* Header */}
        <div className="shrink-0 flex items-center justify-between px-5 py-3.5 border-b border-gray-100 dark:border-gray-800 bg-gray-50/80 dark:bg-gray-800/80">
          <div className="flex items-center gap-2.5">
            <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="text-gray-500">
              <polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>
            </svg>
            <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">LLM Context</span>
            <span className="text-[11px] bg-gray-200 dark:bg-gray-700 text-gray-600 dark:text-gray-400 px-2 py-0.5 rounded-full font-medium">
              {ctx.length} message{ctx.length !== 1 ? 's' : ''}
            </span>
            <span className="text-[11px] text-gray-400 font-mono">
              ~{(totalChars / 4).toFixed(0)} tok
            </span>
          </div>
          <div className="flex items-center gap-1">
            <button
              onClick={copyAll}
              className="text-xs text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 px-2.5 py-1 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition flex items-center gap-1.5"
              title="Copy entire context as text"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
              </svg>
              Copy all
            </button>
            <button onClick={onClose} className="text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition">
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
          </div>
        </div>

        {/* Messages list */}
        <div className="flex-1 overflow-y-auto p-4 space-y-2">
          {ctx.map((msg, i) => {
            const style = ROLE_COLORS[msg.role] ?? ROLE_COLORS.system
            const expanded = expandedIdx.has(i)
            const isLong = msg.content.length > 400
            const preview = isLong && !expanded ? msg.content.slice(0, 400) + '…' : msg.content

            return (
              <div key={i} className={`rounded-xl border ${style.border} ${style.bg} overflow-hidden`}>
                {/* Role bar */}
                <div
                  className={`flex items-center justify-between px-3 py-1.5 cursor-pointer select-none ${style.bg}`}
                  onClick={() => toggleExpand(i)}
                >
                  <div className="flex items-center gap-2">
                    <span className={`text-[10px] font-bold uppercase tracking-widest ${style.text}`}>
                      {style.label}
                    </span>
                    <span className="text-[10px] text-gray-400 font-mono">
                      {msg.content.length.toLocaleString()} chars
                    </span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <button
                      onClick={(e) => { e.stopPropagation(); copyMsg(i, msg.content) }}
                      className="text-[10px] text-gray-400 hover:text-gray-700 px-1.5 py-0.5 rounded hover:bg-white/60 transition flex items-center gap-1"
                    >
                      {copiedIdx === i ? '✓' : (
                        <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                        </svg>
                      )}
                    </button>
                    <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={`text-gray-400 transition-transform ${expanded ? '' : '-rotate-90'}`}>
                      <polyline points="6 9 12 15 18 9"/>
                    </svg>
                  </div>
                </div>
                {/* Content */}
                <pre className={`m-0 px-3 pb-3 pt-1 text-[11.5px] font-mono text-gray-700 dark:text-gray-300 whitespace-pre-wrap break-words leading-relaxed bg-white/60 dark:bg-gray-800/60 ${!expanded && isLong ? 'max-h-32 overflow-hidden' : ''}`}>
                  {preview}
                </pre>
                {isLong && (
                  <button
                    onClick={() => toggleExpand(i)}
                    className={`w-full text-[10px] text-gray-500 hover:text-gray-800 py-1.5 border-t ${style.border} hover:bg-white/40 transition font-medium`}
                  >
                    {expanded ? '▲ Collapse' : `▼ Show all (${msg.content.length.toLocaleString()} chars)`}
                  </button>
                )}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const totalSec = ms / 1000
  const h = Math.floor(totalSec / 3600)
  const m = Math.floor((totalSec % 3600) / 60)
  const s = totalSec % 60
  const parts: string[] = []
  if (h > 0) parts.push(`${h}h`)
  if (m > 0) parts.push(`${m}m`)
  // Always show seconds if no hours/minutes, or if there are remaining seconds
  if (parts.length === 0 || s >= 0.05) parts.push(`${s.toFixed(s < 10 && parts.length > 0 ? 1 : 1)}s`)
  return parts.join(' ')
}

// Threshold above which user messages are collapsed by default
const USER_MSG_COLLAPSE_THRESHOLD = 280

export function MessageBubble({ message, onQueryResult, onSend, onAppendToChat, isStreaming, isLastMessage, autoRun, onSaveSnippet, snippetGroups, agenticMode, agenticActive, agenticCurrentStep, agenticPlanSteps, agenticStepError }: Props) {
  const isUser = message.role === 'user'
  const [showContext, setShowContext] = useState(false)
  const isLongUserMsg = isUser && message.content.length > USER_MSG_COLLAPSE_THRESHOLD
  const [userMsgExpanded, setUserMsgExpanded] = useState(false)

  // Counter incremented for each QueryBlock rendered within this message.
  // Reset to 0 before each ReactMarkdown pass so indices are deterministic.
  const blockIndexRef = useRef(0)
  blockIndexRef.current = 0

  // ── Stable refs so memoized components never go stale ───────────────────────
  const onQueryResultRef = useRef(onQueryResult)
  onQueryResultRef.current = onQueryResult
  const messageIdRef = useRef(message.id)
  messageIdRef.current = message.id

  // ── Context value: only the last (currently streaming) message carries       ──
  // ── isStreaming=true so older messages' QueryBlocks never auto-run          ──
  const onSaveSnippetRef = useRef(onSaveSnippet)
  onSaveSnippetRef.current = onSaveSnippet

  const onAppendToChatRef = useRef(onAppendToChat)
  onAppendToChatRef.current = onAppendToChat

  const ctxValue = useMemo(() => ({
    isStreaming: isLastMessage ? !!isStreaming : false,
    autoRun: isLastMessage ? !!autoRun : false,
    messageId: message.id,
    existingResults: message.queryResults ?? [],
    snippetGroups,
    onSaveSnippet: onSaveSnippet
      ? (query: string, engine: 'blazer' | 'duckdb', name: string, groupId?: string) =>
          onSaveSnippetRef.current?.(query, engine, name, groupId)
      : undefined,
    onSendToChat: onAppendToChat
      ? (text: string) => onAppendToChatRef.current?.(text)
      : undefined,
    agenticMode: !!agenticMode,
    agenticActive: isLastMessage ? !!agenticActive : false,
    agenticCurrentStep: agenticCurrentStep ?? 0,
    agenticPlanSteps: agenticPlanSteps ?? [],
    agenticStepError: isLastMessage ? !!agenticStepError : false,
  }), [isStreaming, autoRun, isLastMessage, message.id, message.queryResults, !!onSaveSnippet, !!onAppendToChat, snippetGroups, agenticMode, agenticActive, agenticCurrentStep, agenticPlanSteps, agenticStepError])

  // ── User message markdown components — code blocks rendered but NOT executable ─
  const userMdComponents = useMemo(() => ({
    code({ className, children }: { className?: string; children?: React.ReactNode }) {
      const match = /language-(\w+)/.exec(className || '')
      const codeStr = String(children).replace(/\n$/, '')
      if (match) {
        // Fenced block — show as styled code with a copy button, no run
        return (
          <div className="my-1.5 rounded-lg overflow-hidden border border-white/10">
            <div className="flex items-center justify-between px-3 py-1 bg-white/10 text-[10px] text-white/50 font-mono">
              <span>{match[1]}</span>
              <CopyBtn text={codeStr} light />
            </div>
            <pre className="m-0 p-3 text-xs font-mono text-white/85 whitespace-pre-wrap break-words overflow-x-auto max-h-64 overflow-y-auto bg-black/20">
              {codeStr}
            </pre>
          </div>
        )
      }
      return (
        <code className="bg-white/15 text-white/90 px-1 py-0.5 rounded text-xs font-mono">
          {children}
        </code>
      )
    },
    p({ children }: { children?: React.ReactNode }) {
      return <p className="my-1 leading-relaxed">{children}</p>
    },
  }), [])

  // ── Fully stable components object — never changes after mount ──────────────
  // Dynamic values (onQueryResult, messageId) are accessed through refs.
  // Because the function reference is stable, ReactMarkdown will NEVER unmount
  // QueryBlock between renders, so prevStreamingRef inside QueryBlock works correctly.
  const mdComponents = useMemo(() => ({
    code({ className, children }: { className?: string; children?: React.ReactNode }) {
      const match = /language-(\w+)/.exec(className || '')
      const language = match ? match[1] : ''
      const codeStr = String(children).replace(/\n$/, '')
      if (language === 'plan') {
        // Plan is shown in the AgenticTimeline sidebar — suppress inline rendering
        return null
      }
      if (language) {
        const blockIndex = blockIndexRef.current++
        return (
          <QueryBlock
            code={codeStr}
            language={language}
            index={blockIndex}
            onQueryResult={(r, q, eng) =>
              onQueryResultRef.current?.(messageIdRef.current, r, q, eng)
            }
          />
        )
      }
      return (
        <code className="bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-200 px-1.5 py-0.5 rounded text-xs font-mono">
          {children}
        </code>
      )
    },
    table({ children }: { children?: React.ReactNode }) {
      return <div className="overflow-x-auto my-2"><table className="min-w-full text-xs border-collapse">{children}</table></div>
    },
    th({ children }: { children?: React.ReactNode }) {
      return <th className="border border-gray-200 px-2 py-1 bg-gray-50 text-left font-medium text-gray-700">{children}</th>
    },
    td({ children }: { children?: React.ReactNode }) {
      return <td className="border border-gray-200 px-2 py-1 text-gray-600">{children}</td>
    },
  }), []) // ← empty deps: truly stable; dynamic values flow through refs

  return (
    <div className={`flex flex-col ${isUser ? 'items-end' : 'items-start'}`}>
      <div className={`group relative max-w-[92%] rounded-2xl px-4 py-3 ${isUser ? 'bg-gray-900 text-white' : 'bg-gray-50 dark:bg-gray-800 text-gray-900 dark:text-gray-100 border border-gray-100 dark:border-gray-700'}`}>

        {/* ── Copy button — always top-right so it never overlaps text ── */}
        <div className="absolute top-2 right-2">
          <CopyBtn text={message.content} light={isUser} />
        </div>

        {/* ── Attached files badge (user only) ────────────────── */}
        {isUser && message.attachedFiles && message.attachedFiles.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mb-2 pr-6">
            {message.attachedFiles.map((f) => (
              <span key={f.path} className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-md bg-white/20 text-white/90" title={f.path}>
                <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>
                </svg>
                {f.name}
              </span>
            ))}
          </div>
        )}

        {isUser ? (
          <>
            {/* ── Collapsible content for long messages ─────────── */}
            <div className={`relative pr-6 ${isLongUserMsg && !userMsgExpanded ? 'max-h-24 overflow-hidden' : ''}`}>
              <div className="text-sm leading-relaxed text-white">
                <ReactMarkdown remarkPlugins={[remarkGfm]} components={userMdComponents}>
                  {message.content}
                </ReactMarkdown>
              </div>
              {/* Gradient fade when collapsed */}
              {isLongUserMsg && !userMsgExpanded && (
                <div className="absolute bottom-0 left-0 right-0 h-8 bg-gradient-to-t from-gray-900 to-transparent pointer-events-none" />
              )}
            </div>

            {/* Expand / collapse toggle */}
            {isLongUserMsg && (
              <button
                onClick={() => setUserMsgExpanded(v => !v)}
                className="mt-1 text-[11px] text-white/50 hover:text-white/90 transition select-none flex items-center gap-1"
              >
                {userMsgExpanded ? (
                  <>
                    <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="18 15 12 9 6 15"/></svg>
                    Show less
                  </>
                ) : (
                  <>
                    <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
                    Show more
                  </>
                )}
              </button>
            )}

            {/* Context inspector button */}
            {message.sentContext && (
              <button
                onClick={() => setShowContext(true)}
                title="View full LLM context sent"
                className="mt-1 flex items-center gap-1 text-[10px] text-white/40 hover:text-white/80 transition select-none"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>
                </svg>
                {message.sentContext.length} ctx msgs
              </button>
            )}
          </>
        ) : (
          <>
            <div className="chat-prose prose prose-sm max-w-none prose-gray pr-6">
              {/* Provider scopes the streaming state to this message's QueryBlocks */}
              <ChatStreamContext.Provider value={ctxValue}>
                <ReactMarkdown remarkPlugins={[remarkGfm]} components={mdComponents}>
                  {message.content}
                </ReactMarkdown>
              </ChatStreamContext.Provider>
            </div>

            {/* ── Timing + token footer ──────────────────────────── */}
            {(message.duration_ms != null || message.tokens_in != null) && (
              <div className="flex items-center gap-2.5 mt-2 pt-2 border-t border-gray-100 dark:border-gray-700 text-[11px] text-gray-400 dark:text-gray-500 select-none">
                {message.duration_ms != null && (
                  <span className="flex items-center gap-1">
                    <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>
                    </svg>
                    {formatDuration(message.duration_ms)}
                  </span>
                )}
                {message.tokens_in != null && (
                  <span className="flex items-center gap-1" title={`${message.tokens_in} prompt + ${message.tokens_out ?? 0} completion`}>
                    <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                    </svg>
                    <span>{(message.tokens_in + (message.tokens_out ?? 0)).toLocaleString()} tok</span>
                    <span className="text-gray-300">({message.tokens_in.toLocaleString()} in · {(message.tokens_out ?? 0).toLocaleString()} out)</span>
                  </span>
                )}
              </div>
            )}
          </>
        )}
      </div>

      {/* ── Follow-up suggestion chips (below the bubble) ───── */}
      {!isUser && message.suggestions && message.suggestions.length > 0 && onSend && !isStreaming && (
        <div className="flex flex-wrap gap-1.5 mt-1.5 pl-1">
          {message.suggestions.map((chip, i) => (
            <button
              key={i}
              onClick={() => onSend(chip)}
              className="inline-flex items-center gap-1 text-[11.5px] text-indigo-600 bg-indigo-50 hover:bg-indigo-100 border border-indigo-100 hover:border-indigo-200 px-2.5 py-1 rounded-full transition-colors select-none"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="shrink-0 opacity-60">
                <polyline points="9 18 15 12 9 6"/>
              </svg>
              {chip}
            </button>
          ))}
        </div>
      )}

      {/* ── LLM Context Inspector modal ──────────────────────── */}
      {showContext && message.sentContext && (
        <ContextInspector ctx={message.sentContext} onClose={() => setShowContext(false)} />
      )}
    </div>
  )
}
