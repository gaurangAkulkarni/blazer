import { createContext } from 'react'
import type { QueryResult, SnippetGroup } from '../../lib/types'

export interface ChatStreamCtx {
  /** True while the AI is streaming THIS message (false for all other messages) */
  isStreaming: boolean
  /** Whether autorun is enabled globally */
  autoRun: boolean
  /** Stable ID of the message that owns this context (used for stable queryId hashing) */
  messageId: string
  /** Persisted query results for this message (used to restore QueryBlock state after restart) */
  existingResults: QueryResult[]
  /** Available snippet groups — passed down so the save popover can show a group picker */
  snippetGroups?: SnippetGroup[]
  /** Save the current query block as a named snippet (with optional group) */
  onSaveSnippet?: (query: string, engine: 'blazer' | 'duckdb', name: string, groupId?: string) => void
  /** Append text to the chat input (used by QueryBlock to send query+error back to chat) */
  onSendToChat?: (text: string) => void
  /** Whether agentic mode is active globally (queries auto-run when true) */
  agenticMode?: boolean
  /** Whether an agentic loop is currently running for this message */
  agenticActive?: boolean
  /** 0-based index of the currently executing step in the plan */
  agenticCurrentStep?: number
  /** Parsed plan step strings for the current agentic run */
  agenticPlanSteps?: string[]
  /** True if the last agentic step returned an error */
  agenticStepError?: boolean
}

export const ChatStreamContext = createContext<ChatStreamCtx>({
  isStreaming: false,
  autoRun: false,
  messageId: '',
  existingResults: [],
})
