import { useState, useCallback, useRef, useEffect } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { listen } from '@tauri-apps/api/event'
import type { ChatMessage, AppSettings, AttachedFile, QueryResult, ConnectionAlias, ToolCallRecord } from '../lib/types'
import { resolveSkillPrompts, ENGINE_SKILL_IDS } from '../lib/skills'
import {
  dbLoadMessages, dbSaveMessage, dbClearMessages,
  dbLoadFiles, dbSaveFile, dbDeleteFile, dbClearFiles,
  migrateFromLocalStorage,
} from '../lib/db'
import { getToolSchemas } from '../lib/llm/toolSchemas'
import { shouldSendTools, cacheToolCallSupport } from '../lib/llm/toolCallSupport'
import { executeToolCall } from '../lib/tools/executeToolCall'

export type Engine = 'blazer' | 'duckdb'

let msgCounter = 0
const nextId = () => `msg-${++msgCounter}-${Date.now()}`

// ── Split multi-statement SQL blocks into separate fenced code blocks ─────────
// Some LLMs (especially smaller local ones) dump multiple SQL statements in a
// single ```sql block. We split them so each gets its own QueryBlock in the UI.

function extractSqlStatements(sql: string): string[] {
  const statements: string[] = []
  let current = ''
  let inLineComment = false
  let inString = false
  let stringChar = ''

  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i]
    const next = sql[i + 1] ?? ''

    if (inString) {
      current += ch
      if (ch === stringChar && sql[i - 1] !== '\\') inString = false
      continue
    }
    if (ch === "'" || ch === '"') { inString = true; stringChar = ch; current += ch; continue }

    if (!inLineComment && ch === '-' && next === '-') { inLineComment = true; current += ch; continue }
    if (inLineComment) {
      current += ch
      if (ch === '\n') inLineComment = false
      continue
    }

    if (ch === ';') {
      current += ch
      const rest = sql.slice(i + 1)
      if (rest.trim().length > 0) {
        // There is more SQL after this semicolon — end the current statement
        statements.push(current.trim())
        current = ''
        // Skip blank lines between statements
        while (i + 1 < sql.length && sql[i + 1] === '\n') i++
      }
      continue
    }

    current += ch
  }

  if (current.trim()) statements.push(current.trim())
  return statements.filter(s => s.length > 0)
}

// Ensure every ``` sequence starts on its own line.
// Fixes LLMs that concatenate fences with SQL content, e.g.:
//   "ORDER BY x DESC;```sql\nSELECT ..."  →  "ORDER BY x DESC;\n```sql\nSELECT ..."
function normalizeFenceMarkers(md: string): string {
  return md.replace(/([^\n])(`{3,})/g, '$1\n$2')
}

// Returns true when the accumulated SQL lines cannot possibly form a complete
// statement — meaning the LLM hallucinated a fence boundary mid-query and we
// should skip the fence marker and keep accumulating.
function sqlLooksIncomplete(lines: string[]): boolean {
  const trimmed = lines.join('\n').trimEnd()
  if (!trimmed) return false

  // Strip SQL line comments to check what's actually present
  const noComments = trimmed.replace(/--[^\n]*/g, '').trim()

  // Only comments, no actual SQL statement — definitely incomplete
  if (!noComments) return true

  // SELECT without a FROM clause — the query body is still being written
  if (/\bSELECT\b/i.test(noComments) && !/\bFROM\b/i.test(noComments)) return true

  // Ends with a keyword or operator that requires more SQL
  if (/(\bTHEN|\bAND|\bOR|\bWHERE|\bON|\bBY|,|\(|\bFROM|\bSELECT|\bCASE|\bWHEN|\bJOIN|\bSET|\bAS)[ \t]*$/i.test(trimmed)) return true

  // Unmatched open parens — definitely mid-expression (catches "month(tep" etc.)
  const opens = (trimmed.match(/\(/g) ?? []).length
  const closes = (trimmed.match(/\)/g) ?? []).length
  if (opens > closes) return true

  return false
}

function splitMultiStatementBlocks(markdown: string): string {
  // Step 1: force all ``` fence markers onto their own lines so the parser can
  // see them even when the LLM concatenates them with SQL content.
  const lines = normalizeFenceMarkers(markdown).split('\n')
  const out: string[] = []
  let inSqlFence = false
  let sqlLines: string[] = []

  function flushBlock() {
    if (sqlLines.length === 0) return
    const body = sqlLines.join('\n').trim()
    sqlLines = []
    if (!body) return
    const stmts = extractSqlStatements(body)
    if (stmts.length <= 1) {
      out.push('```sql')
      out.push(body)
      out.push('```')
    } else {
      for (const stmt of stmts) {
        out.push('```sql')
        out.push(stmt)
        out.push('```')
        out.push('')
      }
    }
  }

  for (const line of lines) {
    if (!inSqlFence) {
      if (/^```sql\s*$/.test(line)) {
        inSqlFence = true
        sqlLines = []
      } else {
        out.push(line)
      }
    } else {
      if (/^```\s*$/.test(line)) {
        // Plain closing fence — skip if SQL is syntactically incomplete
        if (sqlLooksIncomplete(sqlLines)) {
          // Hallucinated mid-query break — skip this fence line, keep accumulating
        } else {
          flushBlock()
          inSqlFence = false
        }
      } else if (/^```/.test(line)) {
        // Another fence line (```sql, ```json, etc.) — if SQL is incomplete treat
        // it as a hallucinated separator and keep accumulating; otherwise flush
        // and start a new block (the fence stays open).
        if (sqlLooksIncomplete(sqlLines)) {
          // Mid-query — skip this fence line, keep accumulating
        } else {
          flushBlock()
          // inSqlFence stays true — collecting next statement
        }
      } else {
        sqlLines.push(line)
      }
    }
  }

  // Handle unclosed fence at end of content (streaming in progress)
  if (inSqlFence) {
    flushBlock()
  }

  return out.join('\n')
}

export function useChat(settings: AppSettings, engine: Engine = 'blazer') {
  const [messages, setMessagesState] = useState<ChatMessage[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [loadedFiles, setLoadedFilesState] = useState<AttachedFile[]>([])

  // Debounce timer — DB writes fire 400 ms after the last setMessages call so
  // rapid streaming chunks don't hammer SQLite.
  const dbSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const scheduleSave = useCallback((msgs: ChatMessage[]) => {
    if (dbSaveTimerRef.current) clearTimeout(dbSaveTimerRef.current)
    dbSaveTimerRef.current = setTimeout(() => {
      // Only upsert the two most recent messages (the user+assistant pair from
      // the current turn).  Older messages are already persisted.
      const tail = msgs.slice(-2)
      tail.forEach((m) => dbSaveMessage(m).catch(console.error))
    }, 400)
  }, [])

  // Load persisted data from SQLite on first mount (also runs legacy migration).
  useEffect(() => {
    migrateFromLocalStorage()
      .then(() => dbLoadMessages())
      .then((msgs) => setMessagesState(msgs))
      .catch(console.error)

    dbLoadFiles()
      .then((files) => setLoadedFilesState(files))
      .catch(console.error)
  }, [])

  // Wrap setters: update React state + schedule a DB write.
  const setMessages = useCallback((updater: ChatMessage[] | ((prev: ChatMessage[]) => ChatMessage[])) => {
    setMessagesState((prev) => {
      const next = typeof updater === 'function' ? updater(prev) : updater
      scheduleSave(next)
      return next
    })
  }, [scheduleSave])

  const setLoadedFiles = useCallback((updater: AttachedFile[] | ((prev: AttachedFile[]) => AttachedFile[])) => {
    setLoadedFilesState((prev) => {
      const next = typeof updater === 'function' ? updater(prev) : updater
      return next
    })
  }, [])
  const streamingRef = useRef('')
  // Holds the cleanup fn for the active stream — called by stopStream()
  const stopStreamRef = useRef<(() => void) | null>(null)

  const addFiles = useCallback((files: AttachedFile[]) => {
    setLoadedFiles((prev) => {
      const existing = new Set(prev.map((f) => f.path))
      const toAdd = files.filter((f) => !existing.has(f.path))
      toAdd.forEach((f) => dbSaveFile(f).catch(console.error))
      return [...prev, ...toAdd]
    })
  }, [])

  const removeFile = useCallback((path: string) => {
    dbDeleteFile(path).catch(console.error)
    setLoadedFiles((prev) => prev.filter((f) => f.path !== path))
  }, [])

  const replaceFile = useCallback((oldPath: string, newFile: AttachedFile) => {
    dbDeleteFile(oldPath).catch(console.error)
    dbSaveFile(newFile).catch(console.error)
    setLoadedFiles((prev) => prev.map((f) => (f.path === oldPath ? newFile : f)))
  }, [])

  const addQueryResult = useCallback((messageId: string, result: QueryResult) => {
    setMessages((prev) =>
      prev.map((m) =>
        m.id === messageId
          ? { ...m, queryResults: [...(m.queryResults || []), result] }
          : m,
      ),
    )
  }, [])

  const sendMessage = useCallback(
    async (content: string, newAttachments?: AttachedFile[], perMessageSkillIds?: string[], opts?: { agenticMode?: boolean; agenticContinuation?: boolean; activeConnections?: ConnectionAlias[]; agenticRunId?: string; isAutoProfile?: boolean; toolCallDepth?: number }) => {
      let allFiles = loadedFiles
      if (newAttachments && newAttachments.length > 0) {
        addFiles(newAttachments)
        const existing = new Set(loadedFiles.map((f) => f.path))
        allFiles = [...loadedFiles, ...newAttachments.filter((f) => !existing.has(f.path))]
      }

      const provider = settings.active_provider
      const providerCfg: { api_key: string; model: string; temperature: number; base_url?: string } =
        provider === 'ollama'
          ? { api_key: '', model: settings.ollama.model, temperature: settings.ollama.temperature }
          : settings[provider]

      // Detect local/custom endpoint — these have small context windows and don't need token-heavy extras
      const isLocalEndpoint = provider === 'ollama' || !!(providerCfg.base_url?.trim())

      // Build file context for the LLM (adapted to engine syntax)
      let fileContext = ''
      if (allFiles.length > 0) {
        if (engine === 'duckdb') {
          fileContext = '## Attached Data Files\nUse these paths with DuckDB reader functions:\n'
          for (const f of allFiles) {
            const isDir = f.ext === 'parquet_dir' || f.ext === 'xlsx_dir' || f.ext === 'csv_dir' || f.ext === ''
            if (isDir && f.ext !== 'xlsx_dir' && f.ext !== 'csv_dir') {
              // Unknown or legacy folder — instruct the LLM to probe first
              fileContext += `- Directory: \`${f.path}\`\n`
              fileContext += `  IMPORTANT: Before querying, run \`SELECT file FROM glob('${f.path}/**/*') LIMIT 10\` to see what files are inside, then use the correct reader:\n`
              fileContext += `  • Excel files (.xlsx): \`read_xlsx((SELECT list(file) FROM glob('${f.path}/*.xlsx')))\`\n`
              fileContext += `  • Parquet files: \`read_parquet('${f.path}/**/*.parquet')\`\n`
              fileContext += `  • CSV files: \`read_csv_auto((SELECT list(file) FROM glob('${f.path}/*.csv')))\`\n`
            } else {
              const fn =
                f.ext === 'csv' || f.ext === 'tsv' ? `read_csv('${f.path}', auto_detect=true)` :
                f.ext === 'xlsx' ? `read_xlsx('${f.path}', all_varchar=true)` :
                f.ext === 'xlsx_dir' ? `read_xlsx((SELECT list(file) FROM glob('${f.path}/*.xlsx')), all_varchar=true)` :
                f.ext === 'csv_dir' ? `read_csv_auto((SELECT list(file) FROM glob('${f.path}/*.csv')))` :
                `read_parquet('${f.path}')`
              fileContext += `- \`${fn}\`\n`
              if (f.ext === 'xlsx' || f.ext === 'xlsx_dir') {
                fileContext += `  NOTE: all_varchar=true is set — every column is VARCHAR to preserve mixed-type cells.\n`
                fileContext += `  Use TRY_CAST(col AS DOUBLE) / TRY_CAST(col AS INTEGER) for numeric operations.\n`
              }
            }
            if (f.columns && f.columns.length > 0) {
              fileContext += `  Columns: ${f.columns.map((c) => `\`${c}\``).join(', ')}\n`
            }
          }
        } else {
          fileContext = '## Attached Data Files\nUse EXACTLY these absolute paths in your query JSON:\n'
          for (const f of allFiles) {
            const kind =
              f.ext === 'csv' || f.ext === 'tsv' || f.ext === 'csv_dir' ? 'csv' :
              f.ext === 'parquet_dir' ? 'parquet_dir' : 'parquet'
            fileContext += `- Path: \`${f.path}\` → source type: \`"${kind}"\`\n`
            if (f.columns && f.columns.length > 0) {
              fileContext += `  Columns: ${f.columns.map((c) => `\`${c}\``).join(', ')}\n`
            }
          }
        }
      }

      // Auto-inject the engine skill; preserve all other non-engine skills the user has active
      const engineSkillId = engine === 'duckdb' ? 'duckdb-engine' : 'blazer-engine'
      const userActiveSkills = settings.active_skills ?? ['blazer-engine']
      const otherSkills = userActiveSkills.filter((id) => !(ENGINE_SKILL_IDS as readonly string[]).includes(id))
      // Merge per-message skills (selected via # picker) — deduplicate
      const perMsgExtra = (perMessageSkillIds ?? []).filter(
        (id) => id !== engineSkillId && !otherSkills.includes(id),
      )
      const effectiveSkills = [engineSkillId, ...otherSkills, ...perMsgExtra]

      const skillPrompt = resolveSkillPrompts(
        effectiveSkills,
        (settings.custom_skills ?? []).map((s) => ({ ...s, builtIn: false as const })),
      )

      // Optionally inject follow-up suggestions instruction
      // Skip for local/custom endpoints — they have tight context windows and often can't follow the format
      const showChips = settings.show_follow_up_chips !== false
      const suggestionsInstruction = showChips && !isLocalEndpoint
        ? '\n\nIMPORTANT: At the very end of EVERY response, you MUST append exactly this block with no extra text after it:\n<suggestions>["short question 1","short question 2","short question 3"]</suggestions>\nRules: replace the placeholder strings with 3 real follow-up questions (each ≤6 words) relevant to the current analysis. Do not explain the block. Do not skip it.'
        : ''

      // Agentic mode instruction — appended when user starts an agentic run
      const agenticInstruction = opts?.agenticMode
        ? `\n\n## Agentic Data Analysis Mode

You are a data analysis agent operating in a step-by-step execution loop. After each SQL query you run, you will receive the FULL result data as a markdown table. Use the actual data to reason, extract insights, and decide what to do next.

### Workflow
1. **First response**: output a \`\`\`plan block describing your intended steps. The LAST step in the plan MUST always be "Synthesize findings & provide final assessment" (or similar wording). Then immediately execute Step 1 with ONE SQL query.
2. **Each subsequent turn**: you receive the actual query results. Read and interpret them. Then either:
   - Run the next SQL query to dig deeper
   - Adapt your plan if the data reveals something unexpected
   - Provide analysis, summary, or insights based on what you found
   - Ask a follow-up query if needed to answer the goal fully
3. **Final response** (the last plan step): when all data steps are done, write a thorough assessment — cite specific numbers, identify patterns, draw conclusions grounded in the data you gathered. Only AFTER writing the full assessment, end with \`DONE\` on its own line.

### SQL Rules (CRITICAL)
- Output EXACTLY ONE \`\`\`sql ... \`\`\` block per response
- The opening fence and closing fence MUST each be on their own line
- NEVER write \`\`\` immediately after SQL text on the same line
- ALL parentheses must be balanced before the closing fence
- After the closing \`\`\`, stop — wait for results before continuing

### Reasoning
- Treat each result as real evidence — comment on patterns, anomalies, or gaps you see
- If a result is empty or unexpected, explain why and adapt
- The final assessment MUST cite specific numbers from the data, not generic statements
- Never say DONE without first writing the complete assessment`
        : ''

      // Build messages for the API — done BEFORE setMessages so we can attach to userMsg
      const apiMessages: { role: string; content: string }[] = []
      if (skillPrompt || suggestionsInstruction || agenticInstruction) {
        apiMessages.push({ role: 'system', content: (skillPrompt ?? '') + suggestionsInstruction + agenticInstruction })
      }
      if (fileContext) apiMessages.push({ role: 'system', content: fileContext })

      // Active connection context
      if (opts?.activeConnections && opts.activeConnections.length > 0) {
        const connLines = opts.activeConnections.map((c) => {
          const safeName = c.name.toLowerCase().replace(/[^a-z0-9]/g, '_')

          // DB extensions: ATTACHed → query via alias.schema.table
          const isDbAttach = ['postgres', 'mysql', 'sqlite'].includes(c.ext_type)
          // Path-based extensions: loaded but NOT attached → queried via scan function
          const isPathScan = ['delta', 'iceberg'].includes(c.ext_type)

          let queryHint: string
          if (isDbAttach && c.connection_string) {
            queryHint = `Attached as \`${safeName}\` — query with: SELECT * FROM ${safeName}.<schema>.<table>`
          } else if (isPathScan && c.connection_string) {
            const scanFn = c.ext_type === 'delta' ? 'delta_scan' : 'iceberg_scan'
            // Build Azure credential SET statements if configured
            let credBlock = ''
            if (c.azure_auth === 'service_principal' && c.azure_tenant_id && c.azure_client_id && c.azure_client_secret) {
              credBlock = [
                `  BEFORE querying, you MUST execute these credential statements first (as separate SQL):`,
                `  SET azure_tenant_id='${c.azure_tenant_id}';`,
                `  SET azure_client_id='${c.azure_client_id}';`,
                `  SET azure_client_secret='${c.azure_client_secret}';`,
              ].join('\n')
            } else if (c.azure_auth === 'account_key' && c.azure_storage_connection_string) {
              credBlock = [
                `  BEFORE querying, you MUST execute this credential statement first:`,
                `  SET azure_storage_connection_string='${c.azure_storage_connection_string}';`,
              ].join('\n')
            } else if (c.azure_auth === 'sas' && c.azure_storage_connection_string) {
              credBlock = [
                `  BEFORE querying, you MUST execute this credential statement first:`,
                `  SET azure_storage_connection_string='${c.azure_storage_connection_string}';`,
              ].join('\n')
            } else if (c.azure_auth === 'azure_cli') {
              credBlock = `  BEFORE querying, you MUST execute: SET azure_use_azure_cli=true;`
            }
            queryHint = [
              `Use: SELECT * FROM ${scanFn}('${c.connection_string}')`,
              `  The extension is already loaded — use this exact path in ${scanFn}().`,
              credBlock || '',
              credBlock ? `  Always include the credential SET statement(s) as separate SQL statements BEFORE the ${scanFn}() query.` : '',
            ].filter(Boolean).join('\n')
          } else {
            queryHint = `Extension \`${c.ext_type}\` is loaded — use its native functions directly`
          }

          return `- **${c.name}** (${c.ext_type})\n  ${queryHint}${c.description ? `\n  Note: ${c.description}` : ''}`
        }).join('\n')
        apiMessages.push({
          role: 'system',
          content: `## Active Connections / Extensions\nThe following are pre-loaded in DuckDB for this query:\n${connLines}`,
        })
      }

      // Include previous messages, capped by context_history_limit (0 = all)
      // Skip: empty assistant messages, and error messages — they waste tokens and confuse local models
      const limit = settings.context_history_limit ?? 20
      const historyMsgs = limit > 0 ? messages.slice(-limit) : messages
      for (const m of historyMsgs) {
        const isEmptyAssistant = m.role === 'assistant' && m.content.trim() === ''
        const isErrorMsg = m.role === 'assistant' && m.content.startsWith('**Error:**')
        if (isEmptyAssistant || isErrorMsg) continue
        apiMessages.push({ role: m.role, content: m.content })
      }
      // Inject a path reminder system message immediately before the user turn so the LLM
      // always has the exact paths fresh in context regardless of conversation length.
      if (allFiles.length > 0) {
        const pathLines = allFiles.map((f) => `  • ${f.path}`).join('\n')
        apiMessages.push({
          role: 'system',
          content: `## File Path Reminder\nThe following exact paths are loaded — copy them character-for-character, never guess or paraphrase:\n${pathLines}`,
        })
      }
      apiMessages.push({ role: 'user', content: content })

      const now = Date.now()
      const userMsg: ChatMessage = {
        id: nextId(), role: 'user', content, timestamp: now,
        attachedFiles: newAttachments,
        sentContext: apiMessages,
        agenticContinuation: opts?.agenticContinuation,
        agenticRunId: opts?.agenticRunId,
      }
      const assistantMsg: ChatMessage = {
        id: nextId(), role: 'assistant', content: '', timestamp: now + 1,
        agenticRunId: opts?.agenticRunId,
        isAutoProfile: opts?.isAutoProfile,
      }

      setMessages((prev) => [...prev, userMsg, assistantMsg])
      setIsStreaming(true)
      streamingRef.current = ''
      const requestStart = Date.now()

      const streamId = `stream-${Date.now()}-${Math.random().toString(36).slice(2)}`

      // Determine whether to send tools for this request
      const toolCallDepth = opts?.toolCallDepth ?? 0
      const MAX_TOOL_CALL_DEPTH = 8
      const useTools = toolCallDepth < MAX_TOOL_CALL_DEPTH &&
        settings.tool_calling_enabled !== false &&
        shouldSendTools(provider, providerCfg.model)

      // Helper: run a single streaming turn and resolve when llm-end fires.
      // Appends chunks to the current last assistant message.
      // Returns the accumulated tool calls (if any) for continuation.
      const runStreamTurn = (
        turnMessages: object[],
        turnStreamId: string,
        sendTools: boolean,
      ): Promise<{ toolCalls: Array<{ id: string; name: string; arguments: string }>; assistantText: string } | null> => {
        return new Promise<{ toolCalls: Array<{ id: string; name: string; arguments: string }>; assistantText: string } | null>((resolve, reject) => {
          let unlistenChunk: (() => void) | null = null
          let unlistenEnd: (() => void) | null = null
          let unlistenToolCalls: (() => void) | null = null
          let toolCallPayload: { toolCalls: Array<{ id: string; name: string; arguments: string }>; assistantText: string } | null = null

          const cleanup = () => {
            unlistenChunk?.()
            unlistenEnd?.()
            unlistenToolCalls?.()
          }

          listen<{ stream_id: string; chunk: string }>('llm-chunk', (event) => {
            if (event.payload.stream_id !== turnStreamId) return
            streamingRef.current += event.payload.chunk
            setMessages((prev) => {
              const updated = [...prev]
              const last = updated[updated.length - 1]
              if (last?.role === 'assistant') {
                updated[updated.length - 1] = { ...last, content: streamingRef.current }
              }
              return updated
            })
          }).then((u) => { unlistenChunk = u })

          listen<{ stream_id: string; tool_calls: Array<{ id: string; name: string; arguments: string }>; assistant_text: string }>('llm-tool-calls', (event) => {
            if (event.payload.stream_id !== turnStreamId) return
            cacheToolCallSupport(provider, providerCfg.model, true)
            toolCallPayload = { toolCalls: event.payload.tool_calls, assistantText: event.payload.assistant_text }

            // Mark running chips on current assistant message
            const runningRecords: ToolCallRecord[] = event.payload.tool_calls.map((tc) => ({
              id: tc.id || `tc-${Date.now()}-${Math.random().toString(36).slice(2)}`,
              name: tc.name,
              arguments: (() => { try { return JSON.parse(tc.arguments) } catch { return {} } })(),
              status: 'running' as const,
            }))
            setMessages((prev) => {
              const updated = [...prev]
              const last = updated[updated.length - 1]
              if (last?.role === 'assistant') {
                const existing = last.toolCalls ?? []
                updated[updated.length - 1] = { ...last, toolCalls: [...existing, ...runningRecords] }
              }
              return updated
            })
          }).then((u) => { unlistenToolCalls = u })

          listen<{ stream_id: string; error: string | null; tokens_in?: number; tokens_out?: number }>('llm-end', (event) => {
            if (event.payload.stream_id !== turnStreamId) return
            cleanup()
            if (event.payload.error) {
              reject(new Error(event.payload.error))
            } else {
              resolve(toolCallPayload)
            }
          }).then((u) => { unlistenEnd = u })

          invoke('stream_llm', {
            args: {
              provider,
              api_key: providerCfg.api_key,
              model: providerCfg.model,
              temperature: providerCfg.temperature,
              messages: turnMessages,
              stream_id: turnStreamId,
              ...(provider === 'ollama'
                ? { base_url: settings.ollama.base_url }
                : providerCfg.base_url
                  ? { base_url: providerCfg.base_url }
                  : {}),
              ...(settings.max_output_tokens != null
                ? { max_tokens: settings.max_output_tokens }
                : {}),
              ...(sendTools ? { tools: getToolSchemas() } : {}),
            },
          }).catch((err) => {
            cleanup()
            reject(new Error(String(err)))
          })
        })
      }

      try {
        // Outer promise wraps the full agentic tool-call loop
        await new Promise<void>(async (resolve, reject) => {
          let cancelled = false
          stopStreamRef.current = () => { cancelled = true; resolve() }

          try {
            let currentMessages: object[] = apiMessages
            let currentDepth = toolCallDepth
            let allToolCalls: ToolCallRecord[] = []

            while (currentDepth < MAX_TOOL_CALL_DEPTH && !cancelled) {
              const turnStreamId = currentDepth === toolCallDepth
                ? streamId
                : `stream-${Date.now()}-${Math.random().toString(36).slice(2)}`

              const sendToolsThisTurn = currentDepth < MAX_TOOL_CALL_DEPTH &&
                settings.tool_calling_enabled !== false &&
                shouldSendTools(provider, providerCfg.model)

              const toolCallPayload = await runStreamTurn(currentMessages, turnStreamId, sendToolsThisTurn)

              if (!toolCallPayload || toolCallPayload.toolCalls.length === 0) {
                // No tool calls — stamp final content and break
                const duration_ms = Date.now() - requestStart

                // We need to get current tokens — re-listen to llm-end was done inside runStreamTurn
                // but we didn't capture tokens_in/tokens_out. For now pass undefined.
                setMessages((prev) => {
                  const updated = [...prev]
                  const last = updated[updated.length - 1]
                  if (last?.role === 'assistant') {
                    const raw = last.content
                    const suggestionsMatch =
                      raw.match(/\n?<suggestions>([\s\S]*?)<\/suggestions>/) ??
                      raw.match(/\n?<suggestions>([\s\S]*)$/)
                    let displayContent = raw
                    let suggestions: string[] | undefined
                    if (suggestionsMatch) {
                      displayContent = raw.replace(suggestionsMatch[0], '').trimEnd()
                      const inner = suggestionsMatch[1].trim()
                      try {
                        const parsed = JSON.parse(inner)
                        if (Array.isArray(parsed)) suggestions = parsed.map(String).filter(Boolean).slice(0, 5)
                      } catch {
                        const arrMatch = inner.match(/\[[\s\S]*?\]/)
                        if (arrMatch) {
                          try {
                            const parsed = JSON.parse(arrMatch[0])
                            if (Array.isArray(parsed)) suggestions = parsed.map(String).filter(Boolean).slice(0, 5)
                          } catch { /* still malformed */ }
                        }
                        if (!suggestions) {
                          const chips = [...inner.matchAll(/"([^"]+)"/g)].map((m) => m[1]).filter(Boolean)
                          if (chips.length > 0) suggestions = chips.slice(0, 5)
                        }
                      }
                    }
                    const finalContent = splitMultiStatementBlocks(displayContent)
                    updated[updated.length - 1] = {
                      ...last,
                      content: finalContent,
                      duration_ms,
                      suggestions,
                    }
                  }
                  return updated
                })
                break
              }

              // Execute tool calls and update chips
              const rawToolCalls = toolCallPayload.toolCalls
              const assistantText = toolCallPayload.assistantText

              const completedRecords: ToolCallRecord[] = []
              for (const tc of rawToolCalls) {
                const tcId = tc.id || `tc-${Date.now()}-${Math.random().toString(36).slice(2)}`
                const parsedArgs: Record<string, unknown> = (() => { try { return JSON.parse(tc.arguments) } catch { return {} } })()
                const start = Date.now()
                const result = await executeToolCall(tc.name, parsedArgs)
                const duration_ms = Date.now() - start
                const isError = result && typeof result === 'object' && (result as any).success === false
                completedRecords.push({
                  id: tcId,
                  name: tc.name,
                  arguments: parsedArgs,
                  result,
                  duration_ms,
                  status: isError ? 'error' : 'success',
                })
              }
              allToolCalls = [...allToolCalls, ...completedRecords]

              // Update the assistant message with completed tool call records
              setMessages((prev) => {
                const updated = [...prev]
                const last = updated[updated.length - 1]
                if (last?.role === 'assistant') {
                  // Replace running records with completed ones (match by name order)
                  const existingCompleted = (last.toolCalls ?? []).filter((tc) => tc.status !== 'running')
                  updated[updated.length - 1] = { ...last, toolCalls: [...existingCompleted, ...completedRecords] }
                }
                return updated
              })

              // Build continuation messages
              const continuationToolCalls = completedRecords.map((tc) => ({
                id: tc.id,
                type: 'function' as const,
                function: { name: tc.name, arguments: JSON.stringify(tc.arguments) },
              }))
              const assistantToolMsg = {
                role: 'assistant',
                content: assistantText || '',
                tool_calls: continuationToolCalls,
              }
              const toolResultMsgs = completedRecords.map((tc) => ({
                role: 'tool',
                content: JSON.stringify(tc.result),
                tool_call_id: tc.id,
              }))

              currentMessages = [...currentMessages, assistantToolMsg, ...toolResultMsgs]
              currentDepth += 1
              // Reset streaming accumulator for the next turn (content appended onto same message)
              // We do NOT reset streamingRef — we want to APPEND to the existing content
            }

            stopStreamRef.current = null
            resolve()
          } catch (err) {
            stopStreamRef.current = null
            reject(err)
          }
        })
      } catch (err: any) {
        setMessages((prev) => {
          const updated = [...prev]
          const last = updated[updated.length - 1]
          if (last?.role === 'assistant') {
            updated[updated.length - 1] = { ...last, content: `**Error:** ${err.message}` }
          }
          return updated
        })
      } finally {
        setIsStreaming(false)
      }
    },
    [messages, settings, loadedFiles, addFiles, engine],
  )

  const clearMessages = useCallback(() => {
    dbClearMessages().catch(console.error)
    dbClearFiles().catch(console.error)
    setMessagesState([])
    setLoadedFilesState([])
  }, [])

  /** Update fields on the last assistant message (e.g. strip DONE, store plan steps).
   *  Writes to DB immediately — bypasses the debounce — so the patched state
   *  is always persisted before the next render cycle. */
  const patchLastMessage = useCallback((patch: Partial<ChatMessage>) => {
    setMessagesState((prev) => {
      const updated = [...prev]
      const last = updated[updated.length - 1]
      if (last?.role === 'assistant') {
        const patched = { ...last, ...patch }
        updated[updated.length - 1] = patched
        dbSaveMessage(patched).catch(console.error)   // immediate, no debounce
      }
      return updated
    })
  }, [])

  /** Hide the last assistant message by marking it as an agentic continuation (not shown in UI).
   *  Writes to DB immediately for the same reason as patchLastMessage. */
  const hideLastMessage = useCallback(() => {
    setMessagesState((prev) => {
      const updated = [...prev]
      const last = updated[updated.length - 1]
      if (last?.role === 'assistant') {
        const hidden = { ...last, agenticContinuation: true }
        updated[updated.length - 1] = hidden
        dbSaveMessage(hidden).catch(console.error)    // immediate, no debounce
      }
      return updated
    })
  }, [])

  const stopStream = useCallback(() => {
    if (stopStreamRef.current) {
      stopStreamRef.current()
      setIsStreaming(false)
    }
  }, [])

  return { messages, sendMessage, isStreaming, stopStream, addQueryResult, clearMessages, patchLastMessage, hideLastMessage, loadedFiles, addFiles, replaceFile, removeFile }
}

