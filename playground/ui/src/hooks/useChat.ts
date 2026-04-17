import { useState, useCallback, useRef, useEffect } from 'react'
import { appLog } from '../lib/appLog'
import { invoke } from '@tauri-apps/api/core'
import { listen } from '@tauri-apps/api/event'
import type { ChatMessage, AppSettings, AttachedFile, QueryResult, ConnectionAlias, ToolCallRecord, QueryHistoryEntry } from '../lib/types'
import { resolveSkillPrompts, ENGINE_SKILL_IDS } from '../lib/skills'
import { readExpr, readerForPath } from '../lib/readExpr'
import {
  dbLoadMessages, dbSaveMessage, dbClearMessages,
  dbLoadFiles, dbSaveFile, dbDeleteFile, dbClearFiles,
  dbAddHistoryEntry,
  migrateFromLocalStorage,
} from '../lib/db'
import { getToolSchemas } from '../lib/llm/toolSchemas'
import { shouldSendTools, cacheToolCallSupport } from '../lib/llm/toolCallSupport'
import { executeToolCall } from '../lib/tools/executeToolCall'
import { toAlias } from '../lib/fileAlias'

export type Engine = 'blazer' | 'duckdb'

let msgCounter = 0
const nextId = () => `msg-${++msgCounter}-${Date.now()}`

// ── Strip Ollama special tokens from displayed content ────────────────────────
// Ollama emits tool calls both as structured tool_calls objects AND as raw text
// tokens like <|tool_call>call:name{...}<tool_call|> in the content stream.
// Strip them so users never see raw model internals.
function stripModelTokens(text: string): string {
  // Remove <|tool_call>...<tool_call|> blocks (may span multiple lines)
  let clean = text.replace(/<\|tool_call\>[\s\S]*?<tool_call\|>/g, '')
  // Remove any remaining <|...|> special tokens (e.g. <|"|>, <|eot_id|>)
  clean = clean.replace(/<\|[^|>]*\|>/g, '')
  return clean
}

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

// ── Tool result truncation ─────────────────────────────────────────────────────
// Local models (Ollama, MLX) often have small context windows (4k–8k tokens).
// Sending 100-row SQL results verbatim for every tool call eats the entire budget,
// leaving 0–1 tokens for the model's response.
//
// Truncation philosophy:
//   • Aggregation queries (GROUP BY, COUNT, SUM…) naturally return few rows (5–30).
//     These are the real analysis results and are never truncated (threshold = 40).
//   • Raw row dumps (SELECT * LIMIT 100) return large result sets the model doesn't
//     need row-by-row — it should use SQL to aggregate, not scan context.
//     These are trimmed to ROWS_HARD_LIMIT with a note to run a tighter query.
//   • describe_tables / column_stats are already compact and pass through unchanged.
//
// The full result is always visible in the chip expand panel for the user.
const ROWS_FREE    = 40   // send in full — covers most aggregations
const ROWS_PARTIAL = 20   // trim to this when result is large

function buildToolResultContent(tc: { name: string; result?: unknown }): string {
  if (!tc.result || typeof tc.result !== 'object') return JSON.stringify(tc.result)
  const r = tc.result as Record<string, unknown>

  if (tc.name === 'run_sql' || tc.name === 'get_sample_rows') {
    const data = r['data']
    if (Array.isArray(data) && data.length > ROWS_FREE) {
      const truncated = {
        ...r,
        data: data.slice(0, ROWS_PARTIAL),
        _note: `${data.length - ROWS_PARTIAL} rows not shown to save context. `
             + `The full result is visible in the UI. `
             + `If you need aggregate statistics on this data, run a GROUP BY / COUNT / AVG query instead of reading raw rows.`,
      }
      return JSON.stringify(truncated)
    }
  }

  if (tc.name === 'describe_tables') {
    const tables = r['tables']
    if (Array.isArray(tables)) {
      const trimmed = tables.map((t: unknown) => {
        if (!t || typeof t !== 'object') return t
        const tbl = t as Record<string, unknown>
        const cols = tbl['columns']
        if (Array.isArray(cols) && cols.length > 60) {
          return { ...tbl, columns: cols.slice(0, 60), _note: `${cols.length - 60} more columns not shown` }
        }
        return tbl
      })
      return JSON.stringify({ ...r, tables: trimmed })
    }
  }

  return JSON.stringify(tc.result)
}

export function useChat(
  settings: AppSettings,
  engine: Engine = 'blazer',
  /** Optional callback to record a new history entry — updates both React state and DB live. */
  onHistoryEntry?: (entry: Omit<QueryHistoryEntry, 'id'>) => void,
) {
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
      .then((files) => {
        // Ensure each persisted file has an alias (views are recreated per-connection by Rust).
        const withAliases = files.map((f) => f.alias ? f : { ...f, alias: toAlias(f) })
        setLoadedFilesState(withAliases)
      })
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

      // Assign aliases synchronously — Rust recreates the VIEW per-connection
      // using the alias + reader expression passed with every tool call.
      const withAliases = toAdd.map((f) =>
        f.alias ? f : { ...f, alias: toAlias(f) }
      )

      withAliases.forEach((f) => dbSaveFile(f).catch(console.error))
      withAliases.forEach((f) => appLog.info('file', `File attached: ${f.name}`, { path: f.path, ext: f.ext }))

      return [...prev, ...withAliases]
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

      // Optionally inject follow-up suggestions instruction (all providers including Ollama —
      // modern local models handle the format fine, and a parse failure is silent/graceful)
      const showChips = settings.show_follow_up_chips !== false
      const suggestionsInstruction = showChips
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

      // File context — MUST be placed here (before history) so all system messages come first.
      // Placing a system message AFTER user/assistant history confuses Ollama/local models.
      //
      // DuckDB engine: rich metadata block with exact reader expressions, schema, and row counts.
      //   This supersedes the legacy per-engine fileContext — only one injection is needed.
      // Blazer engine: simple path+type block (Blazer uses JSON query plans, not SQL readers).
      if (allFiles.length > 0) {
        if (engine === 'duckdb') {
          const fileBlocks = allFiles.map((f) => {
            const reader = f.readerExpr ?? readExpr(f)
            const lines: string[] = [
              // Reader expression first — copy it exactly into FROM / DESCRIBE / etc.
              `Reader: ${reader}  ← USE THIS EXACTLY in SQL (e.g. SELECT * FROM ${reader})`,
              `Path: ${f.path}`,
            ]
            if (f.rowCount != null) lines.push(`Rows: ${f.rowCount.toLocaleString()}`)
            if (f.columns && f.columns.length > 0) {
              const colList = f.columns.map((c) => {
                const t = f.columnTypes?.[c]
                return t ? `${c} (${t})` : c
              }).join(', ')
              lines.push(`Columns (${f.columns.length}): ${colList}`)
            } else {
              lines.push('Schema: not yet profiled — run describe_tables first')
            }
            if (f.ext === 'xlsx' || f.ext === 'xlsx_dir') {
              lines.push('NOTE: all_varchar=true — use TRY_CAST(col AS DOUBLE) for numeric ops')
            }
            return lines.map((l, i) => (i === 0 ? `• ${l}` : `  ${l}`)).join('\n')
          }).join('\n\n')

          apiMessages.push({
            role: 'system',
            content: `## Attached Data Files\nCopy the reader expression EXACTLY as shown — do NOT retype the path, do not shorten it, do not invent a table name.\n\n${fileBlocks}`,
          })
        } else {
          // Blazer engine — simple path + source type
          let blazerCtx = '## Attached Data Files\nUse EXACTLY these absolute paths in your query JSON:\n'
          for (const f of allFiles) {
            const kind =
              f.ext === 'csv' || f.ext === 'tsv' || f.ext === 'csv_dir' ? 'csv' :
              f.ext === 'parquet_dir' ? 'parquet_dir' : 'parquet'
            blazerCtx += `- Path: \`${f.path}\` → source type: \`"${kind}"\`\n`
            if (f.columns && f.columns.length > 0) {
              blazerCtx += `  Columns: ${f.columns.map((c) => `\`${c}\``).join(', ')}\n`
            }
          }
          apiMessages.push({ role: 'system', content: blazerCtx })
        }
      }

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
            const displayContent = stripModelTokens(streamingRef.current)
            setMessages((prev) => {
              const updated = [...prev]
              const last = updated[updated.length - 1]
              if (last?.role === 'assistant') {
                updated[updated.length - 1] = { ...last, content: displayContent }
              }
              return updated
            })
          }).then((u) => { unlistenChunk = u })

          listen<{ stream_id: string; tool_calls: Array<{ id: string; name: string; arguments: string }>; assistant_text: string }>('llm-tool-calls', (event) => {
            if (event.payload.stream_id !== turnStreamId) return
            cacheToolCallSupport(provider, providerCfg.model, true)

            // Assign stable IDs once here so running chips and execution records share the same ID.
            const batchBase = `tc-${Date.now()}`
            const assignedToolCalls = event.payload.tool_calls.map((tc, idx) => ({
              ...tc,
              id: tc.id || `${batchBase}-${idx}`,
            }))
            toolCallPayload = { toolCalls: assignedToolCalls, assistantText: event.payload.assistant_text }

            // Mark running chips on current assistant message
            const runningRecords: ToolCallRecord[] = assignedToolCalls.map((tc) => ({
              id: tc.id,
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

          appLog.info('llm', `Sending to ${provider} · ${providerCfg.model}`, { messages: turnMessages.length, content_preview: (typeof (turnMessages[turnMessages.length - 1] as any)?.content === 'string' ? (turnMessages[turnMessages.length - 1] as any).content as string : '').slice(0, 120) })

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
                // No tool calls — stamp final content and break.
                // If this was the first turn and we sent tools but got plain text
                // back, the model doesn't support tool calling. Cache it so we
                // don't keep sending schemas for the rest of the session.
                if (sendToolsThisTurn && currentDepth === toolCallDepth) {
                  cacheToolCallSupport(provider, providerCfg.model, false)
                }

                // Synthesis turn: if the model ran tools but produced no text,
                // fire one extra text-only turn to force it to write the response.
                // This handles models (especially GPT-4o) that stop after tool calls
                // without ever writing a user-visible answer.
                if (
                  !cancelled &&
                  allToolCalls.length > 0 &&
                  !stripModelTokens(streamingRef.current).trim()
                ) {
                  const synthStreamId = `stream-synth-${Date.now()}-${Math.random().toString(36).slice(2)}`
                  // Fresh accumulator so only the synthesis text appears in the bubble
                  streamingRef.current = ''
                  const synthContent = opts?.isAutoProfile
                    ? 'Based on all the tool results above, write the data profile summary now. Do not call any more tools — write the analysis as plain text: what the data represents, row/column counts, key columns, data quality issues, and 3 analytical questions. 4-6 paragraphs, cite specific numbers.'
                    : 'You have the SQL results above. Now write your analysis in plain text — do NOT call any more tools. Answer the user\'s question directly with specific numbers from the results. Highlight key findings, patterns, or anomalies. Be concise and specific.'
                  await runStreamTurn(
                    [
                      ...currentMessages,
                      {
                        role: 'user' as const,
                        content: synthContent,
                      },
                    ],
                    synthStreamId,
                    false, // no tools — text-only turn
                  )
                }

                const duration_ms = Date.now() - requestStart

                appLog.info('llm', 'Response received', { duration_ms, chars: streamingRef.current.length })

                // We need to get current tokens — re-listen to llm-end was done inside runStreamTurn
                // but we didn't capture tokens_in/tokens_out. For now pass undefined.
                setMessages((prev) => {
                  const updated = [...prev]
                  const last = updated[updated.length - 1]
                  if (last?.role === 'assistant') {
                    const raw = stripModelTokens(last.content)
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

              // Execute tool calls — run all in parallel so chips complete as fast
              // as possible and each chip flips to success/error individually.
              const rawToolCalls = toolCallPayload.toolCalls
              const assistantText = toolCallPayload.assistantText

              const completedRecords: ToolCallRecord[] = await Promise.all(
                rawToolCalls.map(async (tc) => {
                  // IDs were already assigned in the llm-tool-calls event handler
                  const tcId = tc.id
                  const parsedArgs: Record<string, unknown> = (() => {
                    try { return JSON.parse(tc.arguments) } catch { return {} }
                  })()
                  appLog.info('tool', `Tool call: ${tc.name}`, { args: JSON.stringify(parsedArgs).slice(0, 200) })
                  const start = Date.now()
                  const result = await executeToolCall(tc.name, parsedArgs, allFiles)
                  const duration_ms = Date.now() - start
                  appLog.info('tool', `Tool result: ${tc.name}`, { result: JSON.stringify(result).slice(0, 200) })
                  const isError = result && typeof result === 'object' && (result as any).success === false

                  const record: ToolCallRecord = {
                    id: tcId,
                    name: tc.name,
                    arguments: parsedArgs,
                    result,
                    duration_ms,
                    status: isError ? 'error' : 'success',
                  }

                  // Flip this chip to done immediately — don't wait for siblings
                  setMessages((prev) => {
                    const updated = [...prev]
                    const last = updated[updated.length - 1]
                    if (last?.role === 'assistant' && last.toolCalls) {
                      updated[updated.length - 1] = {
                        ...last,
                        toolCalls: last.toolCalls.map((c) => c.id === tcId ? record : c),
                      }
                    }
                    return updated
                  })

                  // Log SQL-producing tool calls to query history
                  if (result && typeof result === 'object') {
                    const r = result as Record<string, unknown>
                    const sql = (r['sql_executed'] ?? r['sql_attempted']) as string | undefined
                    if (sql) {
                      const rowCount = (r['row_count'] as number | undefined) ?? 0
                      const cols     = Array.isArray(r['columns']) ? (r['columns'] as unknown[]).length : 0
                      const errorMsg = r['error'] as string | undefined
                      const historyEntry: Omit<QueryHistoryEntry, 'id'> = {
                        engine:      'duckdb',
                        query:       sql,
                        timestamp:   start,
                        success:     !isError,
                        duration_ms,
                        rows:        rowCount,
                        cols,
                        error:       errorMsg,
                      }
                      if (onHistoryEntry) {
                        onHistoryEntry(historyEntry)
                      } else {
                        dbAddHistoryEntry({ id: `tool-${tcId}`, ...historyEntry }).catch(console.error)
                      }
                    }
                  }

                  return record
                }),
              )
              allToolCalls = [...allToolCalls, ...completedRecords]

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
                // Truncated to avoid filling the context window on small local models
                content: buildToolResultContent(tc),
                tool_call_id: tc.id,
              }))

              currentMessages = [...currentMessages, assistantToolMsg, ...toolResultMsgs]
              currentDepth += 1
              // Reset streaming accumulator for the next turn (content appended onto same message)
              // We do NOT reset streamingRef — we want to APPEND to the existing content
            }

            // ── Metadata caching from tool results ─────────────────────────────
            // Runs after EVERY agentic turn (not just auto-profile) so schema,
            // column types, and row counts are always persisted onto the
            // AttachedFile objects and injected into subsequent turns.
            if (allToolCalls.length > 0) {
              // Accumulate per-path updates
              const pathToColumns  = new Map<string, string[]>()
              const pathToTypes    = new Map<string, Record<string, string>>()
              const pathToRowCount = new Map<string, number>()
              // When describe_tables returns a specific file path that lives inside
              // a loaded folder, store the correct reader so the folder's default
              // (e.g. read_parquet) is overridden with the actual type (e.g. read_json_auto).
              const pathToReaderExpr = new Map<string, string>()

              for (const tc of allToolCalls) {
                const r = tc.result as Record<string, unknown> | null
                if (!r || r['success'] === false) continue

                if (tc.name === 'describe_tables') {
                  // { tables: [{name, columns: [{name, type}]}] }
                  const tables = r['tables'] as Array<{
                    name: string
                    columns: Array<{ name: string; type: string }>
                  }> | undefined
                  if (Array.isArray(tables)) {
                    for (const t of tables) {
                      if (!t.name || !Array.isArray(t.columns) || t.columns.length === 0) continue
                      pathToColumns.set(t.name, t.columns.map((c) => c.name))
                      const types: Record<string, string> = {}
                      for (const c of t.columns) types[c.name] = c.type ?? ''
                      pathToTypes.set(t.name, types)

                      // If t.name is a file INSIDE a loaded folder (not the folder itself),
                      // derive the correct reader from the file's extension and store it
                      // on the parent folder so future turns use the right reader.
                      const parentFolder = allFiles.find(
                        (f) => f.path !== t.name && t.name.startsWith(f.path),
                      )
                      if (parentFolder) {
                        // Only override if it differs from the current reader to avoid noise
                        const derived = readerForPath(t.name)
                        const current = parentFolder.readerExpr ?? readExpr(parentFolder)
                        if (derived !== current) pathToReaderExpr.set(parentFolder.path, derived)
                        // Also map columns/types to the folder (so it gets the schema too)
                        pathToColumns.set(parentFolder.path, t.columns.map((c) => c.name))
                        pathToTypes.set(parentFolder.path, types)
                      }
                    }
                  }

                } else if (tc.name === 'column_stats') {
                  // { table, column_profiles: [{column, total_count, ...}] }
                  const tablePath = String(r['table'] ?? tc.arguments['table'] ?? '')
                  const profiles = r['column_profiles'] as Array<{ column: string; total_count?: number }> | undefined
                  if (Array.isArray(profiles) && profiles.length > 0) {
                    const totalCount = profiles[0].total_count
                    if (totalCount != null && tablePath) {
                      const match = allFiles.find(
                        (f) => tablePath === f.path || tablePath.includes(f.path) || f.path.includes(tablePath),
                      )
                      if (match) pathToRowCount.set(match.path, totalCount)
                    }
                    // Extract column names if not already from describe
                    if (tablePath && !pathToColumns.has(tablePath)) {
                      const match = allFiles.find(
                        (f) => tablePath === f.path || tablePath.includes(f.path) || f.path.includes(tablePath),
                      )
                      if (match) {
                        pathToColumns.set(match.path, profiles.map((p) => p.column))
                      }
                    }
                  }

                } else if (tc.name === 'get_sample_rows' || tc.name === 'run_sql') {
                  // { columns: ["col1", ...], row_count?, ... }
                  const cols = r['columns'] as string[] | undefined
                  const rowCount = r['row_count'] as number | undefined
                  const tablePath = String(
                    tc.arguments['table'] ?? tc.arguments['sql'] ?? '',
                  )
                  // Match against a loaded file by path substring
                  const match = allFiles.find(
                    (f) => tablePath.includes(f.path) || f.path.includes(tablePath),
                  )
                  if (match) {
                    if (Array.isArray(cols) && cols.length > 0) pathToColumns.set(match.path, cols)
                    if (rowCount != null) pathToRowCount.set(match.path, rowCount)
                  }
                }
              }

              // Merge discovered metadata onto AttachedFile objects
              const hasUpdates = pathToColumns.size > 0 || pathToTypes.size > 0 || pathToRowCount.size > 0 || pathToReaderExpr.size > 0
              if (hasUpdates) {
                setLoadedFiles((prev) =>
                  prev.map((f) => {
                    let updated = f
                    const cols       = pathToColumns.get(f.path)
                    const types      = pathToTypes.get(f.path)
                    const rowCount   = pathToRowCount.get(f.path)
                    const readerExpr = pathToReaderExpr.get(f.path)
                    if (cols && cols.length > 0)                  updated = { ...updated, columns: cols }
                    if (types && Object.keys(types).length > 0)   updated = { ...updated, columnTypes: types }
                    if (rowCount != null)                          updated = { ...updated, rowCount }
                    if (readerExpr)                                updated = { ...updated, readerExpr }
                    if (updated !== f) dbSaveFile(updated).catch(console.error)
                    return updated
                  }),
                )
              }
            }

            stopStreamRef.current = null
            resolve()
          } catch (err) {
            stopStreamRef.current = null
            reject(err)
          }
        })
      } catch (err: any) {
        appLog.error('llm', `LLM error: ${String(err)}`)
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

// Exported for testing only
export { extractSqlStatements, normalizeFenceMarkers, sqlLooksIncomplete, splitMultiStatementBlocks }

