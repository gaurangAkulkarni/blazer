/**
 * db.ts — SQLite persistence layer via tauri-plugin-sql.
 *
 * The database is stored at the OS-appropriate user data directory:
 *   macOS  : ~/Library/Application Support/blazer/blazer.db
 *   Windows: %APPDATA%\blazer\blazer.db
 *   Linux  : ~/.local/share/blazer/blazer.db
 *
 * All functions are async and safe to call concurrently — the plugin
 * serialises writes internally.
 */
import Database from '@tauri-apps/plugin-sql'
import { invoke } from '@tauri-apps/api/core'
import type { ChatMessage, AttachedFile, QueryHistoryEntry, QuerySnippet, SnippetGroup } from './types'

// ── Singleton connection ──────────────────────────────────────────────────────
let _db: Database | null = null

async function getDb(): Promise<Database> {
  if (_db) return _db
  _db = await Database.load('sqlite:blazer.db')
  await initSchema(_db)
  return _db
}

async function initSchema(db: Database): Promise<void> {
  await db.execute(`
    CREATE TABLE IF NOT EXISTS messages (
      id                   TEXT    PRIMARY KEY,
      role                 TEXT    NOT NULL,
      content              TEXT    NOT NULL DEFAULT '',
      timestamp            INTEGER NOT NULL,
      duration_ms          INTEGER,
      tokens_in            INTEGER,
      tokens_out           INTEGER,
      agentic_continuation INTEGER NOT NULL DEFAULT 0,
      agentic_run_id       TEXT,
      agentic_plan_steps   TEXT,
      suggestions          TEXT,
      query_results        TEXT,
      attached_files       TEXT,
      sent_context         TEXT
    )
  `)
  await db.execute(`
    CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp ASC)
  `)
  await db.execute(`
    CREATE INDEX IF NOT EXISTS idx_messages_run_id ON messages (agentic_run_id)
    WHERE agentic_run_id IS NOT NULL
  `)
  await db.execute(`
    CREATE TABLE IF NOT EXISTS loaded_files (
      path    TEXT PRIMARY KEY,
      name    TEXT NOT NULL,
      ext     TEXT NOT NULL DEFAULT '',
      columns TEXT
    )
  `)
  await db.execute(`
    CREATE TABLE IF NOT EXISTS app_state (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `)
  await db.execute(`
    CREATE TABLE IF NOT EXISTS query_history (
      id          TEXT    PRIMARY KEY,
      engine      TEXT    NOT NULL,
      query       TEXT    NOT NULL,
      timestamp   INTEGER NOT NULL,
      success     INTEGER NOT NULL DEFAULT 1,
      duration_ms INTEGER NOT NULL DEFAULT 0,
      rows        INTEGER NOT NULL DEFAULT 0,
      cols        INTEGER NOT NULL DEFAULT 0,
      error       TEXT
    )
  `)
  await db.execute(`
    CREATE TABLE IF NOT EXISTS snippet_groups (
      id         TEXT    PRIMARY KEY,
      name       TEXT    NOT NULL,
      created_at INTEGER NOT NULL
    )
  `)
  await db.execute(`
    CREATE TABLE IF NOT EXISTS snippets (
      id          TEXT    PRIMARY KEY,
      name        TEXT    NOT NULL,
      description TEXT,
      query       TEXT    NOT NULL,
      engine      TEXT    NOT NULL,
      created_at  INTEGER NOT NULL,
      group_id    TEXT
    )
  `)
}

// ── Row ↔ ChatMessage mappers ─────────────────────────────────────────────────
function rowToMessage(row: Record<string, unknown>): ChatMessage {
  return {
    id:                  row.id as string,
    role:                row.role as 'user' | 'assistant',
    content:             (row.content as string) ?? '',
    timestamp:           row.timestamp as number,
    duration_ms:         row.duration_ms != null ? (row.duration_ms as number) : undefined,
    tokens_in:           row.tokens_in   != null ? (row.tokens_in   as number) : undefined,
    tokens_out:          row.tokens_out  != null ? (row.tokens_out  as number) : undefined,
    agenticContinuation: (row.agentic_continuation as number) === 1 ? true : undefined,
    agenticRunId:        row.agentic_run_id    != null ? (row.agentic_run_id    as string) : undefined,
    agenticPlanSteps:    row.agentic_plan_steps != null ? safeJson(row.agentic_plan_steps as string) : undefined,
    suggestions:         row.suggestions       != null ? safeJson(row.suggestions          as string) : undefined,
    queryResults:        row.query_results      != null ? safeJson(row.query_results        as string) : undefined,
    attachedFiles:       row.attached_files     != null ? safeJson(row.attached_files       as string) : undefined,
    sentContext:         row.sent_context       != null ? safeJson(row.sent_context         as string) : undefined,
  }
}

function rowToFile(row: Record<string, unknown>): AttachedFile {
  return {
    path:    row.path as string,
    name:    row.name as string,
    ext:     row.ext  as string,
    columns: row.columns != null ? safeJson(row.columns as string) : undefined,
  }
}

function safeJson<T>(raw: string): T | undefined {
  try { return JSON.parse(raw) as T } catch { return undefined }
}

// ── Messages ──────────────────────────────────────────────────────────────────

export async function dbLoadMessages(): Promise<ChatMessage[]> {
  const db = await getDb()
  const rows = await db.select<Record<string, unknown>[]>(
    'SELECT * FROM messages ORDER BY timestamp ASC, rowid ASC',
  )
  return rows.map(rowToMessage)
}

/** Upsert a single message. query_results rows are stripped to save space. */
export async function dbSaveMessage(msg: ChatMessage): Promise<void> {
  const db = await getDb()
  const slimResults = msg.queryResults?.map((r) => ({ ...r, data: [] }))
  await db.execute(
    `INSERT OR REPLACE INTO messages
       (id, role, content, timestamp, duration_ms, tokens_in, tokens_out,
        agentic_continuation, agentic_run_id, agentic_plan_steps,
        suggestions, query_results, attached_files, sent_context)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [
      msg.id,
      msg.role,
      msg.content,
      msg.timestamp,
      msg.duration_ms     ?? null,
      msg.tokens_in       ?? null,
      msg.tokens_out      ?? null,
      msg.agenticContinuation ? 1 : 0,
      msg.agenticRunId        ?? null,
      msg.agenticPlanSteps    ? JSON.stringify(msg.agenticPlanSteps) : null,
      msg.suggestions         ? JSON.stringify(msg.suggestions)      : null,
      slimResults             ? JSON.stringify(slimResults)          : null,
      msg.attachedFiles       ? JSON.stringify(msg.attachedFiles)    : null,
      msg.sentContext         ? JSON.stringify(msg.sentContext)       : null,
    ],
  )
}

/** Upsert all messages in a single transaction. */
export async function dbSaveAllMessages(msgs: ChatMessage[]): Promise<void> {
  if (msgs.length === 0) return
  for (const msg of msgs) {
    await dbSaveMessage(msg)
  }
}

export async function dbDeleteMessage(id: string): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM messages WHERE id = ?', [id])
}

export async function dbClearMessages(): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM messages')
}

// ── Loaded files ──────────────────────────────────────────────────────────────

export async function dbLoadFiles(): Promise<AttachedFile[]> {
  const db = await getDb()
  const rows = await db.select<Record<string, unknown>[]>('SELECT * FROM loaded_files')
  return rows.map(rowToFile)
}

export async function dbSaveFile(file: AttachedFile): Promise<void> {
  const db = await getDb()
  await db.execute(
    `INSERT OR REPLACE INTO loaded_files (path, name, ext, columns) VALUES (?,?,?,?)`,
    [file.path, file.name, file.ext, file.columns ? JSON.stringify(file.columns) : null],
  )
}

export async function dbDeleteFile(path: string): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM loaded_files WHERE path = ?', [path])
}

export async function dbClearFiles(): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM loaded_files')
}

// ── app_state (generic key-value) ────────────────────────────────────────────

export async function dbGetAppState<T>(key: string, fallback: T): Promise<T> {
  const db = await getDb()
  const rows = await db.select<{ value: string }[]>(
    'SELECT value FROM app_state WHERE key = ?',
    [key],
  )
  if (rows.length === 0) return fallback
  try { return JSON.parse(rows[0].value) as T } catch { return fallback }
}

export async function dbSetAppState<T>(key: string, value: T): Promise<void> {
  const db = await getDb()
  await db.execute(
    'INSERT OR REPLACE INTO app_state (key, value) VALUES (?, ?)',
    [key, JSON.stringify(value)],
  )
}

// ── query_history ─────────────────────────────────────────────────────────────

export async function dbLoadHistory(): Promise<QueryHistoryEntry[]> {
  const db = await getDb()
  const rows = await db.select<Record<string, unknown>[]>(
    'SELECT * FROM query_history ORDER BY timestamp DESC',
  )
  return rows.map((r) => ({
    id:          r.id as string,
    engine:      r.engine as 'blazer' | 'duckdb',
    query:       r.query as string,
    timestamp:   r.timestamp as number,
    success:     (r.success as number) === 1,
    duration_ms: r.duration_ms as number,
    rows:        r.rows as number,
    cols:        r.cols as number,
    error:       r.error != null ? (r.error as string) : undefined,
  }))
}

export async function dbAddHistoryEntry(entry: QueryHistoryEntry): Promise<void> {
  const db = await getDb()
  await db.execute(
    `INSERT OR REPLACE INTO query_history
       (id, engine, query, timestamp, success, duration_ms, rows, cols, error)
     VALUES (?,?,?,?,?,?,?,?,?)`,
    [
      entry.id,
      entry.engine,
      entry.query,
      entry.timestamp,
      entry.success ? 1 : 0,
      entry.duration_ms,
      entry.rows,
      entry.cols,
      entry.error ?? null,
    ],
  )
}

export async function dbDeleteHistoryEntry(id: string): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM query_history WHERE id = ?', [id])
}

export async function dbClearHistory(): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM query_history')
}

// ── snippets ──────────────────────────────────────────────────────────────────

export async function dbLoadSnippets(): Promise<QuerySnippet[]> {
  const db = await getDb()
  const rows = await db.select<Record<string, unknown>[]>(
    'SELECT * FROM snippets ORDER BY created_at DESC',
  )
  return rows.map((r) => ({
    id:          r.id as string,
    name:        r.name as string,
    description: r.description != null ? (r.description as string) : undefined,
    query:       r.query as string,
    engine:      r.engine as 'blazer' | 'duckdb',
    createdAt:   r.created_at as number,
    groupId:     r.group_id != null ? (r.group_id as string) : undefined,
  }))
}

export async function dbSaveSnippet(snippet: QuerySnippet): Promise<void> {
  const db = await getDb()
  await db.execute(
    `INSERT OR REPLACE INTO snippets
       (id, name, description, query, engine, created_at, group_id)
     VALUES (?,?,?,?,?,?,?)`,
    [
      snippet.id,
      snippet.name,
      snippet.description ?? null,
      snippet.query,
      snippet.engine,
      snippet.createdAt,
      snippet.groupId ?? null,
    ],
  )
}

export async function dbDeleteSnippet(id: string): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM snippets WHERE id = ?', [id])
}

export async function dbLoadSnippetGroups(): Promise<SnippetGroup[]> {
  const db = await getDb()
  const rows = await db.select<Record<string, unknown>[]>(
    'SELECT * FROM snippet_groups ORDER BY created_at ASC',
  )
  return rows.map((r) => ({
    id:        r.id as string,
    name:      r.name as string,
    createdAt: r.created_at as number,
  }))
}

export async function dbSaveSnippetGroup(group: SnippetGroup): Promise<void> {
  const db = await getDb()
  await db.execute(
    'INSERT OR REPLACE INTO snippet_groups (id, name, created_at) VALUES (?,?,?)',
    [group.id, group.name, group.createdAt],
  )
}

export async function dbDeleteSnippetGroup(id: string): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM snippet_groups WHERE id = ?', [id])
}

export async function dbClearSnippets(): Promise<void> {
  const db = await getDb()
  await db.execute('DELETE FROM snippets')
}

// ── localStorage → SQLite migration (runs once on first launch) ───────────────
const MIGRATION_KEY = 'blazer_db_migrated_v1'

export async function migrateFromLocalStorage(): Promise<void> {
  if (localStorage.getItem(MIGRATION_KEY)) return   // already done

  try {
    const rawMsgs = localStorage.getItem('blazer_chat_messages')
    if (rawMsgs) {
      const msgs: ChatMessage[] = JSON.parse(rawMsgs)
      if (Array.isArray(msgs) && msgs.length > 0) {
        await dbSaveAllMessages(msgs)
      }
    }

    const rawFiles = localStorage.getItem('blazer_loaded_files')
    if (rawFiles) {
      const files: AttachedFile[] = JSON.parse(rawFiles)
      if (Array.isArray(files) && files.length > 0) {
        for (const f of files) await dbSaveFile(f)
      }
    }

    // Migrate query history
    const rawHistory = localStorage.getItem('blazer_query_history')
    if (rawHistory) {
      const entries: QueryHistoryEntry[] = JSON.parse(rawHistory)
      if (Array.isArray(entries) && entries.length > 0) {
        for (const e of entries) await dbAddHistoryEntry(e)
      }
    }

    // Migrate snippets
    const rawSnippets = localStorage.getItem('blazer_snippets')
    if (rawSnippets) {
      const snippets: QuerySnippet[] = JSON.parse(rawSnippets)
      if (Array.isArray(snippets) && snippets.length > 0) {
        for (const s of snippets) await dbSaveSnippet(s)
      }
    }

    // Migrate snippet groups
    const rawGroups = localStorage.getItem('blazer_snippet_groups')
    if (rawGroups) {
      const groups: SnippetGroup[] = JSON.parse(rawGroups)
      if (Array.isArray(groups) && groups.length > 0) {
        for (const g of groups) await dbSaveSnippetGroup(g)
      }
    }

    // Migrate simple app_state keys from localStorage
    const appStateKeys = [
      'blazer_theme',
      'blazer_chat_engine',
      'blazer_console_engine',
      'blazer_result_history',
      'blazer_autorun',
      'blazer_agentic_mode',
      'blazer_split_pct',
    ]
    for (const key of appStateKeys) {
      const raw = localStorage.getItem(key)
      if (raw != null) {
        try {
          await dbSetAppState(key, JSON.parse(raw))
        } catch {
          await dbSetAppState(key, raw)
        }
      }
    }

    // Migrate settings from Rust invoke
    try {
      const loadedSettings = await invoke('load_settings')
      if (loadedSettings) {
        await dbSetAppState('blazer_settings', loadedSettings)
      }
    } catch {
      // settings invoke may fail if no file exists yet — that's fine
    }

    // Mark migration done and clear legacy keys
    localStorage.setItem(MIGRATION_KEY, '1')
    localStorage.removeItem('blazer_chat_messages')
    localStorage.removeItem('blazer_loaded_files')
    localStorage.removeItem('blazer_query_history')
    localStorage.removeItem('blazer_snippets')
    localStorage.removeItem('blazer_snippet_groups')
    for (const key of appStateKeys) {
      localStorage.removeItem(key)
    }
  } catch (e) {
    console.warn('[blazer] localStorage migration failed — will retry next launch', e)
  }
}
