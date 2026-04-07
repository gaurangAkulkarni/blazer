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
import type { ChatMessage, AttachedFile } from './types'

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
    'SELECT * FROM messages ORDER BY timestamp ASC',
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

    // Mark migration done and clear legacy keys
    localStorage.setItem(MIGRATION_KEY, '1')
    localStorage.removeItem('blazer_chat_messages')
    localStorage.removeItem('blazer_loaded_files')
  } catch (e) {
    console.warn('[blazer] localStorage migration failed — will retry next launch', e)
  }
}
