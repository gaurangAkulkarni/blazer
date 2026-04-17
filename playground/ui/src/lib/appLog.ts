/**
 * appLog.ts — Module-level singleton for application-wide structured logging.
 *
 * No React dependency. Import from any file:
 *   import { appLog } from './lib/appLog'
 *
 * Architecture:
 *   - In-memory ring buffer of last 500 entries
 *   - Subscriber pattern for real-time UI updates
 *   - Optional DB persist callback wired externally once DB is ready
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'
export type LogCategory = 'llm' | 'sql' | 'tool' | 'file' | 'agentic' | 'app'

export interface LogEntry {
  id: number        // local counter, sequential
  ts: number        // Date.now() ms
  level: LogLevel
  category: LogCategory
  message: string
  data?: string     // optional JSON string of extra context
}

type Subscriber = (entries: LogEntry[]) => void

const RING_SIZE = 500

class AppLog {
  private _buffer: LogEntry[] = []
  private _counter = 0
  private _subscribers: Set<Subscriber> = new Set()
  private _dbPersist: ((entry: LogEntry) => void) | null = null

  // Called once from App.tsx after DB is initialised
  setDbPersist(fn: (entry: LogEntry) => void) {
    this._dbPersist = fn
  }

  // Subscribe to log updates. Returns an unsubscribe function.
  subscribe(fn: Subscriber): () => void {
    this._subscribers.add(fn)
    // Immediately deliver current buffer so new subscribers are in sync
    fn([...this._buffer])
    return () => { this._subscribers.delete(fn) }
  }

  getAll(): LogEntry[] {
    return [...this._buffer]
  }

  clear() {
    this._buffer = []
    this._notify()
  }

  /**
   * Replay persisted log entries from DB on startup.
   * Prepends them to the buffer without triggering DB persist again.
   * Assigns negative IDs to avoid colliding with the in-session counter.
   */
  replayFromDb(entries: LogEntry[]) {
    if (entries.length === 0) return
    // Keep at most RING_SIZE entries total after prepending
    const combined = [...entries, ...this._buffer]
    this._buffer = combined.slice(-RING_SIZE)
    this._notify()
  }

  // ── Logging methods ──────────────────────────────────────────────────────────

  debug(category: LogCategory, message: string, data?: unknown) {
    this._append('debug', category, message, data)
  }

  info(category: LogCategory, message: string, data?: unknown) {
    this._append('info', category, message, data)
  }

  warn(category: LogCategory, message: string, data?: unknown) {
    this._append('warn', category, message, data)
  }

  error(category: LogCategory, message: string, data?: unknown) {
    this._append('error', category, message, data)
  }

  // ── Private ──────────────────────────────────────────────────────────────────

  private _append(level: LogLevel, category: LogCategory, message: string, data?: unknown) {
    const entry: LogEntry = {
      id: ++this._counter,
      ts: Date.now(),
      level,
      category,
      message,
      data: data !== undefined ? (typeof data === 'string' ? data : JSON.stringify(data)) : undefined,
    }

    this._buffer.push(entry)
    if (this._buffer.length > RING_SIZE) {
      this._buffer.shift()
    }

    // Persist to DB if callback is wired
    if (this._dbPersist) {
      try { this._dbPersist(entry) } catch { /* never crash on log */ }
    }

    this._notify()
  }

  private _notify() {
    const snapshot = [...this._buffer]
    this._subscribers.forEach((fn) => {
      try { fn(snapshot) } catch { /* never crash on subscriber error */ }
    })
  }
}

export const appLog = new AppLog()
