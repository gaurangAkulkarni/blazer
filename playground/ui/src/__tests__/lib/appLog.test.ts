import { describe, it, expect, beforeEach, vi } from 'vitest'
import { appLog } from '../../lib/appLog'
import type { LogEntry } from '../../lib/appLog'

beforeEach(() => {
  appLog.clear()
  // Reset the dbPersist callback between tests
  appLog.setDbPersist(() => {})
  appLog.setDbPersist(null as unknown as (entry: LogEntry) => void)
})

describe('AppLog — basic logging', () => {
  it('appends an info entry with the correct level, category, and message', () => {
    appLog.info('sql', 'test query')
    const entries = appLog.getAll()
    expect(entries).toHaveLength(1)
    expect(entries[0].level).toBe('info')
    expect(entries[0].category).toBe('sql')
    expect(entries[0].message).toBe('test query')
  })

  it('appends a warn entry', () => {
    appLog.warn('llm', 'slow response')
    const entries = appLog.getAll()
    expect(entries[0].level).toBe('warn')
  })

  it('appends an error entry', () => {
    appLog.error('app', 'crash')
    const entries = appLog.getAll()
    expect(entries[0].level).toBe('error')
  })

  it('appends a debug entry', () => {
    appLog.debug('tool', 'tool called')
    const entries = appLog.getAll()
    expect(entries[0].level).toBe('debug')
  })

  it('assigns sequential unique ids to each entry', () => {
    appLog.info('sql', 'first')
    appLog.info('sql', 'second')
    appLog.info('sql', 'third')
    const entries = appLog.getAll()
    const ids = entries.map((e) => e.id)
    // All unique
    expect(new Set(ids).size).toBe(3)
    // Sequential (each id is greater than the previous)
    expect(ids[1]).toBeGreaterThan(ids[0])
    expect(ids[2]).toBeGreaterThan(ids[1])
  })

  it('sets ts to approximately Date.now()', () => {
    const before = Date.now()
    appLog.info('sql', 'timing test')
    const after = Date.now()
    const ts = appLog.getAll()[0].ts
    expect(ts).toBeGreaterThanOrEqual(before)
    expect(ts).toBeLessThanOrEqual(after)
  })
})

describe('AppLog — data serialization', () => {
  it('JSON-stringifies object data', () => {
    appLog.info('sql', 'with obj', { foo: 'bar', n: 42 })
    const entry = appLog.getAll()[0]
    expect(entry.data).toBe(JSON.stringify({ foo: 'bar', n: 42 }))
  })

  it('stores string data as-is', () => {
    appLog.info('sql', 'with string', 'raw string')
    const entry = appLog.getAll()[0]
    expect(entry.data).toBe('raw string')
  })

  it('leaves data undefined when not provided', () => {
    appLog.info('sql', 'no data')
    const entry = appLog.getAll()[0]
    expect(entry.data).toBeUndefined()
  })
})

describe('AppLog — ring buffer', () => {
  it('caps the buffer at 500 entries and drops the oldest', () => {
    for (let i = 0; i < 501; i++) {
      appLog.info('app', `entry ${i}`)
    }
    const entries = appLog.getAll()
    expect(entries).toHaveLength(500)
    // The very first message (entry 0) should be gone
    expect(entries[0].message).toBe('entry 1')
    // The last message should be the 501st
    expect(entries[499].message).toBe('entry 500')
  })
})

describe('AppLog — clear', () => {
  it('empties the buffer', () => {
    appLog.info('sql', 'a')
    appLog.info('sql', 'b')
    appLog.clear()
    expect(appLog.getAll()).toHaveLength(0)
  })

  it('notifies subscribers with an empty array after clear', () => {
    appLog.info('sql', 'a')
    const received: LogEntry[][] = []
    appLog.subscribe((entries) => received.push(entries))
    // clear triggers notification
    appLog.clear()
    // Last notification should be empty
    const last = received[received.length - 1]
    expect(last).toHaveLength(0)
  })
})

describe('AppLog — subscribe', () => {
  it('immediately delivers the current buffer to a new subscriber', () => {
    appLog.info('sql', 'existing entry')
    let received: LogEntry[] = []
    appLog.subscribe((entries) => { received = entries })
    expect(received).toHaveLength(1)
    expect(received[0].message).toBe('existing entry')
  })

  it('notifies all subscribers when a new entry arrives', () => {
    const calls1: number[] = []
    const calls2: number[] = []
    appLog.subscribe((entries) => calls1.push(entries.length))
    appLog.subscribe((entries) => calls2.push(entries.length))
    appLog.info('sql', 'new entry')
    // Each subscriber should have been called at least once with 1 entry
    expect(calls1[calls1.length - 1]).toBeGreaterThanOrEqual(1)
    expect(calls2[calls2.length - 1]).toBeGreaterThanOrEqual(1)
  })

  it('stops delivering after unsubscribe', () => {
    const calls: LogEntry[][] = []
    const unsub = appLog.subscribe((entries) => calls.push(entries))
    const countBefore = calls.length
    unsub()
    appLog.info('sql', 'after unsub')
    // No new calls should have been made after unsub
    expect(calls.length).toBe(countBefore)
  })
})

describe('AppLog — replayFromDb', () => {
  it('prepends DB entries to the buffer', () => {
    const dbEntries: LogEntry[] = [
      { id: -2, ts: 1000, level: 'info', category: 'sql', message: 'from db 1' },
      { id: -1, ts: 2000, level: 'warn', category: 'app', message: 'from db 2' },
    ]
    appLog.replayFromDb(dbEntries)
    const all = appLog.getAll()
    expect(all).toHaveLength(2)
    expect(all[0].message).toBe('from db 1')
    expect(all[1].message).toBe('from db 2')
  })

  it('does NOT call dbPersist for replayed entries', () => {
    const persistFn = vi.fn()
    appLog.setDbPersist(persistFn)

    const dbEntries: LogEntry[] = [
      { id: -1, ts: 1000, level: 'info', category: 'sql', message: 'replayed' },
    ]
    appLog.replayFromDb(dbEntries)
    expect(persistFn).not.toHaveBeenCalled()
  })

  it('is a no-op for an empty array', () => {
    appLog.info('sql', 'existing')
    appLog.replayFromDb([])
    expect(appLog.getAll()).toHaveLength(1)
  })
})

describe('AppLog — setDbPersist', () => {
  it('calls dbPersist for each new entry once set', () => {
    const persistFn = vi.fn()
    appLog.setDbPersist(persistFn)
    appLog.info('sql', 'persisted entry')
    expect(persistFn).toHaveBeenCalledTimes(1)
    const calledWith = persistFn.mock.calls[0][0] as LogEntry
    expect(calledWith.message).toBe('persisted entry')
  })

  it('does not call dbPersist before it is set', () => {
    const persistFn = vi.fn()
    appLog.info('sql', 'before set')
    appLog.setDbPersist(persistFn)
    expect(persistFn).not.toHaveBeenCalled()
  })
})

describe('AppLog — subscriber error isolation', () => {
  it('does not crash when a subscriber throws on a new entry; other subscribers still receive it', () => {
    let throwOnNext = false
    const goodCalls: LogEntry[][] = []

    // Register both subscribers first (with the buffer empty they both receive [] without throwing)
    appLog.subscribe(() => {
      if (throwOnNext) throw new Error('subscriber crash')
    })
    appLog.subscribe((entries) => goodCalls.push(entries))

    // Count calls from initial deliveries before we trigger a crash
    const countBeforeCrash = goodCalls.length

    // Now arm the bad subscriber to throw on the next notification
    throwOnNext = true
    expect(() => appLog.info('app', 'test isolation')).not.toThrow()

    // The good subscriber should have received at least one new call after the crash
    expect(goodCalls.length).toBeGreaterThan(countBeforeCrash)
    const lastSnapshot = goodCalls[goodCalls.length - 1]
    expect(lastSnapshot.some((e) => e.message === 'test isolation')).toBe(true)
  })
})
