import { describe, it, expect, vi, beforeEach } from 'vitest'
import type { Mock } from 'vitest'

// ---------------------------------------------------------------------------
// Use vi.hoisted so the variables are defined before vi.mock hoisting runs
// ---------------------------------------------------------------------------

const { mockExecute, mockSelect } = vi.hoisted(() => {
  const mockExecute = vi.fn().mockResolvedValue(undefined)
  const mockSelect  = vi.fn().mockResolvedValue([])
  return { mockExecute, mockSelect }
})

vi.mock('@tauri-apps/plugin-sql', () => ({
  default: {
    load: vi.fn().mockResolvedValue({
      execute: mockExecute,
      select:  mockSelect,
    }),
  },
}))

vi.mock('@tauri-apps/api/core', () => ({
  invoke: vi.fn().mockResolvedValue({}),
}))

// Import AFTER mocking
import { dbAppendLog, dbGetLogs, dbFlushOldLogs } from '../../lib/db'
import type { LogEntry } from '../../lib/appLog'

function makeLogEntry(overrides: Partial<LogEntry> = {}): LogEntry {
  return {
    id:       1,
    ts:       Date.now(),
    level:    'info',
    category: 'sql',
    message:  'test message',
    data:     undefined,
    ...overrides,
  }
}

beforeEach(() => {
  vi.clearAllMocks()
  // Reset select to return empty by default
  mockSelect.mockResolvedValue([])
  mockExecute.mockResolvedValue(undefined)
})

describe('dbAppendLog', () => {
  it('calls db.execute with an INSERT INTO app_logs statement', async () => {
    const entry = makeLogEntry({ ts: 1234567890, level: 'warn', category: 'llm', message: 'hello', data: '{"x":1}' })
    await dbAppendLog(entry)
    expect(mockExecute).toHaveBeenCalledWith(
      expect.stringMatching(/INSERT INTO app_logs/i),
      expect.arrayContaining([entry.ts, entry.level, entry.category, entry.message, entry.data]),
    )
  })

  it('passes null as the data argument when data is undefined', async () => {
    const entry = makeLogEntry({ data: undefined })
    await dbAppendLog(entry)
    const calls = (mockExecute as Mock).mock.calls
    const lastCall = calls[calls.length - 1] as [string, unknown[]]
    const params = lastCall[1]
    // data is the last param — should be null
    expect(params[params.length - 1]).toBeNull()
  })
})

describe('dbGetLogs', () => {
  it('calls db.select with the correct query and maps rows to LogEntry shape', async () => {
    const fakeRow = {
      id:       42,
      ts:       9999,
      level:    'error',
      category: 'app',
      message:  'oops',
      data:     null,
    }
    mockSelect.mockResolvedValueOnce([fakeRow])

    const logs = await dbGetLogs(100)

    expect(mockSelect).toHaveBeenCalledWith(
      expect.stringMatching(/SELECT.*FROM app_logs/si),
      expect.arrayContaining([100]),
    )
    expect(logs).toHaveLength(1)
    expect(logs[0]).toMatchObject({
      id:       42,
      ts:       9999,
      level:    'error',
      category: 'app',
      message:  'oops',
    })
    expect(logs[0].data).toBeUndefined()
  })

  it('maps data string from row to the LogEntry data field', async () => {
    const fakeRow = {
      id:       1,
      ts:       1000,
      level:    'info',
      category: 'sql',
      message:  'with data',
      data:     '{"rows":5}',
    }
    mockSelect.mockResolvedValueOnce([fakeRow])

    const logs = await dbGetLogs()
    expect(logs[0].data).toBe('{"rows":5}')
  })

  it('uses a default limit of 2000 when no limit argument is provided', async () => {
    await dbGetLogs()
    expect(mockSelect).toHaveBeenCalledWith(
      expect.any(String),
      expect.arrayContaining([2000]),
    )
  })
})

describe('dbFlushOldLogs', () => {
  it('calls db.execute with a DELETE WHERE ts < ... for the 7-day cutoff', async () => {
    const before = Date.now() - 7 * 24 * 60 * 60 * 1000
    await dbFlushOldLogs()
    const [, params] = (mockExecute as Mock).mock.calls[0] as [string, unknown[]]
    const deleteCutoff = params[0] as number
    // The cutoff should be approximately 7 days ago (within 1 second tolerance)
    expect(deleteCutoff).toBeGreaterThanOrEqual(before - 1000)
    expect(deleteCutoff).toBeLessThanOrEqual(Date.now())
  })

  it('performs the size-check select after the 7-day purge', async () => {
    await dbFlushOldLogs()
    // After the DELETE, a SELECT SUM(...) query should be issued.
    // The size-check select is called with only the SQL string (no params array).
    const selectCalls = (mockSelect as Mock).mock.calls
    const sizeCheckCall = selectCalls.find(([sql]: [string]) =>
      typeof sql === 'string' && /SUM.*length/i.test(sql),
    )
    expect(sizeCheckCall).toBeTruthy()
  })

  it('deletes oldest 25% when size exceeds 50 MB (52428800 bytes)', async () => {
    // First select: size check returns > 50MB
    mockSelect
      .mockResolvedValueOnce([{ sz: 60000000 }])  // size check → over limit
      .mockResolvedValueOnce([{ cnt: 400 }])       // count check → 400 rows

    await dbFlushOldLogs()

    // Should have called execute at least twice:
    // 1. DELETE WHERE ts < cutoff
    // 2. DELETE oldest 25% = 100 rows
    const deleteCalls = (mockExecute as Mock).mock.calls.filter(([sql]: [string]) =>
      typeof sql === 'string' && /DELETE/i.test(sql),
    )
    expect(deleteCalls.length).toBeGreaterThanOrEqual(2)

    // Find the LIMIT-based delete call
    const limitDeleteCall = deleteCalls.find(([sql]: [string]) => /LIMIT/i.test(sql))
    expect(limitDeleteCall).toBeTruthy()
    const limitParam = limitDeleteCall![1][0] as number
    expect(limitParam).toBe(100) // 25% of 400
  })

  it('does NOT delete extra rows when size is under 50 MB', async () => {
    mockSelect.mockResolvedValueOnce([{ sz: 1000 }]) // well under 50 MB

    await dbFlushOldLogs()

    // Only the initial 7-day DELETE should have fired — no extra DELETE
    const executeCalls = (mockExecute as Mock).mock.calls
    expect(executeCalls.length).toBe(1)
  })
})
