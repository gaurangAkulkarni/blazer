import { describe, it, expect } from 'vitest'
import {
  extractSqlStatements,
  normalizeFenceMarkers,
  sqlLooksIncomplete,
  splitMultiStatementBlocks,
} from '../../hooks/useChat'

// ---------------------------------------------------------------------------
// extractSqlStatements
// ---------------------------------------------------------------------------

describe('extractSqlStatements', () => {
  it('returns a single statement with no semicolon', () => {
    expect(extractSqlStatements('SELECT 1')).toEqual(['SELECT 1'])
  })

  it('returns a single statement that has a trailing semicolon', () => {
    expect(extractSqlStatements('SELECT 1;')).toEqual(['SELECT 1;'])
  })

  it('splits two statements separated by a semicolon', () => {
    const result = extractSqlStatements('SELECT 1; SELECT 2')
    expect(result).toHaveLength(2)
    expect(result[0]).toBe('SELECT 1;')
    expect(result[1]).toBe('SELECT 2')
  })

  it('splits three statements and preserves trailing semicolons', () => {
    const sql = 'stmt1; stmt2; stmt3'
    const result = extractSqlStatements(sql)
    expect(result).toHaveLength(3)
    expect(result[0]).toBe('stmt1;')
    expect(result[1]).toBe('stmt2;')
    expect(result[2]).toBe('stmt3')
  })

  it('does NOT split on semicolons inside single-quoted strings', () => {
    const sql = "SELECT ';' AS x; SELECT 2"
    const result = extractSqlStatements(sql)
    expect(result).toHaveLength(2)
    // First statement contains the literal semicolon inside the string
    expect(result[0]).toContain("';'")
    expect(result[1].trim()).toBe('SELECT 2')
  })

  it('does NOT split on semicolons inside line comments', () => {
    const sql = 'SELECT -- semi;\n 1; SELECT 2'
    const result = extractSqlStatements(sql)
    expect(result).toHaveLength(2)
    expect(result[0]).toContain('-- semi;')
  })

  it('returns an empty array for empty input', () => {
    expect(extractSqlStatements('')).toEqual([])
  })

  it('returns an empty array for whitespace-only input', () => {
    expect(extractSqlStatements('   \n  ')).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// normalizeFenceMarkers
// ---------------------------------------------------------------------------

describe('normalizeFenceMarkers', () => {
  it('inserts a newline before a fence that follows non-newline content', () => {
    const input = 'text```sql'
    const result = normalizeFenceMarkers(input)
    expect(result).toBe('text\n```sql')
  })

  it('inserts a newline when fence follows a semicolon with no newline', () => {
    const input = 'query;```sql\nSELECT'
    const result = normalizeFenceMarkers(input)
    expect(result).toBe('query;\n```sql\nSELECT')
  })

  it('leaves a fence already on its own line unchanged', () => {
    const input = 'text\n```sql\nSELECT 1\n```'
    expect(normalizeFenceMarkers(input)).toBe(input)
  })

  it('returns empty string for empty input', () => {
    expect(normalizeFenceMarkers('')).toBe('')
  })

  it('handles closing fences adjacent to content', () => {
    const input = 'SELECT 1```'
    const result = normalizeFenceMarkers(input)
    expect(result).toBe('SELECT 1\n```')
  })
})

// ---------------------------------------------------------------------------
// sqlLooksIncomplete
// ---------------------------------------------------------------------------

describe('sqlLooksIncomplete', () => {
  it('returns false for an empty array', () => {
    expect(sqlLooksIncomplete([])).toBe(false)
  })

  it('returns true for SELECT 1 — it has SELECT but no FROM clause', () => {
    // The implementation treats any SELECT without a FROM as incomplete
    expect(sqlLooksIncomplete(['SELECT 1'])).toBe(true)
  })

  it('returns false for SELECT * FROM table (has both SELECT and FROM)', () => {
    expect(sqlLooksIncomplete(['SELECT * FROM t'])).toBe(false)
  })

  it('returns true for SELECT alone (no FROM clause)', () => {
    expect(sqlLooksIncomplete(['SELECT'])).toBe(true)
  })

  it('returns true for SELECT with a column but no FROM', () => {
    expect(sqlLooksIncomplete(['SELECT col'])).toBe(true)
  })

  it('returns true when query ends with the FROM keyword', () => {
    expect(sqlLooksIncomplete(['SELECT * FROM'])).toBe(true)
  })

  it('returns true when query ends with WHERE', () => {
    expect(sqlLooksIncomplete(['SELECT * FROM t WHERE'])).toBe(true)
  })

  it('returns true when query ends with a bare keyword like WHERE', () => {
    expect(sqlLooksIncomplete(['WHERE'])).toBe(true)
  })

  it('returns true when there is an unmatched open parenthesis', () => {
    expect(sqlLooksIncomplete(['SELECT ('])).toBe(true)
  })

  it('returns true for SELECT (a, b) — SELECT with no FROM is considered incomplete', () => {
    // Even with balanced parens, SELECT without FROM triggers the incomplete check
    expect(sqlLooksIncomplete(['SELECT (a, b)'])).toBe(true)
  })

  it('returns false for a complete SELECT with balanced parens and FROM clause', () => {
    expect(sqlLooksIncomplete(['SELECT count(*) FROM t'])).toBe(false)
  })

  it('returns true when the content is only a line comment', () => {
    expect(sqlLooksIncomplete(['-- just a comment'])).toBe(true)
  })

  it('returns true when query ends with a trailing comma', () => {
    expect(sqlLooksIncomplete(['SELECT a,'])).toBe(true)
  })

  it('returns true when query ends with ORDER BY', () => {
    expect(sqlLooksIncomplete(['SELECT * FROM t ORDER BY'])).toBe(true)
  })

  it('returns false for a multi-line complete query', () => {
    expect(sqlLooksIncomplete([
      'SELECT *',
      'FROM t',
      'WHERE id = 1',
    ])).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// splitMultiStatementBlocks
// ---------------------------------------------------------------------------

describe('splitMultiStatementBlocks', () => {
  it('leaves a single SQL block with one complete statement unchanged', () => {
    const input = '```sql\nSELECT * FROM t\n```'
    const result = splitMultiStatementBlocks(input)
    expect(result).toContain('```sql')
    expect(result).toContain('SELECT * FROM t')
    // Still exactly one block
    const opens = (result.match(/^```sql/gm) ?? []).length
    expect(opens).toBe(1)
  })

  it('splits two complete statements in one block into two separate blocks', () => {
    const input = '```sql\nSELECT * FROM a; SELECT * FROM b\n```'
    const result = splitMultiStatementBlocks(input)
    const opens = (result.match(/^```sql/gm) ?? []).length
    expect(opens).toBe(2)
    expect(result).toContain('SELECT * FROM a;')
    expect(result).toContain('SELECT * FROM b')
  })

  it('passes through non-SQL content unchanged', () => {
    const input = 'Here is some **markdown** text with no fences.'
    expect(splitMultiStatementBlocks(input)).toBe(input)
  })

  it('ignores a hallucinated fence mid-SELECT (no FROM) and keeps the block together', () => {
    // LLM writes ``` before completing the SELECT — should be skipped
    const input = '```sql\nSELECT col\n```\nFROM t\n```'
    const result = splitMultiStatementBlocks(input)
    // Should produce one block containing both parts merged
    const opens = (result.match(/^```sql/gm) ?? []).length
    expect(opens).toBe(1)
    expect(result).toContain('FROM t')
  })

  it('processes multiple SQL blocks separated by prose text independently', () => {
    const input = [
      'First query:',
      '```sql',
      'SELECT * FROM a',
      '```',
      'Second query:',
      '```sql',
      'SELECT * FROM b; SELECT * FROM c',
      '```',
    ].join('\n')
    const result = splitMultiStatementBlocks(input)
    expect(result).toContain('First query:')
    expect(result).toContain('Second query:')
    const opens = (result.match(/^```sql/gm) ?? []).length
    // First block: 1 statement → 1 block; Second block: 2 statements → 2 blocks
    expect(opens).toBe(3)
  })

  it('handles an unclosed fence at the end (streaming) by flushing the partial block', () => {
    const input = '```sql\nSELECT * FROM t'
    const result = splitMultiStatementBlocks(input)
    // Should emit the accumulated SQL (the block was open at EOF)
    expect(result).toContain('SELECT * FROM t')
  })

  it('preserves interleaved text and multiple sql blocks', () => {
    const input = [
      'Analysis:',
      '```sql',
      'SELECT count(*) FROM t',
      '```',
      'Then:',
      '```sql',
      'SELECT avg(x) FROM t',
      '```',
    ].join('\n')
    const result = splitMultiStatementBlocks(input)
    expect(result).toContain('Analysis:')
    expect(result).toContain('Then:')
    expect(result).toContain('count(*)')
    expect(result).toContain('avg(x)')
  })

  it('handles three complete statements in one block — splits into three blocks', () => {
    const input = '```sql\nSELECT * FROM a; SELECT * FROM b; SELECT * FROM c\n```'
    const result = splitMultiStatementBlocks(input)
    const opens = (result.match(/^```sql/gm) ?? []).length
    expect(opens).toBe(3)
  })
})
