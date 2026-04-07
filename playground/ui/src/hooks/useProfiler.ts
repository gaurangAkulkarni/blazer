import { useState, useCallback } from 'react'
import { invoke } from '@tauri-apps/api/core'
import type { AttachedFile } from '../lib/types'
import { resolveReadExpr } from '../lib/readExpr'

// ── Types ─────────────────────────────────────────────────────────────────────

export interface TopValue {
  val: string
  cnt: number
  pct: number   // percent of non-null rows
}

export interface ColumnProfile {
  column_name: string
  column_type: string
  count: number           // non-null row count
  null_percentage: number // 0–100
  approx_unique: number
  min: string | null
  max: string | null
  avg: number | null
  std: number | null
  q25: string | null
  q50: string | null      // median
  q75: string | null
  top_values?: TopValue[] // categorical columns only
}

export interface FileProfile {
  path: string
  name: string
  total_rows: number
  columns: ColumnProfile[]
  loading: boolean
  duration_ms?: number
  error?: string
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// readExpr is replaced by resolveReadExpr from lib/readExpr (async, handles xlsx_dir/csv_dir)

function isNumericType(t: string): boolean {
  return /INT|BIGINT|HUGEINT|FLOAT|DOUBLE|DECIMAL|REAL|NUMERIC|TINYINT|SMALLINT/i.test(t)
}

function isStringType(t: string): boolean {
  return /VARCHAR|TEXT|STRING|CHAR|CATEGORY/i.test(t)
}

function toNum(v: unknown): number | null {
  if (v === null || v === undefined || v === '') return null
  const n = parseFloat(String(v))
  return isNaN(n) ? null : n
}

type QueryResult = { success: boolean; data: Record<string, unknown>[]; error?: string }

// ── Hook ──────────────────────────────────────────────────────────────────────

export function useProfiler() {
  const [profiles, setProfiles] = useState<Record<string, FileProfile>>({})

  const profileFile = useCallback(async (file: AttachedFile) => {
    const { path } = file

    setProfiles(prev => ({
      ...prev,
      [path]: { path, name: file.name, total_rows: 0, columns: [], loading: true },
    }))

    const t0 = Date.now()
    const expr = await resolveReadExpr(file)

    try {
      // ── Phase 1: SUMMARIZE ───────────────────────────────────────────────
      const sumResult = await invoke<QueryResult>('run_duckdb_query', {
        sql: `SUMMARIZE SELECT * FROM ${expr}`,
      })
      if (!sumResult.success) throw new Error(sumResult.error ?? 'SUMMARIZE failed')

      const columns: ColumnProfile[] = (sumResult.data ?? []).map(row => ({
        column_name:     String(row['column_name'] ?? ''),
        column_type:     String(row['column_type'] ?? 'unknown'),
        count:           Number(row['count']          ?? 0),
        null_percentage: toNum(row['null_percentage']) ?? 0,
        approx_unique:   Number(row['approx_unique']  ?? 0),
        min:             row['min']  != null ? String(row['min'])  : null,
        max:             row['max']  != null ? String(row['max'])  : null,
        avg:             toNum(row['avg']),
        std:             toNum(row['std']),
        q25:             row['q25']  != null ? String(row['q25'])  : null,
        q50:             row['q50']  != null ? String(row['q50'])  : null,
        q75:             row['q75']  != null ? String(row['q75'])  : null,
      }))

      // Total rows = max count column (null_percentage=0 col), or first col count / (1 - null_pct)
      const total_rows = columns.reduce((max, c) => Math.max(max, c.count), 0)
        || (columns[0] ? Math.round(columns[0].count / (1 - columns[0].null_percentage / 100)) : 0)

      // ── Phase 2: top-5 values for low-cardinality string columns ─────────
      // Limit to first 8 qualifying columns to avoid scanning very wide tables
      const catCols = columns
        .filter(c => isStringType(c.column_type) && c.approx_unique > 0 && c.approx_unique <= 200)
        .slice(0, 8)

      if (catCols.length > 0) {
        const unionParts = catCols.map(c => {
          const colQ = `"${c.column_name.replace(/"/g, '""')}"`
          return `(SELECT ${JSON.stringify(c.column_name)} AS col_name, ` +
                 `CAST(${colQ} AS VARCHAR) AS val, COUNT(*) AS cnt ` +
                 `FROM ${expr} WHERE ${colQ} IS NOT NULL GROUP BY ${colQ} ORDER BY cnt DESC LIMIT 5)`
        })
        const topSql = unionParts.join('\nUNION ALL\n')

        try {
          const topResult = await invoke<QueryResult>('run_duckdb_query', { sql: topSql })
          if (topResult.success && topResult.data) {
            // Group by column name
            const byCol: Record<string, { val: string; cnt: number }[]> = {}
            for (const row of topResult.data) {
              const col = String(row['col_name'])
              if (!byCol[col]) byCol[col] = []
              byCol[col].push({ val: String(row['val']), cnt: Number(row['cnt']) })
            }
            // Attach top_values to matching column profiles
            for (const col of columns) {
              const rows = byCol[col.column_name]
              if (rows?.length) {
                const totalNonNull = rows.reduce((s, r) => s + r.cnt, 0)
                col.top_values = rows.map(r => ({
                  val: r.val,
                  cnt: r.cnt,
                  pct: totalNonNull > 0 ? (r.cnt / totalNonNull) * 100 : 0,
                }))
              }
            }
          }
        } catch { /* top-values failure is non-fatal */ }
      }

      setProfiles(prev => ({
        ...prev,
        [path]: {
          path, name: file.name, total_rows, columns,
          loading: false,
          duration_ms: Date.now() - t0,
        },
      }))
    } catch (e) {
      setProfiles(prev => ({
        ...prev,
        [path]: {
          path, name: file.name, total_rows: 0, columns: [],
          loading: false, error: String(e),
        },
      }))
    }
  }, [])

  const clearProfile = useCallback((path: string) => {
    setProfiles(prev => { const n = { ...prev }; delete n[path]; return n })
  }, [])

  return { profiles, profileFile, clearProfile }
}
