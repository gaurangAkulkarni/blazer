import { useState, useCallback } from 'react'
import { invoke } from '@tauri-apps/api/core'
import type { AttachedFile } from '../lib/types'
import { resolveReadExpr } from '../lib/readExpr'

export interface ColumnInfo {
  name: string
  type: string
}

export interface FileSchema {
  path: string
  name: string
  ext: string
  columns: ColumnInfo[]
  rowCount?: number
  loading: boolean
  error?: string
  /** Detected reader expression (may differ from the ext-based default for dirs) */
  detectedReader?: string
}

// ── Directory content detection ───────────────────────────────────────────────
// When a directory is attached, we don't know what file types live inside.
// This globs the directory and builds the correct DuckDB reader expression
// based on the dominant file extension found.
async function detectDirReader(dirPath: string): Promise<{ expr: string; detectedExt: string } | null> {
  const p = dirPath.replace(/'/g, "''")
  try {
    const res = await invoke<{ success: boolean; data: Record<string, unknown>[] }>(
      'run_duckdb_query',
      { sql: `SELECT file FROM glob('${p}/**/*') WHERE file NOT LIKE '%.DS_Store' AND file NOT LIKE '%/.git%' LIMIT 100` },
    )
    if (!res.success || !res.data.length) return null

    // Tally extensions, ignoring system files
    const extCounts: Record<string, number> = {}
    for (const row of res.data) {
      const f = String(row['file'] ?? '')
      const ext = f.split('.').pop()?.toLowerCase() ?? ''
      if (ext && ext.length <= 8) extCounts[ext] = (extCounts[ext] ?? 0) + 1
    }

    const dominant = Object.entries(extCounts).sort((a, b) => b[1] - a[1])[0]?.[0] ?? ''

    const dataFiles = res.data.map(r => String(r['file'] ?? ''))
    const escape = (s: string) => s.replace(/'/g, "''")

    if (dominant === 'parquet') {
      return { expr: `read_parquet('${p}/**/*.parquet')`, detectedExt: 'parquet_dir' }
    }
    if (dominant === 'csv' || dominant === 'tsv') {
      return { expr: `read_csv_auto('${p}/*.${dominant}')`, detectedExt: `${dominant}_dir` }
    }
    if (dominant === 'xlsx') {
      return { expr: `read_xlsx('${p}/*.xlsx', all_varchar=true)`, detectedExt: 'xlsx_dir' }
    }
    if (dominant === 'json' || dominant === 'ndjson' || dominant === 'jsonl') {
      // Single file → reference it directly for the most reliable read
      const jsonFiles = dataFiles.filter(f => /\.(json|ndjson|jsonl)$/i.test(f))
      if (jsonFiles.length === 1) {
        return { expr: `read_json_auto('${escape(jsonFiles[0])}')`, detectedExt: dominant }
      }
      return { expr: `read_json_auto('${p}/**/*.${dominant}')`, detectedExt: `${dominant}_dir` }
    }
  } catch { /* fall through */ }
  return null
}

const DIR_EXTS = new Set(['', 'parquet_dir', 'csv_dir', 'xlsx_dir', 'json_dir', 'ndjson_dir'])
const isDir = (file: AttachedFile) => DIR_EXTS.has(file.ext.toLowerCase())

export function useSchema() {
  const [schemas, setSchemas] = useState<Record<string, FileSchema>>({})

  const fetchSchema = useCallback(async (file: AttachedFile) => {
    const { path } = file
    // Skip if already loaded (or currently loading)
    if (schemas[path] && (schemas[path].columns.length > 0 || schemas[path].loading)) return

    setSchemas(prev => ({
      ...prev,
      [path]: { path, name: file.name, ext: file.ext, columns: [], loading: true },
    }))

    try {
      let expr = await resolveReadExpr(file)
      let detectedReader: string | undefined
      let detectedExt = file.ext

      // ── First attempt ─────────────────────────────────────────────────────
      let descResult = await invoke<{ success: boolean; data: Record<string, unknown>[]; error?: string }>(
        'run_duckdb_query',
        { sql: `DESCRIBE SELECT * FROM ${expr} LIMIT 0` },
      )

      // ── Fallback: auto-detect actual content type for directories ─────────
      // If the default reader failed (e.g. read_parquet on a folder with only
      // NDJSON files), glob the directory to find what's actually there.
      if (!descResult.success && isDir(file)) {
        const detected = await detectDirReader(path)
        if (detected) {
          expr = detected.expr
          detectedExt = detected.detectedExt
          detectedReader = detected.expr
          descResult = await invoke<{ success: boolean; data: Record<string, unknown>[]; error?: string }>(
            'run_duckdb_query',
            { sql: `DESCRIBE SELECT * FROM ${expr} LIMIT 0` },
          )
        }
      }

      if (!descResult.success) throw new Error(descResult.error ?? 'Describe failed')

      const columns: ColumnInfo[] = (descResult.data ?? []).map(row => ({
        name: String(row['column_name'] ?? row['Field'] ?? ''),
        type: String(row['column_type'] ?? row['Type'] ?? 'unknown'),
      }))

      // Row count (best-effort — skip on error)
      let rowCount: number | undefined
      try {
        const cntResult = await invoke<{ success: boolean; data: Record<string, unknown>[] }>(
          'run_duckdb_query',
          { sql: `SELECT COUNT(*) AS _cnt FROM ${expr}` },
        )
        if (cntResult.success && cntResult.data?.[0]) {
          rowCount = Number(cntResult.data[0]['_cnt'])
        }
      } catch { /* ignore */ }

      setSchemas(prev => ({
        ...prev,
        [path]: { path, name: file.name, ext: detectedExt, columns, rowCount, loading: false, detectedReader },
      }))
    } catch (e) {
      setSchemas(prev => ({
        ...prev,
        [path]: { path, name: file.name, ext: file.ext, columns: [], loading: false, error: String(e) },
      }))
    }
  }, [schemas])

  const fetchAll = useCallback((files: AttachedFile[]) => {
    files.forEach(f => fetchSchema(f))
  }, [fetchSchema])

  const invalidate = useCallback((path: string) => {
    setSchemas(prev => {
      const next = { ...prev }
      delete next[path]
      return next
    })
  }, [])

  return { schemas, fetchSchema, fetchAll, invalidate }
}
