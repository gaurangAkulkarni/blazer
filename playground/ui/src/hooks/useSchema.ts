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
}

// readExpr replaced by resolveReadExpr from lib/readExpr (async, handles xlsx_dir/csv_dir)

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
      const expr = await resolveReadExpr(file)

      // Column names + types
      const descResult = await invoke<{ success: boolean; data: Record<string, unknown>[]; error?: string }>(
        'run_duckdb_query',
        { sql: `DESCRIBE SELECT * FROM ${expr}` },
      )

      if (!descResult.success) throw new Error(descResult.error ?? 'Describe failed')

      const columns: ColumnInfo[] = (descResult.data ?? []).map(row => ({
        name:  String(row['column_name'] ?? row['Field'] ?? ''),
        type:  String(row['column_type'] ?? row['Type'] ?? 'unknown'),
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
        [path]: { path, name: file.name, ext: file.ext, columns, rowCount, loading: false },
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
