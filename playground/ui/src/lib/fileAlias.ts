/**
 * fileAlias.ts
 *
 * Derives a safe SQL identifier from an attached file name.
 * Used internally to tag files and pass metadata to Rust tool calls.
 */

import type { AttachedFile } from './types'

/**
 * Derive a safe SQL identifier from a file's name.
 * e.g. "trip complete data.parquet" → "trip_complete_data"
 *      "tracker" (dir)              → "tracker"
 *      "2024-sales.csv"             → "_2024_sales"
 */
export function toAlias(file: AttachedFile): string {
  // Strip known extensions
  const base = file.name
    .replace(/\.(parquet|csv|tsv|xlsx|json|ndjson|jsonl)$/i, '')
    .trim()

  // Replace non-identifier chars with underscores
  let alias = base.replace(/[^a-zA-Z0-9_]/g, '_')

  // SQL identifiers must not start with a digit
  if (/^\d/.test(alias)) alias = '_' + alias

  // Collapse runs of underscores and trim
  alias = alias.replace(/_+/g, '_').replace(/^_+|_+$/g, '')

  return alias || 'data'
}

