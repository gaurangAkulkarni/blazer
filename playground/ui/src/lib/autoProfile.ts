import type { AttachedFile } from './types'

/** Human-readable label for the file type — shown in the auto-profile message. */
function fileTypeLabel(ext: string): string {
  switch (ext.toLowerCase()) {
    case 'parquet':     return 'Parquet'
    case 'parquet_dir': return 'directory — format auto-detected'
    case 'csv':         return 'CSV'
    case 'tsv':         return 'TSV'
    case 'csv_dir':     return 'CSV directory'
    case 'xlsx':        return 'Excel'
    case 'xlsx_dir':    return 'Excel directory'
    case 'json':        return 'JSON'
    case 'ndjson':      return 'NDJSON'
    case 'jsonl':       return 'JSONL'
    case 'json_dir':    return 'JSON directory'
    case '':            return 'directory — format auto-detected'
    default:            return ext
  }
}

/** Builds the auto-profile prompt for a set of newly attached files. */
export function buildAutoProfilePrompt(files: AttachedFile[]): string {
  const fileList = files.map(f => `- ${f.path} (${fileTypeLabel(f.ext)})`).join('\n')
  return `New data files attached:\n${fileList}

Analyze in order:
1. Call describe_tables to see all available columns and types
2. Call get_sample_rows for each file (5 rows each)
3. Call column_stats on 3-5 key columns per file (prefer numeric, date, and low-cardinality categorical columns)
4. Write a concise data profile: what the data represents, row/column counts, key columns, data quality issues (nulls, outliers), join keys if multiple files, and 3 analytical questions this data could answer. 4-6 paragraphs, cite specific numbers.

CRITICAL: After running all tools, step 4 is MANDATORY — you must write the data profile as plain text. Do NOT stop after calling tools. The written summary is the required final output. Start writing it immediately after tool results are available.`
}
