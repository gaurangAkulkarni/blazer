import type { AttachedFile } from './types'

/** Builds the auto-profile prompt for a set of newly attached files. */
export function buildAutoProfilePrompt(files: AttachedFile[]): string {
  const fileList = files.map(f => `- ${f.path} (${f.ext})`).join('\n')
  return `New data files attached:\n${fileList}

Analyze in order:
1. Call describe_tables to see all available columns and types
2. Call get_sample_rows for each file (5 rows each)
3. Call column_stats on 3-5 key columns per file (prefer numeric, date, and low-cardinality categorical columns)
4. Write a concise data profile: what the data represents, row/column counts, key columns, data quality issues (nulls, outliers), join keys if multiple files, and 3 analytical questions this data could answer. 4-6 paragraphs, cite specific numbers.`
}
