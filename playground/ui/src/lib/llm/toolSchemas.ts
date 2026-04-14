export interface ToolSchema {
  type: 'function'
  function: {
    name: string
    description: string
    parameters: {
      type: 'object'
      properties: Record<string, { type: string; description: string; enum?: string[] }>
      required: string[]
    }
  }
}

export function getToolSchemas(): ToolSchema[] {
  return [
    {
      type: 'function',
      function: {
        name: 'run_sql',
        description: 'Execute a DuckDB SQL query and return results as JSON. Add LIMIT automatically if not present.',
        parameters: {
          type: 'object',
          properties: {
            sql: { type: 'string', description: 'The SQL query to execute' },
            limit: { type: 'number', description: 'Max rows to return (default 100)' },
          },
          required: ['sql'],
        },
      },
    },
    {
      type: 'function',
      function: {
        name: 'describe_tables',
        description: 'List all tables/views with column names and types. Call this FIRST before any analysis.',
        parameters: { type: 'object', properties: {}, required: [] },
      },
    },
    {
      type: 'function',
      function: {
        name: 'get_sample_rows',
        description: 'Get the first N rows from a table or file for quick inspection.',
        parameters: {
          type: 'object',
          properties: {
            table: { type: 'string', description: 'Table name or file path' },
            n: { type: 'number', description: 'Number of rows (default 10)' },
          },
          required: ['table'],
        },
      },
    },
    {
      type: 'function',
      function: {
        name: 'column_stats',
        description: 'Statistical profile for columns: count, nulls, distinct, min, max, mean, median, top 5 values.',
        parameters: {
          type: 'object',
          properties: {
            table: { type: 'string', description: 'Table name or file path' },
            columns: { type: 'array', items: { type: 'string' }, description: 'Columns to profile (omit for all)' } as any,
          },
          required: ['table'],
        },
      },
    },
    {
      type: 'function',
      function: {
        name: 'export_result',
        description: 'Export a query result to a file in the Downloads folder.',
        parameters: {
          type: 'object',
          properties: {
            sql: { type: 'string', description: 'SQL query whose results to export' },
            format: { type: 'string', enum: ['csv', 'parquet', 'json'], description: 'Output format' },
            filename: { type: 'string', description: 'Output filename (e.g. results.csv)' },
          },
          required: ['sql', 'format', 'filename'],
        },
      },
    },
  ]
}
