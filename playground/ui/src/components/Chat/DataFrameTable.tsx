import React from 'react'

interface Props {
  data: Record<string, unknown>[]
  columns: string[]
  shape: [number, number]
  durationMs?: number
}

export function DataFrameTable({ data, columns, shape, durationMs }: Props) {
  const displayRows = data.slice(0, 200)
  const [totalRows, totalCols] = shape

  return (
    <div className="my-2">
      <div className="text-xs text-gray-500 mb-1.5 flex items-center gap-2">
        <span className="font-semibold text-gray-700">Result</span>
        <span>{totalRows.toLocaleString()} rows × {totalCols} cols</span>
        {displayRows.length < totalRows && (
          <span className="text-gray-400">(showing first {displayRows.length})</span>
        )}
        {durationMs !== undefined && (
          <span className="text-gray-400 ml-auto">{durationMs}ms</span>
        )}
      </div>
      <div className="overflow-x-auto rounded-lg border border-gray-200 max-h-[420px] overflow-y-auto">
        <table className="min-w-full text-xs">
          <thead className="sticky top-0 z-10">
            <tr className="bg-gray-50">
              <th className="px-3 py-2 text-left text-gray-400 font-medium border-b border-gray-200 w-10">#</th>
              {columns.map((col) => (
                <th key={col} className="px-3 py-2 text-left text-gray-600 font-semibold border-b border-gray-200 whitespace-nowrap">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayRows.map((row, i) => (
              <tr key={i} className={`${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'} hover:bg-blue-50/50 transition-colors`}>
                <td className="px-3 py-1.5 text-gray-400 border-b border-gray-100">{i}</td>
                {columns.map((col) => {
                  const val = row[col]
                  const isNum = typeof val === 'number'
                  return (
                    <td key={col} className={`px-3 py-1.5 border-b border-gray-100 whitespace-nowrap ${isNum ? 'text-right tabular-nums text-gray-800' : 'text-gray-700'} ${val === null ? 'text-gray-300 italic' : ''}`}>
                      {val === null ? 'null' : isNum ? (Number.isInteger(val) ? val : (val as number).toFixed(4)) : String(val)}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
