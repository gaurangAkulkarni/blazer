import React from 'react'

interface Props {
  value: string
  models: string[]
  onChange: (v: string) => void
}

export function ModelSelector({ value, models, onChange }: Props) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="w-full bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 rounded-lg px-3 py-2 text-sm border border-gray-200 dark:border-gray-600 focus:outline-none focus:ring-2 focus:ring-gray-900/10 dark:focus:ring-gray-100/10 transition"
    >
      {models.map((m) => <option key={m} value={m}>{m}</option>)}
    </select>
  )
}
