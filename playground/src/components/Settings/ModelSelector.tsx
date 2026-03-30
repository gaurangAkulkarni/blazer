import React from 'react'

interface Props {
  value: string
  models: string[]
  onChange: (model: string) => void
}

export function ModelSelector({ value, models, onChange }: Props) {
  return (
    <div>
      <label className="text-xs text-gray-400 mb-1 block">Model</label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full bg-gray-800 text-gray-200 rounded-lg px-3 py-2 text-sm border border-gray-700 focus:outline-none focus:ring-2 focus:ring-blazer-500/50"
      >
        {models.map((m) => (
          <option key={m} value={m}>
            {m}
          </option>
        ))}
      </select>
    </div>
  )
}
