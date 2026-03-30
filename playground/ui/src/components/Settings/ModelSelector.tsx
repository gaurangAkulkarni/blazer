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
      className="w-full bg-white text-gray-900 rounded-lg px-3 py-2 text-sm border border-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-900/10 transition"
    >
      {models.map((m) => <option key={m} value={m}>{m}</option>)}
    </select>
  )
}
