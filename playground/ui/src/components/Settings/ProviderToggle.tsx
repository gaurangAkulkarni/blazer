import React from 'react'

interface Props {
  active: 'openai' | 'claude'
  onChange: (p: 'openai' | 'claude') => void
}

export function ProviderToggle({ active, onChange }: Props) {
  return (
    <div className="flex rounded-lg border border-gray-200 overflow-hidden">
      {(['openai', 'claude'] as const).map((p) => (
        <button
          key={p}
          onClick={() => onChange(p)}
          className={`flex-1 py-2 text-xs font-medium transition ${active === p ? 'bg-gray-900 text-white' : 'bg-white text-gray-500 hover:bg-gray-50'}`}
        >
          {p === 'openai' ? 'OpenAI' : 'Claude'}
        </button>
      ))}
    </div>
  )
}
