import React from 'react'

interface Props {
  active: 'openai' | 'claude'
  onChange: (provider: 'openai' | 'claude') => void
}

export function ProviderToggle({ active, onChange }: Props) {
  return (
    <div className="flex bg-gray-100 rounded-lg p-0.5">
      <button
        onClick={() => onChange('openai')}
        className={`flex-1 text-sm py-2 px-3 rounded-md font-medium transition ${
          active === 'openai'
            ? 'bg-white text-gray-900 shadow-sm'
            : 'text-gray-500 hover:text-gray-700'
        }`}
      >
        OpenAI
      </button>
      <button
        onClick={() => onChange('claude')}
        className={`flex-1 text-sm py-2 px-3 rounded-md font-medium transition ${
          active === 'claude'
            ? 'bg-white text-gray-900 shadow-sm'
            : 'text-gray-500 hover:text-gray-700'
        }`}
      >
        Claude
      </button>
    </div>
  )
}
