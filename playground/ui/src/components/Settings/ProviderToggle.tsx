import React from 'react'

type Provider = 'openai' | 'claude' | 'ollama'

interface Props {
  active: Provider
  onChange: (p: Provider) => void
}

const LABELS: Record<Provider, string> = {
  openai: 'OpenAI',
  claude: 'Claude',
  ollama: 'Ollama',
}

export function ProviderToggle({ active, onChange }: Props) {
  return (
    <div className="flex rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
      {(['openai', 'claude', 'ollama'] as Provider[]).map((p) => (
        <button
          key={p}
          onClick={() => onChange(p)}
          className={`flex-1 py-2 text-xs font-medium transition ${active === p ? 'bg-gray-900 text-white dark:bg-white dark:text-gray-900' : 'bg-white dark:bg-gray-800 text-gray-500 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700'}`}
        >
          {LABELS[p]}
        </button>
      ))}
    </div>
  )
}
