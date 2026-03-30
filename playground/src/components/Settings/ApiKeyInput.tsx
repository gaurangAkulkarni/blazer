import React, { useState } from 'react'

interface Props {
  value: string
  onChange: (value: string) => void
  placeholder?: string
}

export function ApiKeyInput({ value, onChange, placeholder }: Props) {
  const [visible, setVisible] = useState(false)

  return (
    <div>
      <label className="text-xs text-gray-500 mb-1 block">API Key</label>
      <div className="relative">
        <input
          type={visible ? 'text' : 'password'}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          className="w-full bg-white text-gray-900 rounded-lg px-3 py-2 pr-16 text-sm border border-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-900/10 focus:border-gray-300 placeholder-gray-300 transition"
        />
        <button
          onClick={() => setVisible(!visible)}
          className="absolute right-2 top-1/2 -translate-y-1/2 text-xs text-gray-400 hover:text-gray-600 px-1.5 py-0.5 rounded transition"
        >
          {visible ? 'Hide' : 'Show'}
        </button>
      </div>
    </div>
  )
}
