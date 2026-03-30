import React, { useState } from 'react'

interface Props {
  value: string
  onChange: (v: string) => void
  placeholder?: string
}

export function ApiKeyInput({ value, onChange, placeholder = 'API key' }: Props) {
  const [show, setShow] = useState(false)
  return (
    <div className="relative">
      <input
        type={show ? 'text' : 'password'}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 pr-10 focus:outline-none focus:ring-2 focus:ring-gray-900/10 font-mono"
      />
      <button
        type="button"
        onClick={() => setShow((s) => !s)}
        className="absolute right-2.5 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
      >
        {show ? '🙈' : '👁'}
      </button>
    </div>
  )
}
