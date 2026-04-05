import React from 'react'

interface Props {
  value: number
  onChange: (v: number) => void
}

export function TemperatureSlider({ value, onChange }: Props) {
  return (
    <div>
      <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400 mb-1">
        <span>Temperature</span>
        <span className="tabular-nums font-mono">{value.toFixed(1)}</span>
      </div>
      <input
        type="range"
        min={0} max={1} step={0.1}
        value={value}
        onChange={(e) => onChange(parseFloat(e.target.value))}
        className="w-full accent-gray-900"
      />
    </div>
  )
}
