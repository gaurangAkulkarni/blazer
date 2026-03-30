import React from 'react'

interface Props {
  value: number
  onChange: (value: number) => void
}

export function TemperatureSlider({ value, onChange }: Props) {
  return (
    <div>
      <label className="text-xs text-gray-500 mb-1 flex items-center justify-between">
        <span>Temperature</span>
        <span className="tabular-nums text-gray-700 font-medium">{value.toFixed(1)}</span>
      </label>
      <input
        type="range"
        min="0"
        max="2"
        step="0.1"
        value={value}
        onChange={(e) => onChange(parseFloat(e.target.value))}
        className="w-full h-1 bg-gray-200 rounded-full appearance-none cursor-pointer accent-gray-900"
      />
      <div className="flex justify-between text-xs text-gray-400 mt-0.5">
        <span>Precise</span>
        <span>Creative</span>
      </div>
    </div>
  )
}
