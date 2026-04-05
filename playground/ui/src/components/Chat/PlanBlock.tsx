import React, { useContext } from 'react'
import { ChatStreamContext } from './ChatStreamContext'

interface Props {
  code: string
}

function parsePlanLines(raw: string): string[] {
  return raw
    .split('\n')
    .map((l) => l.replace(/^\s*\d+\.\s*/, '').replace(/^\s*[-*]\s*/, '').trim())
    .filter(Boolean)
}

function CheckIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="shrink-0 text-green-500">
      <polyline points="20 6 9 17 4 12"/>
    </svg>
  )
}

function SpinnerIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="shrink-0 animate-spin text-indigo-500">
      <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
    </svg>
  )
}

function ErrorIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="shrink-0 text-red-500">
      <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
    </svg>
  )
}

function DotIcon({ done }: { done?: boolean }) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={`shrink-0 ${done ? 'text-gray-300' : 'text-gray-300'}`}>
      <circle cx="12" cy="12" r="4" fill="currentColor" stroke="none"/>
    </svg>
  )
}

export function PlanBlock({ code }: Props) {
  const {
    agenticActive = false,
    agenticCurrentStep = 0,
    agenticPlanSteps = [],
    agenticStepError = false,
    isStreaming,
  } = useContext(ChatStreamContext)

  // Use steps from context (updated as the loop runs) if available,
  // otherwise fall back to parsing the code block directly.
  const steps = agenticPlanSteps.length > 0 ? agenticPlanSteps : parsePlanLines(code)

  if (steps.length === 0) return null

  const allDone = !agenticActive && agenticCurrentStep >= steps.length

  return (
    <div className="my-2 rounded-lg border border-indigo-100 dark:border-indigo-800 overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2 bg-indigo-50 dark:bg-indigo-950/40 border-b border-indigo-100 dark:border-indigo-800">
        <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="text-indigo-500 shrink-0">
          <rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M9 21V9"/>
        </svg>
        <span className="text-[11px] font-semibold text-indigo-700 dark:text-indigo-300 uppercase tracking-wide">
          Execution Plan
        </span>
        <span className="text-[10px] text-indigo-400 dark:text-indigo-500">
          {steps.length} step{steps.length !== 1 ? 's' : ''}
        </span>

        {/* Right-side status */}
        <div className="ml-auto flex items-center gap-1.5">
          {(agenticActive || isStreaming) && (
            <span className="flex items-center gap-1 text-[10px] text-indigo-500 font-medium">
              <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="animate-spin">
                <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
              </svg>
              Step {Math.min(agenticCurrentStep + 1, steps.length)}/{steps.length}
            </span>
          )}
          {allDone && (
            <span className="flex items-center gap-1 text-[10px] font-semibold text-green-600 dark:text-green-400 bg-green-50 dark:bg-green-950/40 px-1.5 py-0.5 rounded">
              <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="20 6 9 17 4 12"/>
              </svg>
              Complete
            </span>
          )}
        </div>
      </div>

      {/* Steps */}
      <div className="divide-y divide-indigo-50 dark:divide-indigo-900/40">
        {steps.map((step, i) => {
          const isDone = i < agenticCurrentStep || (allDone && i < steps.length)
          const isCurrent = i === agenticCurrentStep && !allDone
          const isError = isCurrent && agenticStepError && !agenticActive

          let rowBg = ''
          let textColor = ''
          let icon: React.ReactNode

          if (isDone) {
            rowBg = 'bg-green-50/40 dark:bg-green-950/20'
            textColor = 'text-green-700 dark:text-green-400'
            icon = <CheckIcon />
          } else if (isError) {
            rowBg = 'bg-red-50/60 dark:bg-red-950/20'
            textColor = 'text-red-700 dark:text-red-400'
            icon = <ErrorIcon />
          } else if (isCurrent && (agenticActive || isStreaming)) {
            rowBg = 'bg-indigo-50/60 dark:bg-indigo-950/30'
            textColor = 'text-indigo-700 dark:text-indigo-300 font-medium'
            icon = <SpinnerIcon />
          } else if (isCurrent) {
            rowBg = 'bg-indigo-50/40 dark:bg-indigo-950/20'
            textColor = 'text-indigo-600 dark:text-indigo-400 font-medium'
            icon = <DotIcon />
          } else {
            rowBg = ''
            textColor = 'text-gray-400 dark:text-gray-600'
            icon = <DotIcon />
          }

          return (
            <div key={i} className={`flex items-start gap-2.5 px-3 py-2 text-xs ${rowBg} ${textColor}`}>
              <span className="shrink-0 mt-0.5">{icon}</span>
              <span className="leading-relaxed">
                <span className="font-mono text-[10px] opacity-50 mr-1">{i + 1}.</span>
                {step}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
