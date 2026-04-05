import React from 'react'

interface Props {
  steps: string[]
  currentStep: number
  active: boolean
  stepError: boolean
  iteration: number
  maxIterations: number
  onClear?: () => void
}


export function AgenticTimeline({ steps, currentStep, active, stepError, iteration, maxIterations, onClear }: Props) {
  if (steps.length === 0) return null

  // Consider all done if either the loop stopped OR the step pointer is past the end
  const allDone = !active || currentStep >= steps.length

  return (
    <div className="shrink-0 flex flex-col w-[168px] border-r border-gray-100 dark:border-gray-800 bg-gray-50/60 dark:bg-gray-900/60 overflow-y-auto">
      {/* Header */}
      <div className="shrink-0 flex items-center justify-between px-3 pt-3 pb-2">
        <span className="text-[10px] font-semibold uppercase tracking-widest text-gray-400 dark:text-gray-500">
          Plan
        </span>
        <div className="flex items-center gap-1.5">
          <span className="text-[10px] font-medium tabular-nums text-gray-400 dark:text-gray-500">
            {allDone ? (
              <span className="text-green-500 font-semibold">Done</span>
            ) : active ? (
              <span className="text-indigo-500">{Math.min(currentStep + 1, steps.length)}/{steps.length}</span>
            ) : (
              <span>{steps.length}</span>
            )}
          </span>
          {/* Clear button — always visible so user can dismiss at any point */}
          {onClear && (
            <button
              onClick={onClear}
              title="Clear plan"
              className="p-0.5 rounded text-gray-300 dark:text-gray-600 hover:text-gray-500 dark:hover:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700 transition"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
              </svg>
            </button>
          )}
        </div>
      </div>

      {/* Timeline */}
      <div className="flex flex-col px-3 pb-3 gap-0">
        {steps.map((step, i) => {
          const isDone = allDone || i < currentStep
          const isCurrent = !allDone && i === currentStep
          const isRunning = isCurrent && active
          const isErr = isCurrent && stepError && !active
          const isLast = i === steps.length - 1
          // The last step is always the "final assessment / DONE" step
          const isFinalStep = isLast

          return (
            <div key={i} className="flex gap-2.5 min-w-0">
              {/* Timeline spine */}
              <div className="flex flex-col items-center shrink-0" style={{ width: 16 }}>
                {/* Node — final step uses a flag shape when pending/running */}
                <div
                  className={`shrink-0 rounded-full flex items-center justify-center transition-all duration-300 ${
                    isDone
                      ? 'w-4 h-4 bg-green-500'
                      : isRunning
                      ? `w-4 h-4 ring-2 ring-indigo-200 dark:ring-indigo-800 ${isFinalStep ? 'bg-amber-500' : 'bg-indigo-500'}`
                      : isErr
                      ? 'w-4 h-4 bg-red-500'
                      : isFinalStep
                      ? 'w-3.5 h-3.5 mt-0.5 bg-amber-100 dark:bg-amber-900/40 border border-amber-300 dark:border-amber-700'
                      : 'w-3 h-3 mt-0.5 bg-gray-200 dark:bg-gray-700'
                  }`}
                >
                  {isDone && (
                    <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                      <polyline points="20 6 9 17 4 12"/>
                    </svg>
                  )}
                  {isRunning && isFinalStep && (
                    // Pulse animation for the "writing assessment" state
                    <svg width="7" height="7" viewBox="0 0 24 24" fill="white" stroke="none" className="animate-pulse">
                      <path d="M4 15s1-1 4-1 5 2 8 2 4-1 4-1V3s-1 1-4 1-5-2-8-2-4 1-4 1z"/>
                    </svg>
                  )}
                  {isRunning && !isFinalStep && (
                    <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" className="animate-spin">
                      <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
                    </svg>
                  )}
                  {isErr && (
                    <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                      <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
                    </svg>
                  )}
                  {!isDone && !isRunning && !isErr && isFinalStep && (
                    // Flag outline when step is still pending
                    <svg width="7" height="7" viewBox="0 0 24 24" fill="none" stroke="#d97706" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M4 15s1-1 4-1 5 2 8 2 4-1 4-1V3s-1 1-4 1-5-2-8-2-4 1-4 1z"/>
                      <line x1="4" y1="22" x2="4" y2="15"/>
                    </svg>
                  )}
                </div>
                {/* Connector line */}
                {!isLast && (
                  <div
                    className={`w-px flex-1 min-h-[12px] transition-colors duration-500 ${
                      isDone ? 'bg-green-300 dark:bg-green-700' : 'bg-gray-200 dark:bg-gray-700'
                    }`}
                  />
                )}
              </div>

              {/* Step label */}
              <div className={`flex-1 min-w-0 py-0.5 ${isLast ? '' : 'pb-3'}`}>
                <p
                  className={`text-[11px] leading-snug break-words transition-colors ${
                    isDone
                      ? 'text-green-600 dark:text-green-400'
                      : isRunning && isFinalStep
                      ? 'text-amber-600 dark:text-amber-400 font-semibold'
                      : isRunning
                      ? 'text-indigo-600 dark:text-indigo-300 font-semibold'
                      : isErr
                      ? 'text-red-500 dark:text-red-400'
                      : isFinalStep
                      ? 'text-amber-500/70 dark:text-amber-600/60'
                      : 'text-gray-400 dark:text-gray-600'
                  }`}
                >
                  {step}
                </p>
              </div>
            </div>
          )
        })}
      </div>

      {/* Iteration counter — only while steps remain */}
      {active && !allDone && (
        <div className="shrink-0 mt-auto px-3 py-2 border-t border-gray-100 dark:border-gray-800">
          <div className="flex items-center justify-between text-[10px] text-gray-400 dark:text-gray-600">
            <span>iterations</span>
            <span className="tabular-nums font-medium">{iteration}/{maxIterations}</span>
          </div>
          <div className="mt-1 h-1 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
            <div
              className="h-full rounded-full bg-indigo-400 transition-all duration-500"
              style={{ width: `${(iteration / maxIterations) * 100}%` }}
            />
          </div>
        </div>
      )}
    </div>
  )
}
