import React, { useState, useCallback, useRef } from 'react'
import { ChatContainer } from './components/Chat/ChatContainer'
import { ResultsPanel } from './components/Results/ResultsPanel'
import { SettingsPanel } from './components/Settings/SettingsPanel'
import { useSettings } from './hooks/useSettings'
import { useChat } from './hooks/useChat'
import type { ResultEntry, ExecutionResult, AddResultFn } from './lib/types'

export default function App() {
  const { settings, updateSettings, loaded } = useSettings()
  const chat = useChat(settings)
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [results, setResults] = useState<ResultEntry[]>([])
  const [resultsVisible, setResultsVisible] = useState(true)
  const [resultsPct, setResultsPct] = useState(55)
  const dragging = useRef(false)
  const containerRef = useRef<HTMLDivElement>(null)

  const handleDragStart = (e: React.MouseEvent) => {
    e.preventDefault()
    dragging.current = true
    const onMove = (ev: MouseEvent) => {
      if (!dragging.current || !containerRef.current) return
      const rect = containerRef.current.getBoundingClientRect()
      const pct = ((ev.clientX - rect.left) / rect.width) * 100
      setResultsPct(100 - Math.min(80, Math.max(20, pct)))
    }
    const onUp = () => {
      dragging.current = false
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }

  const addResult = useCallback<AddResultFn>((label, code, result) => {
    const id = `r-${Date.now()}-${Math.random()}`
    setResults((prev) => [...prev, { id, label, code, result, timestamp: Date.now() }])
    return id
  }, [])

  if (!loaded) {
    return (
      <div className="h-screen flex items-center justify-center bg-white">
        <div className="text-gray-400 text-sm">Loading...</div>
      </div>
    )
  }

  const hasApiKey = settings[settings.activeProvider].apiKey.length > 0

  return (
    <div className="h-screen flex flex-col bg-white">
      {/* Title bar */}
      <header
        className="flex items-center justify-between pl-20 pr-4 py-2.5 border-b border-gray-200 bg-white/80 backdrop-blur-xl shrink-0"
        style={{ WebkitAppRegion: 'drag' } as any}
      >
        <div className="flex items-center gap-3" style={{ WebkitAppRegion: 'no-drag' } as any}>
          <h1 className="text-sm font-semibold tracking-tight">
            <span className="text-gray-900">blazer</span>
            <span className="text-gray-400 font-normal ml-1">playground</span>
          </h1>
          <span className="text-xs px-2.5 py-0.5 rounded-full font-medium bg-gray-100 text-gray-600 border border-gray-200">
            {settings.activeProvider === 'openai' ? 'OpenAI' : 'Claude'}
            {' · '}
            {settings[settings.activeProvider].model}
          </span>
        </div>
        <div className="flex items-center gap-1.5" style={{ WebkitAppRegion: 'no-drag' } as any}>
          {!hasApiKey && (
            <span className="text-xs text-orange-500 mr-2">Set API key in settings</span>
          )}
          <button
            onClick={() => chat.clearMessages()}
            className="text-xs text-gray-500 hover:text-gray-900 px-2.5 py-1 rounded-md hover:bg-gray-100 transition"
          >
            Clear
          </button>
          <button
            onClick={() => window.blazerAPI.toggleDevTools()}
            className="text-gray-500 hover:text-gray-900 p-1.5 rounded-md hover:bg-gray-100 transition"
            title="Toggle DevTools"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>
            </svg>
          </button>
          <button
            onClick={() => setSettingsOpen(true)}
            className="text-gray-500 hover:text-gray-900 p-1.5 rounded-md hover:bg-gray-100 transition"
            title="Settings"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/>
            </svg>
          </button>
        </div>
      </header>

      {/* Body: chat left, results right */}
      <div className="flex-1 flex min-h-0" ref={containerRef}>
        {/* Chat panel */}
        <div
          className="flex flex-col min-h-0"
          style={{ width: resultsVisible ? `${100 - resultsPct}%` : '100%' }}
        >
          <ChatContainer
            messages={chat.messages}
            isStreaming={chat.isStreaming}
            onSendMessage={chat.sendMessage}
            onAddResult={addResult}
            preferredLanguage={settings.execution.preferredLanguage}
            loadedFiles={chat.loadedFiles}
            onRemoveFile={chat.removeFile}
            onReplaceFile={chat.replaceFile}
          />
        </div>

        {resultsVisible && (
          <>
            {/* Drag handle */}
            <div
              className="w-1 cursor-col-resize bg-gray-200 hover:bg-blue-400 transition-colors shrink-0 active:bg-blue-500"
              onMouseDown={handleDragStart}
            />
            {/* Results panel */}
            <div className="flex flex-col min-h-0" style={{ width: `${resultsPct}%` }}>
              <ResultsPanel
                results={results}
                onClear={() => setResults([])}
                onHide={() => setResultsVisible(false)}
              />
            </div>
          </>
        )}

        {/* Show results button when hidden */}
        {!resultsVisible && (
          <button
            onClick={() => setResultsVisible(true)}
            className="shrink-0 w-8 flex flex-col items-center justify-center border-l border-gray-200 bg-gray-50 hover:bg-gray-100 text-gray-400 hover:text-gray-700 transition gap-1"
            title="Show results"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="15 18 9 12 15 6"/>
            </svg>
            <span className="text-xs font-medium" style={{ writingMode: 'vertical-rl', transform: 'rotate(180deg)' }}>Results</span>
            {results.length > 0 && (
              <span className="text-xs bg-gray-200 text-gray-600 px-1 py-0.5 rounded-full font-medium">{results.length}</span>
            )}
          </button>
        )}
      </div>

      {settingsOpen && (
        <SettingsPanel
          settings={settings}
          onUpdate={updateSettings}
          onClose={() => setSettingsOpen(false)}
        />
      )}
    </div>
  )
}
