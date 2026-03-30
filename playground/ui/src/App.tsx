import React, { useState, useCallback, useRef } from 'react'
import { MessageList } from './components/Chat/MessageList'
import { InputBar } from './components/Chat/InputBar'
import { SettingsPanel } from './components/Settings/SettingsPanel'
import { useSettings } from './hooks/useSettings'
import { useChat } from './hooks/useChat'

export default function App() {
  const { settings, updateSettings, loaded } = useSettings()
  const { messages, sendMessage, isStreaming, addQueryResult, clearMessages, loadedFiles, replaceFile, removeFile } = useChat(settings)
  const [settingsOpen, setSettingsOpen] = useState(false)

  if (!loaded) {
    return (
      <div className="h-screen flex items-center justify-center bg-white">
        <div className="text-gray-400 text-sm">Loading…</div>
      </div>
    )
  }

  const hasApiKey = settings[settings.active_provider].api_key.length > 0
  const providerLabel = settings.active_provider === 'openai' ? 'OpenAI' : 'Claude'

  return (
    <div className="h-screen flex flex-col bg-white">
      {/* Title bar — draggable */}
      <header
        className="flex items-center justify-between pl-20 pr-4 py-2.5 border-b border-gray-200 bg-white/80 backdrop-blur-xl shrink-0"
        style={{ WebkitAppRegion: 'drag' } as React.CSSProperties}
      >
        <div className="flex items-center gap-3" style={{ WebkitAppRegion: 'no-drag' } as React.CSSProperties}>
          <h1 className="text-sm font-semibold tracking-tight">
            <span className="text-gray-900">blazer</span>
            <span className="text-gray-400 font-normal ml-1">playground</span>
          </h1>
          <span className="text-xs px-2.5 py-0.5 rounded-full font-medium bg-gray-100 text-gray-600 border border-gray-200">
            {providerLabel} · {settings[settings.active_provider].model}
          </span>
        </div>
        <div className="flex items-center gap-1.5" style={{ WebkitAppRegion: 'no-drag' } as React.CSSProperties}>
          {!hasApiKey && (
            <span className="text-xs text-orange-500 mr-2">Set API key in settings</span>
          )}
          <button
            onClick={clearMessages}
            className="text-xs text-gray-500 hover:text-gray-900 px-2.5 py-1 rounded-md hover:bg-gray-100 transition"
          >
            Clear
          </button>
          <button
            onClick={() => setSettingsOpen(true)}
            className="text-gray-500 hover:text-gray-900 p-1.5 rounded-md hover:bg-gray-100 transition"
            title="Settings"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="3"/>
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/>
            </svg>
          </button>
        </div>
      </header>

      {/* Chat */}
      <div className="flex-1 flex flex-col min-h-0">
        <MessageList
          messages={messages}
          isStreaming={isStreaming}
          onQueryResult={addQueryResult}
        />
        <InputBar
          onSend={sendMessage}
          disabled={isStreaming}
          loadedFiles={loadedFiles}
          onRemoveFile={removeFile}
          onReplaceFile={replaceFile}
        />
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
