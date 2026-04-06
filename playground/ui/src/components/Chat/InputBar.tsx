import React, { useState, useRef, useEffect } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { DataExplorer } from '../DataExplorer'
import type { AttachedFile, ConnectionAlias } from '../../lib/types'
import type { Skill } from '../../lib/skills'

interface FileInfo {
  path: string
  name: string
  ext: string
  columns?: string[]
}

interface Props {
  onSend: (content: string, attachments?: AttachedFile[], perMessageSkillIds?: string[]) => void
  onClear: () => void
  disabled: boolean
  loadedFiles: AttachedFile[]
  onRemoveFile: (path: string) => void
  onReplaceFile: (oldPath: string, newFile: AttachedFile) => void
  /** When set, auto-fills the textarea and focuses it, then clears itself */
  prefill?: string
  onPrefillConsumed?: () => void
  availableSkills?: Skill[]
  /** All configured connection aliases from settings */
  availableConnections?: ConnectionAlias[]
  /** Currently active connections for this chat */
  activeConnections?: ConnectionAlias[]
  onAddConnection?: (conn: ConnectionAlias) => void
  onRemoveConnection?: (id: string) => void
}

export function InputBar({ onSend, onClear, disabled, loadedFiles, onRemoveFile, onReplaceFile, prefill, onPrefillConsumed, availableSkills = [], availableConnections = [], activeConnections = [], onAddConnection, onRemoveConnection }: Props) {
  const [input, setInput] = useState('')
  const [pendingFiles, setPendingFiles] = useState<AttachedFile[]>([])
  const [converting, setConverting] = useState<Set<string>>(new Set())
  const [explorerFile, setExplorerFile] = useState<AttachedFile | null>(null)

  // Per-message skill state
  const [activeSkillIds, setActiveSkillIds] = useState<string[]>([])
  const [skillFilter, setSkillFilter] = useState<string | null>(null)
  const [skillPickerIdx, setSkillPickerIdx] = useState(0)
  const skillPickerRef = useRef<HTMLDivElement>(null)
  const [connPickerOpen, setConnPickerOpen] = useState(false)
  const connPickerRef = useRef<HTMLDivElement>(null)

  const textareaRef = useRef<HTMLTextAreaElement>(null)

  // Prefill from external source (e.g. "Ask AI" from error/result card) — always APPENDS
  useEffect(() => {
    if (prefill) {
      setInput(prev => prev.trim() ? `${prev.trim()}\n\n${prefill}` : prefill)
      setTimeout(() => {
        textareaRef.current?.focus()
        const len = textareaRef.current?.value.length ?? 0
        textareaRef.current?.setSelectionRange(len, len)
      }, 50)
      onPrefillConsumed?.()
    }
  }, [prefill]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 200) + 'px'
    }
  }, [input])

  // Scroll active skill picker item into view
  useEffect(() => {
    if (skillFilter === null) return
    const el = skillPickerRef.current?.children[skillPickerIdx] as HTMLElement | undefined
    el?.scrollIntoView({ block: 'nearest' })
  }, [skillPickerIdx, skillFilter])

  // Close connection picker when clicking outside
  useEffect(() => {
    if (!connPickerOpen) return
    const handler = (e: MouseEvent) => {
      if (connPickerRef.current && !connPickerRef.current.contains(e.target as Node)) {
        setConnPickerOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [connPickerOpen])

  // Filtered skills for the picker
  const filteredSkills = skillFilter === null
    ? []
    : availableSkills.filter((s) => {
        const q = skillFilter.toLowerCase()
        return (
          s.name.toLowerCase().includes(q) ||
          s.id.toLowerCase().includes(q) ||
          s.description.toLowerCase().includes(q)
        )
      })

  const selectSkill = (skill: Skill) => {
    if (!activeSkillIds.includes(skill.id)) {
      setActiveSkillIds((prev) => [...prev, skill.id])
    }
    // Remove the trailing # + filter text from input
    setInput((prev) => prev.replace(/#\w*$/, ''))
    setSkillFilter(null)
    textareaRef.current?.focus()
  }

  const removeActiveSkill = (id: string) => {
    setActiveSkillIds((prev) => prev.filter((s) => s !== id))
  }

  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const val = e.target.value
    setInput(val)
    // Detect # at end of input (may have typed more chars after #)
    const match = val.match(/#(\w*)$/)
    if (match) {
      setSkillFilter(match[1])
      setSkillPickerIdx(0)
    } else {
      setSkillFilter(null)
    }
  }

  const handleSubmit = () => {
    const trimmed = input.trim()
    if ((!trimmed && pendingFiles.length === 0) || disabled) return
    const message = pendingFiles.length > 0 && !trimmed
      ? `I've attached ${pendingFiles.map((f) => f.name).join(', ')}. Please explore this data.`
      : trimmed
    onSend(
      message,
      pendingFiles.length > 0 ? pendingFiles : undefined,
      activeSkillIds.length > 0 ? activeSkillIds : undefined,
    )
    setInput('')
    setPendingFiles([])
    setActiveSkillIds([])
    setSkillFilter(null)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    // Skill picker keyboard nav takes priority
    if (skillFilter !== null && filteredSkills.length > 0) {
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setSkillPickerIdx((i) => Math.min(i + 1, filteredSkills.length - 1))
        return
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault()
        setSkillPickerIdx((i) => Math.max(i - 1, 0))
        return
      }
      if (e.key === 'Enter') {
        e.preventDefault()
        selectSkill(filteredSkills[skillPickerIdx])
        return
      }
      if (e.key === 'Escape') {
        e.preventDefault()
        setSkillFilter(null)
        return
      }
    }
    if (e.key === 'Escape' && skillFilter !== null) {
      setSkillFilter(null)
      return
    }
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSubmit() }
  }

  const handleAttach = async () => {
    const results = await invoke<FileInfo[]>('open_file_dialog').catch(() => [])
    if (!results.length) return
    const files: AttachedFile[] = results.map((r) => ({
      path: r.path, name: r.name, ext: r.ext, columns: r.columns,
    }))
    setPendingFiles((prev) => [...prev, ...files])
  }

  const handleAttachFolder = async () => {
    const result = await invoke<FileInfo | null>('open_folder_dialog').catch(() => null)
    if (!result) return
    setPendingFiles((prev) => [...prev, { path: result.path, name: result.name, ext: 'parquet_dir' }])
  }

  const convertFile = async (file: AttachedFile, onSuccess: (newFile: AttachedFile) => void) => {
    setConverting((prev) => new Set(prev).add(file.path))
    try {
      const newPath = await invoke<string>('convert_to_parquet', { csvPath: file.path })
      const parts = newPath.split('/')
      const newFile: AttachedFile = { path: newPath, name: parts[parts.length - 1], ext: 'parquet' }
      onSuccess(newFile)
    } catch (e: any) {
      alert(`Conversion failed: ${e}`)
    } finally {
      setConverting((prev) => { const s = new Set(prev); s.delete(file.path); return s })
    }
  }

  const extColor = (ext: string) => {
    if (ext === 'csv' || ext === 'tsv' || ext === 'csv_dir') return 'bg-green-50 text-green-700 border-green-200'
    if (ext === 'parquet' || ext === 'parquet_dir') return 'bg-blue-50 text-blue-700 border-blue-200'
    if (ext === 'xlsx' || ext === 'xlsx_dir') return 'bg-emerald-50 text-emerald-700 border-emerald-200'
    return 'bg-gray-50 text-gray-700 border-gray-200'
  }

  const renderFileBadge = (
    f: AttachedFile,
    onRemove: () => void,
    onConvert?: () => void,
    onExplore?: () => void,
  ) => (
    <span key={f.path} className={`inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-md border ${extColor(f.ext)}`}>
      <button
        onClick={onExplore}
        className="font-medium hover:underline underline-offset-2 cursor-pointer"
        title={onExplore ? `Explore ${f.name} (schema + data)` : f.path}
        disabled={!onExplore}
      >
        {f.name}
      </button>
      {onConvert && (f.ext === 'csv' || f.ext === 'tsv') && (
        <button onClick={onConvert} disabled={converting.has(f.path)} className="opacity-60 hover:opacity-100 ml-0.5 text-blue-600 hover:text-blue-800 disabled:opacity-30" title="Convert to Parquet">
          {converting.has(f.path) ? (
            <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="animate-spin">
              <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
            </svg>
          ) : (
            <>
              <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/>
              </svg>
              <span>parquet</span>
            </>
          )}
        </button>
      )}
      <button onClick={onRemove} className="opacity-50 hover:opacity-100 ml-0.5 flex items-center" title="Remove">
        <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
      </button>
    </span>
  )

  return (
    <>
    <div className="border-t border-gray-200 dark:border-gray-700 px-4 py-3 bg-white dark:bg-gray-900 shrink-0">
      {loadedFiles.length > 0 && (
        <div className="flex items-center gap-1.5 mb-2 max-w-4xl mx-auto flex-wrap">
          <span className="text-xs text-gray-400 mr-1">Loaded:</span>
          {loadedFiles.map((f) => renderFileBadge(
            f,
            () => onRemoveFile(f.path),
            () => convertFile(f, (nf) => onReplaceFile(f.path, nf)),
            () => setExplorerFile(f),
          ))}
        </div>
      )}
      {pendingFiles.length > 0 && (
        <div className="flex items-center gap-1.5 mb-2 max-w-4xl mx-auto flex-wrap">
          <span className="text-xs text-gray-400 mr-1">Attaching:</span>
          {pendingFiles.map(f => renderFileBadge(
            f,
            () => setPendingFiles(p => p.filter(x => x.path !== f.path)),
            () => convertFile(f, nf => setPendingFiles(p => p.map(x => x.path === f.path ? nf : x))),
            () => setExplorerFile(f),
          ))}
        </div>
      )}

      {/* Active per-message skill chips */}
      {activeSkillIds.length > 0 && (
        <div className="flex items-center gap-1.5 mb-2 max-w-4xl mx-auto flex-wrap">
          <span className="text-xs text-gray-400 mr-1">Skills:</span>
          {activeSkillIds.map((id) => {
            const skill = availableSkills.find((s) => s.id === id)
            return (
              <span key={id} className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-md border bg-violet-50 text-violet-700 border-violet-200 dark:bg-violet-900/30 dark:text-violet-300 dark:border-violet-700">
                <span className="font-medium">{skill?.name ?? id}</span>
                <button
                  onClick={() => removeActiveSkill(id)}
                  className="opacity-50 hover:opacity-100 ml-0.5 flex items-center"
                  title="Remove skill"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                </button>
              </span>
            )
          })}
        </div>
      )}

      {/* Active connection chips */}
      {activeConnections.length > 0 && (
        <div className="flex items-center gap-1.5 mb-2 max-w-4xl mx-auto flex-wrap">
          <span className="text-xs text-gray-400 mr-1">Connected:</span>
          {activeConnections.map((conn) => (
            <span key={conn.id} className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-md border bg-amber-50 text-amber-700 border-amber-200">
              <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
              <span className="font-medium">{conn.name}</span>
              <button
                onClick={() => onRemoveConnection?.(conn.id)}
                className="opacity-50 hover:opacity-100 ml-0.5 flex items-center"
                title="Remove connection"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
              </button>
            </span>
          ))}
        </div>
      )}

      {/* Connection picker dropdown */}
      {connPickerOpen && (
        <div className="relative max-w-4xl mx-auto mb-1">
          <div
            ref={connPickerRef}
            className="absolute bottom-full left-0 mb-1 bg-white border border-gray-200 rounded-xl shadow-lg overflow-hidden z-50 min-w-[260px] max-h-56 overflow-y-auto"
          >
            {availableConnections.length === 0 ? (
              <div className="px-3 py-3 text-xs text-gray-400 text-center">
                <p>No connections configured.</p>
                <p className="mt-1">Add them in <span className="font-medium text-gray-600">Settings → Extensions → Connections</span></p>
              </div>
            ) : (
              availableConnections.map((conn) => {
                const isActive = activeConnections.some((c) => c.id === conn.id)
                return (
                  <button
                    key={conn.id}
                    onMouseDown={(e) => {
                      e.preventDefault()
                      if (isActive) {
                        onRemoveConnection?.(conn.id)
                      } else {
                        onAddConnection?.(conn)
                      }
                      setConnPickerOpen(false)
                    }}
                    className={`w-full text-left px-3 py-2 flex items-center gap-2.5 transition-colors ${
                      isActive ? 'bg-amber-50' : 'hover:bg-gray-50'
                    }`}
                  >
                    <div className={`w-5 h-5 rounded-full flex items-center justify-center shrink-0 ${isActive ? 'bg-amber-200' : 'bg-gray-100'}`}>
                      <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className={isActive ? 'text-amber-700' : 'text-gray-500'}><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="text-xs font-semibold text-gray-800 truncate">{conn.name}</div>
                      <div className="text-[11px] text-gray-400 truncate">{conn.ext_type}{conn.connection_string ? ` · ${conn.connection_string.replace(/:([^:@]+)@/, ':●●●@')}` : ''}</div>
                    </div>
                    {isActive && (
                      <span className="ml-auto shrink-0 text-[10px] text-amber-600 font-medium">active</span>
                    )}
                  </button>
                )
              })
            )}
          </div>
        </div>
      )}

      {/* Skill picker dropdown */}
      {skillFilter !== null && (
        <div className="relative max-w-4xl mx-auto mb-1">
          <div
            ref={skillPickerRef}
            className="absolute bottom-full left-0 right-0 mb-1 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl shadow-lg overflow-hidden z-50 max-h-56 overflow-y-auto"
          >
            {filteredSkills.length === 0 ? (
              <div className="px-3 py-2 text-xs text-gray-400 dark:text-gray-500">
                No skills match "{skillFilter}" — type to filter
              </div>
            ) : (
              filteredSkills.map((skill, i) => (
                <button
                  key={skill.id}
                  onMouseDown={(e) => { e.preventDefault(); selectSkill(skill) }}
                  className={`w-full text-left px-3 py-2 flex items-start gap-2 transition-colors ${
                    i === skillPickerIdx
                      ? 'bg-violet-50 dark:bg-violet-900/30'
                      : 'hover:bg-gray-50 dark:hover:bg-gray-700/50'
                  }`}
                >
                  <div className="shrink-0 mt-0.5">
                    <div className="w-5 h-5 rounded-full bg-violet-100 dark:bg-violet-800 flex items-center justify-center">
                      <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="text-violet-600 dark:text-violet-300">
                        <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
                      </svg>
                    </div>
                  </div>
                  <div className="min-w-0">
                    <div className="text-xs font-semibold text-gray-800 dark:text-gray-200">{skill.name}</div>
                    <div className="text-[11px] text-gray-400 dark:text-gray-500 truncate">{skill.description}</div>
                  </div>
                  {activeSkillIds.includes(skill.id) && (
                    <span className="ml-auto shrink-0 text-[10px] text-violet-500 dark:text-violet-400 font-medium">active</span>
                  )}
                </button>
              ))
            )}
            <div className="border-t border-gray-100 dark:border-gray-700 px-3 py-1.5 flex items-center gap-3">
              <span className="text-[10px] text-gray-400 dark:text-gray-500">↑↓ navigate</span>
              <span className="text-[10px] text-gray-400 dark:text-gray-500">↵ select</span>
              <span className="text-[10px] text-gray-400 dark:text-gray-500">esc close</span>
            </div>
          </div>
        </div>
      )}

      <div className="flex items-end gap-2 max-w-4xl mx-auto">
        <button onClick={handleAttach} disabled={disabled} className="p-2.5 text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-xl transition disabled:opacity-30 shrink-0" title="Attach data file (CSV, Parquet…)">
          <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/>
          </svg>
        </button>
        <button onClick={handleAttachFolder} disabled={disabled} className="p-2.5 text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-xl transition disabled:opacity-30 shrink-0" title="Attach partitioned Parquet folder">
          <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
          </svg>
        </button>
        {availableConnections.length > 0 && (
          <button
            onClick={() => setConnPickerOpen((p) => !p)}
            disabled={disabled}
            className={`relative p-2.5 rounded-xl transition disabled:opacity-30 shrink-0 ${
              activeConnections.length > 0
                ? 'text-amber-600 bg-amber-50 hover:bg-amber-100'
                : 'text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800'
            }`}
            title="Select database connections"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
            {activeConnections.length > 0 && (
              <span className="absolute -top-0.5 -right-0.5 w-3.5 h-3.5 bg-amber-500 text-white text-[9px] font-bold rounded-full flex items-center justify-center">
                {activeConnections.length}
              </span>
            )}
          </button>
        )}
        <div className="relative flex-1">
          <textarea
            ref={textareaRef}
            value={input}
            onChange={handleInputChange}
            onKeyDown={handleKeyDown}
            placeholder={activeSkillIds.length > 0 ? 'Message with active skills… (type # to add more)' : pendingFiles.length > 0 ? 'Ask about this data… (type # to add a skill)' : 'Ask about your data… (type # to add a skill)'}
            disabled={disabled}
            rows={1}
            className="w-full bg-gray-50 dark:bg-gray-800 text-gray-900 dark:text-gray-100 rounded-xl px-4 py-2.5 text-sm resize-none placeholder-gray-400 dark:placeholder-gray-600 border border-gray-200 dark:border-gray-700 focus:outline-none focus:ring-2 focus:ring-gray-900/10 dark:focus:ring-gray-100/10 focus:border-gray-300 dark:focus:border-gray-600 disabled:opacity-50 transition"
          />
          {activeSkillIds.length > 0 && (
            <div className="absolute right-2 bottom-2 flex items-center gap-0.5">
              <div className="w-4 h-4 rounded-full bg-violet-100 dark:bg-violet-800 flex items-center justify-center" title={`${activeSkillIds.length} skill${activeSkillIds.length > 1 ? 's' : ''} active`}>
                <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="text-violet-600 dark:text-violet-300">
                  <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
                </svg>
              </div>
            </div>
          )}
        </div>
        <button
          onClick={onClear}
          disabled={disabled}
          className="p-2.5 text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-xl transition disabled:opacity-30 shrink-0"
          title="Clear chat"
        >
          <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/>
          </svg>
        </button>
        <button
          onClick={handleSubmit}
          disabled={disabled || (!input.trim() && pendingFiles.length === 0)}
          className="px-4 py-2.5 bg-gray-900 dark:bg-white text-white dark:text-gray-900 rounded-xl text-sm font-medium hover:bg-gray-700 dark:hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed transition shrink-0"
        >
          Send
        </button>
      </div>
    </div>

    {explorerFile && (
      <DataExplorer
        file={explorerFile}
        onClose={() => setExplorerFile(null)}
      />
    )}
    </>
  )
}
