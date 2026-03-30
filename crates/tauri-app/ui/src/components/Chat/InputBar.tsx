import React, { useState, useRef, useEffect } from 'react'
import { invoke } from '@tauri-apps/api/core'
import type { AttachedFile } from '../../lib/types'

interface FileInfo {
  path: string
  name: string
  ext: string
  columns?: string[]
}

interface Props {
  onSend: (content: string, attachments?: AttachedFile[]) => void
  disabled: boolean
  loadedFiles: AttachedFile[]
  onRemoveFile: (path: string) => void
  onReplaceFile: (oldPath: string, newFile: AttachedFile) => void
}

export function InputBar({ onSend, disabled, loadedFiles, onRemoveFile, onReplaceFile }: Props) {
  const [input, setInput] = useState('')
  const [pendingFiles, setPendingFiles] = useState<AttachedFile[]>([])
  const [converting, setConverting] = useState<Set<string>>(new Set())
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 200) + 'px'
    }
  }, [input])

  const handleSubmit = () => {
    const trimmed = input.trim()
    if ((!trimmed && pendingFiles.length === 0) || disabled) return
    const message = pendingFiles.length > 0 && !trimmed
      ? `I've attached ${pendingFiles.map((f) => f.name).join(', ')}. Please explore this data.`
      : trimmed
    onSend(message, pendingFiles.length > 0 ? pendingFiles : undefined)
    setInput('')
    setPendingFiles([])
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
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
    if (ext === 'csv' || ext === 'tsv') return 'bg-green-50 text-green-700 border-green-200'
    if (ext === 'parquet' || ext === 'parquet_dir') return 'bg-blue-50 text-blue-700 border-blue-200'
    return 'bg-gray-50 text-gray-700 border-gray-200'
  }

  const renderFileBadge = (f: AttachedFile, onRemove: () => void, onConvert?: () => void) => (
    <span key={f.path} className={`inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-md border ${extColor(f.ext)}`} title={f.path}>
      <span className="font-medium">{f.name}</span>
      {onConvert && (f.ext === 'csv' || f.ext === 'tsv') && (
        <button onClick={onConvert} disabled={converting.has(f.path)} className="opacity-60 hover:opacity-100 ml-0.5 text-blue-600 hover:text-blue-800 disabled:opacity-30" title="Convert to Parquet">
          {converting.has(f.path) ? '⏳' : '⇒ parquet'}
        </button>
      )}
      <button onClick={onRemove} className="opacity-50 hover:opacity-100 ml-0.5">&times;</button>
    </span>
  )

  return (
    <div className="border-t border-gray-200 px-4 py-3 bg-white shrink-0">
      {loadedFiles.length > 0 && (
        <div className="flex items-center gap-1.5 mb-2 max-w-4xl mx-auto flex-wrap">
          <span className="text-xs text-gray-400 mr-1">Loaded:</span>
          {loadedFiles.map((f) => renderFileBadge(f, () => onRemoveFile(f.path), () => convertFile(f, (nf) => onReplaceFile(f.path, nf))))}
        </div>
      )}
      {pendingFiles.length > 0 && (
        <div className="flex items-center gap-1.5 mb-2 max-w-4xl mx-auto flex-wrap">
          <span className="text-xs text-gray-400 mr-1">Attaching:</span>
          {pendingFiles.map((f) => renderFileBadge(f, () => setPendingFiles((p) => p.filter((x) => x.path !== f.path)), () => convertFile(f, (nf) => setPendingFiles((p) => p.map((x) => x.path === f.path ? nf : x)))))}
        </div>
      )}

      <div className="flex items-end gap-2 max-w-4xl mx-auto">
        <button onClick={handleAttach} disabled={disabled} className="p-2.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-xl transition disabled:opacity-30 shrink-0" title="Attach data file (CSV, Parquet…)">
          <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/>
          </svg>
        </button>
        <button onClick={handleAttachFolder} disabled={disabled} className="p-2.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-xl transition disabled:opacity-30 shrink-0" title="Attach partitioned Parquet folder">
          <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
          </svg>
        </button>
        <textarea
          ref={textareaRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={pendingFiles.length > 0 ? 'Ask about this data…' : 'Ask about your data…'}
          disabled={disabled}
          rows={1}
          className="flex-1 bg-gray-50 text-gray-900 rounded-xl px-4 py-2.5 text-sm resize-none placeholder-gray-400 border border-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-900/10 focus:border-gray-300 disabled:opacity-50 transition"
        />
        <button
          onClick={handleSubmit}
          disabled={disabled || (!input.trim() && pendingFiles.length === 0)}
          className="px-4 py-2.5 bg-gray-900 text-white rounded-xl text-sm font-medium hover:bg-gray-700 disabled:opacity-30 disabled:cursor-not-allowed transition shrink-0"
        >
          Send
        </button>
      </div>
    </div>
  )
}
