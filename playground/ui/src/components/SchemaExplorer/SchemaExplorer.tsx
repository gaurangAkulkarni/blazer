import React, { useEffect, useState } from 'react'
import type { AttachedFile } from '../../lib/types'
import type { FileSchema, ColumnInfo } from '../../hooks/useSchema'
import type { FileProfile } from '../../hooks/useProfiler'
import { ProfilerView } from './ProfilerView'

interface Props {
  loadedFiles: AttachedFile[]
  schemas: Record<string, FileSchema>
  profiles: Record<string, FileProfile>
  onFetch: (file: AttachedFile) => void
  onFetchAll: (files: AttachedFile[]) => void
  onProfile: (file: AttachedFile) => void
}

// ── Type badge colour ─────────────────────────────────────────────────────────
function typeBadgeClass(type: string): string {
  const t = type.toUpperCase()
  if (/INT|BIGINT|HUGEINT|UBIGINT|TINYINT|SMALLINT/.test(t)) return 'bg-blue-50 text-blue-600 border-blue-100'
  if (/FLOAT|DOUBLE|DECIMAL|REAL|NUMERIC/.test(t))            return 'bg-violet-50 text-violet-600 border-violet-100'
  if (/VARCHAR|TEXT|STRING|CHAR/.test(t))                     return 'bg-gray-100 text-gray-500 border-gray-200'
  if (/TIMESTAMP|DATE|TIME|INTERVAL/.test(t))                 return 'bg-orange-50 text-orange-600 border-orange-100'
  if (/BOOL/.test(t))                                         return 'bg-green-50 text-green-600 border-green-100'
  if (/LIST|STRUCT|MAP|JSON/.test(t))                         return 'bg-pink-50 text-pink-600 border-pink-100'
  return 'bg-gray-100 text-gray-400 border-gray-200'
}

function shortType(type: string): string {
  return type
    .replace('VARCHAR', 'STR').replace('TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ')
    .replace('DOUBLE', 'DBL').replace('BIGINT', 'INT64').replace('INTEGER', 'INT32')
    .replace('HUGEINT', 'INT128').replace('BOOLEAN', 'BOOL').replace('FLOAT', 'FLT')
}

// ── Column row ────────────────────────────────────────────────────────────────
function ColumnRow({ col }: { col: ColumnInfo }) {
  const [copied, setCopied] = useState(false)
  const copy = () => {
    navigator.clipboard.writeText(col.name).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    })
  }
  return (
    <button onClick={copy} title={`${col.name} · ${col.type}\nClick to copy column name`}
      className="group/col w-full flex items-center justify-between px-3 py-1 hover:bg-blue-50/60 transition-colors text-left">
      <span className="flex items-center gap-1.5 min-w-0">
        <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          className="text-gray-300 shrink-0">
          <line x1="5" y1="12" x2="19" y2="12"/>
        </svg>
        <span className="text-xs text-gray-700 dark:text-gray-300 font-mono truncate">{col.name}</span>
        {copied && (
          <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"
            className="text-green-500 shrink-0">
            <polyline points="20 6 9 17 4 12"/>
          </svg>
        )}
      </span>
      <span className={`text-[10px] font-medium px-1.5 py-0 rounded border shrink-0 ml-2 ${typeBadgeClass(col.type)}`}>
        {shortType(col.type)}
      </span>
    </button>
  )
}

// ── File card ─────────────────────────────────────────────────────────────────
interface FileCardProps {
  file: AttachedFile
  schema?: FileSchema
  onFetch: () => void
  onProfile: () => void
}

function FileCard({ file, schema, onFetch, onProfile }: FileCardProps) {
  const [expanded, setExpanded] = useState(true)

  useEffect(() => {
    if (!schema) onFetch()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const extBadge = (file.ext || 'dir').toUpperCase()
  const extColor = /CSV|TSV/.test(extBadge) ? 'bg-green-100 text-green-700' : 'bg-blue-100 text-blue-700'

  return (
    <div className="border border-gray-100 dark:border-gray-700 rounded-lg overflow-hidden mb-2">
      {/* File header */}
      <div className="flex items-center gap-0 bg-gray-50 dark:bg-gray-800 hover:bg-gray-100/80 dark:hover:bg-gray-700/80 transition">
        <button onClick={() => setExpanded(v => !v)}
          className="flex items-center gap-2 px-3 py-2 flex-1 text-left min-w-0">
          <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
            className={`text-gray-400 shrink-0 transition-transform ${expanded ? '' : '-rotate-90'}`}>
            <polyline points="6 9 12 15 18 9"/>
          </svg>
          <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
            className="text-gray-400 shrink-0">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
            <polyline points="14 2 14 8 20 8"/>
          </svg>
          <span className="text-xs font-semibold text-gray-700 dark:text-gray-300 truncate">{file.name}</span>
          <span className={`text-[10px] font-bold px-1.5 rounded shrink-0 ${extColor}`}>{extBadge}</span>
        </button>

        {/* Profile button */}
        <button
          onClick={(e) => { e.stopPropagation(); onProfile() }}
          title="Profile this file"
          className="flex items-center gap-1 text-[11px] font-medium px-2.5 py-1.5 mr-2 rounded-md
            text-indigo-600 bg-indigo-50 hover:bg-indigo-100 border border-indigo-100 transition-colors shrink-0"
        >
          <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/>
            <line x1="6" y1="20" x2="6" y2="14"/><line x1="3" y1="20" x2="21" y2="20"/>
          </svg>
          Profile
        </button>
      </div>

      {/* Meta row */}
      {expanded && (
        <div className="px-3 py-1.5 bg-white dark:bg-gray-900 border-t border-gray-50 dark:border-gray-800 flex items-center gap-3 text-[11px] text-gray-400 dark:text-gray-500">
          {schema?.loading ? (
            <span className="flex items-center gap-1">
              <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none"
                stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"
                className="animate-spin">
                <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
              </svg>
              Loading schema…
            </span>
          ) : schema?.error ? (
            <span className="text-red-400 truncate">{schema.error}</span>
          ) : (
            <>
              {schema?.rowCount !== undefined && <span>{schema.rowCount.toLocaleString()} rows</span>}
              {schema?.columns.length ? <span>{schema.columns.length} columns</span> : null}
              <span className="ml-auto text-gray-300 truncate" title={file.path}>
                {file.path.split('/').slice(-2).join('/')}
              </span>
            </>
          )}
        </div>
      )}

      {/* Column list */}
      {expanded && schema && !schema.loading && schema.columns.length > 0 && (
        <div className="border-t border-gray-50 dark:border-gray-800 py-0.5 max-h-64 overflow-y-auto">
          {schema.columns.map(col => <ColumnRow key={col.name} col={col} />)}
        </div>
      )}
    </div>
  )
}

// ── Main explorer ─────────────────────────────────────────────────────────────
export function SchemaExplorer({ loadedFiles, schemas, profiles, onFetch, onFetchAll, onProfile }: Props) {
  const [search, setSearch] = useState('')
  // null = showing file list; string path = showing profiler for that file
  const [profilingPath, setProfilingPath] = useState<string | null>(null)

  useEffect(() => {
    if (loadedFiles.length > 0) onFetchAll(loadedFiles)
  }, [loadedFiles.map(f => f.path).join('|')]) // eslint-disable-line react-hooks/exhaustive-deps

  // If a profile is requested, switch to profiler view
  const handleProfile = (file: AttachedFile) => {
    setProfilingPath(file.path)
    onProfile(file)
  }

  // ── Profiler drill-down ───────────────────────────────────────────────────
  if (profilingPath) {
    const file = loadedFiles.find(f => f.path === profilingPath)
    const profile = profiles[profilingPath]

    // Show loading placeholder if profile not in state yet
    if (!profile || profile.loading) {
      return (
        <div className="flex flex-col flex-1 min-h-0">
          <div className="shrink-0 flex items-center gap-2 px-3 py-2 border-b border-gray-100 bg-white">
            <button onClick={() => setProfilingPath(null)}
              className="flex items-center gap-1 text-xs text-gray-400 hover:text-gray-700 transition">
              <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24"
                fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="15 18 9 12 15 6"/>
              </svg>
              Schema
            </button>
            <span className="text-gray-300">/</span>
            <span className="text-xs font-semibold text-gray-700 truncate">{file?.name ?? profilingPath}</span>
          </div>
          <div className="flex-1 flex flex-col items-center justify-center gap-3 text-gray-400">
            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"
              fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
              className="animate-spin text-indigo-500">
              <path d="M21 12a9 9 0 1 1-6.219-8.56"/>
            </svg>
            <p className="text-sm text-gray-500">Profiling…</p>
          </div>
        </div>
      )
    }

    return (
      <ProfilerView
        profile={profile}
        onBack={() => setProfilingPath(null)}
        onRefresh={() => file && onProfile(file)}
      />
    )
  }

  // ── Schema list view ──────────────────────────────────────────────────────
  const filtered = search.trim()
    ? loadedFiles.filter(f =>
        f.name.toLowerCase().includes(search.toLowerCase()) ||
        (schemas[f.path]?.columns ?? [])
          .some(c => c.name.toLowerCase().includes(search.toLowerCase()))
      )
    : loadedFiles

  if (loadedFiles.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center text-gray-400 gap-3 p-6 select-none">
        <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round" className="opacity-25">
          <ellipse cx="12" cy="5" rx="9" ry="3"/>
          <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
          <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
        </svg>
        <p className="text-xs text-center">No files loaded yet.<br/>Attach a file in the Chat tab.</p>
      </div>
    )
  }

  return (
    <div className="flex flex-col flex-1 min-h-0">
      {/* Toolbar */}
      <div className="shrink-0 flex items-center gap-2 px-3 py-2 border-b border-gray-100 dark:border-gray-800 bg-gray-50/60 dark:bg-gray-800/60">
        <div className="flex-1 relative">
          <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
            className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400">
            <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
          </svg>
          <input type="text" value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search columns…"
            className="w-full pl-6 pr-2 py-1 text-xs border border-gray-200 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400 transition" />
        </div>
        <button onClick={() => onFetchAll(loadedFiles)} title="Refresh all schemas"
          className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-200 transition">
          <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="23 4 23 10 17 10"/>
            <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
          </svg>
        </button>
      </div>

      {/* File cards */}
      <div className="flex-1 min-h-0 overflow-y-auto px-3 pt-3 pb-4">
        {filtered.map(file => (
          <FileCard key={file.path} file={file} schema={schemas[file.path]}
            onFetch={() => onFetch(file)} onProfile={() => handleProfile(file)} />
        ))}
        {filtered.length === 0 && search && (
          <p className="text-xs text-gray-400 text-center py-6">No columns matching "{search}"</p>
        )}
      </div>

      {/* Legend */}
      <div className="shrink-0 px-3 py-2 border-t border-gray-100 dark:border-gray-800 flex flex-wrap gap-x-3 gap-y-1">
        {[
          { label: 'INT',  cls: 'bg-blue-50 text-blue-600 border-blue-100' },
          { label: 'FLT',  cls: 'bg-violet-50 text-violet-600 border-violet-100' },
          { label: 'STR',  cls: 'bg-gray-100 text-gray-500 border-gray-200' },
          { label: 'TIME', cls: 'bg-orange-50 text-orange-600 border-orange-100' },
          { label: 'BOOL', cls: 'bg-green-50 text-green-600 border-green-100' },
        ].map(({ label, cls }) => (
          <span key={label} className={`text-[10px] font-medium px-1.5 rounded border ${cls}`}>{label}</span>
        ))}
        <span className="text-[10px] text-gray-400 ml-1">click column to copy name</span>
      </div>
    </div>
  )
}
