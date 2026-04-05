import React, { useState, useCallback, useRef, useEffect } from 'react'
import type { QuerySnippet, SnippetGroup } from '../../lib/types'
import type { Engine } from '../../hooks/useChat'

// ── Special group filter IDs (UI only, not persisted) ─────────────────────────
const GROUP_ALL     = '__all__'
const GROUP_DEFAULT = '__default__'

interface Props {
  snippets: QuerySnippet[]
  groups: SnippetGroup[]
  onRemove: (id: string) => void
  onUpdate: (id: string, updates: Partial<Omit<QuerySnippet, 'id' | 'createdAt'>>) => void
  onClear: () => void
  onAddGroup: (name: string) => SnippetGroup
  onRenameGroup: (id: string, name: string) => void
  onRemoveGroup: (id: string) => void
  onLoadToConsole: (engine: Engine, query: string) => void
  onSendToChat: (text: string) => void
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function timeAgo(ts: number): string {
  const diff = Date.now() - ts
  const s = Math.floor(diff / 1000)
  if (s < 5) return 'just now'
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}

function queryPreview(query: string): string {
  const first = query.trim().split('\n')[0] ?? ''
  return first.length > 68 ? first.slice(0, 65) + '…' : first
}

// ── Main component ────────────────────────────────────────────────────────────

export function SnippetsLibrary({
  snippets, groups,
  onRemove, onUpdate, onClear,
  onAddGroup, onRenameGroup, onRemoveGroup,
  onLoadToConsole, onSendToChat,
}: Props) {
  const [selectedGroup, setSelectedGroup] = useState<string>(GROUP_ALL)
  const [search, setSearch] = useState('')
  const [copiedId, setCopiedId] = useState<string | null>(null)
  const [addingGroup, setAddingGroup] = useState(false)
  const [newGroupName, setNewGroupName] = useState('')

  // When a group is deleted and we were viewing it, fall back to All
  useEffect(() => {
    if (selectedGroup !== GROUP_ALL && selectedGroup !== GROUP_DEFAULT) {
      if (!groups.find((g) => g.id === selectedGroup)) {
        setSelectedGroup(GROUP_ALL)
      }
    }
  }, [groups, selectedGroup])

  const handleCopy = useCallback((snippet: QuerySnippet) => {
    navigator.clipboard.writeText(snippet.query).then(() => {
      setCopiedId(snippet.id)
      setTimeout(() => setCopiedId(null), 1500)
    })
  }, [])

  const commitNewGroup = () => {
    if (newGroupName.trim()) {
      const g = onAddGroup(newGroupName.trim())
      setSelectedGroup(g.id)
    }
    setNewGroupName('')
    setAddingGroup(false)
  }

  // Filter snippets by group
  const groupFiltered = snippets.filter((s) => {
    if (selectedGroup === GROUP_ALL) return true
    if (selectedGroup === GROUP_DEFAULT) return !s.groupId
    return s.groupId === selectedGroup
  })

  // Then filter by search
  const filtered = search.trim()
    ? groupFiltered.filter(
        (s) =>
          s.name.toLowerCase().includes(search.toLowerCase()) ||
          s.query.toLowerCase().includes(search.toLowerCase()) ||
          (s.description ?? '').toLowerCase().includes(search.toLowerCase()),
      )
    : groupFiltered

  const defaultCount = snippets.filter((s) => !s.groupId).length

  return (
    <div className="flex h-full min-h-0">
      {/* ── Groups sidebar ─────────────────────────────────────────────────── */}
      <div className="w-[128px] shrink-0 flex flex-col border-r border-gray-100 dark:border-gray-800 bg-gray-50/40 dark:bg-gray-800/40 min-h-0">
        <div className="shrink-0 px-2 pt-2 pb-1">
          <span className="text-[10px] font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wider px-1">Folders</span>
        </div>

        <div className="flex-1 overflow-y-auto min-h-0 pb-1">
          {/* All */}
          <GroupRow
            label="All"
            count={snippets.length}
            active={selectedGroup === GROUP_ALL}
            icon="all"
            onClick={() => setSelectedGroup(GROUP_ALL)}
          />
          {/* Default */}
          <GroupRow
            label="Default"
            count={defaultCount}
            active={selectedGroup === GROUP_DEFAULT}
            icon="default"
            onClick={() => setSelectedGroup(GROUP_DEFAULT)}
          />
          {/* Divider */}
          {groups.length > 0 && (
            <div className="mx-2 my-1 border-t border-gray-200 dark:border-gray-700" />
          )}
          {/* Named groups */}
          {groups.map((g) => (
            <EditableGroupRow
              key={g.id}
              group={g}
              count={snippets.filter((s) => s.groupId === g.id).length}
              active={selectedGroup === g.id}
              onClick={() => setSelectedGroup(g.id)}
              onRename={onRenameGroup}
              onDelete={() => onRemoveGroup(g.id)}
            />
          ))}
        </div>

        {/* New folder */}
        <div className="shrink-0 px-2 pb-2">
          {addingGroup ? (
            <div className="flex flex-col gap-1">
              <input
                autoFocus
                value={newGroupName}
                onChange={(e) => setNewGroupName(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') commitNewGroup()
                  if (e.key === 'Escape') { setAddingGroup(false); setNewGroupName('') }
                }}
                placeholder="Folder name…"
                className="text-xs w-full border border-gray-300 dark:border-gray-600 rounded px-1.5 py-1 focus:outline-none focus:border-indigo-400 text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800"
              />
              <div className="flex gap-1">
                <button
                  onClick={commitNewGroup}
                  disabled={!newGroupName.trim()}
                  className="flex-1 text-[10px] font-medium bg-indigo-600 text-white py-0.5 rounded hover:bg-indigo-700 disabled:opacity-40 transition"
                >
                  Create
                </button>
                <button
                  onClick={() => { setAddingGroup(false); setNewGroupName('') }}
                  className="text-[10px] text-gray-500 px-1.5 py-0.5 rounded hover:bg-gray-200 transition"
                >
                  ✕
                </button>
              </div>
            </div>
          ) : (
            <button
              onClick={() => setAddingGroup(true)}
              className="w-full flex items-center gap-1.5 text-[11px] text-gray-400 dark:text-gray-500 hover:text-indigo-600 px-1 py-1 rounded hover:bg-white dark:hover:bg-gray-700 hover:shadow-sm transition"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
              </svg>
              New folder
            </button>
          )}
        </div>
      </div>

      {/* ── Snippet list ───────────────────────────────────────────────────── */}
      <div className="flex-1 flex flex-col min-h-0 min-w-0">
        {snippets.length === 0 ? (
          <EmptyState />
        ) : (
          <>
            {/* Search + clear */}
            <div className="shrink-0 flex items-center gap-2 px-3 py-2 border-b border-gray-100 dark:border-gray-800 bg-white dark:bg-gray-900">
              <div className="flex-1 relative">
                <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400">
                  <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                </svg>
                <input
                  type="text"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search…"
                  className="w-full text-xs pl-6 pr-2 py-1 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 focus:outline-none focus:border-gray-400 text-gray-700 dark:text-gray-300 placeholder:text-gray-300 dark:placeholder:text-gray-600"
                />
              </div>
              <span className="text-xs text-gray-400 shrink-0">{filtered.length}/{groupFiltered.length}</span>
              {snippets.length > 0 && (
                <button
                  onClick={onClear}
                  className="text-xs text-gray-400 hover:text-red-500 px-1.5 py-0.5 rounded hover:bg-gray-100 transition shrink-0"
                  title="Delete all snippets"
                >
                  Clear all
                </button>
              )}
            </div>

            {/* Cards */}
            <div className="flex-1 min-h-0 overflow-y-auto divide-y divide-gray-100 dark:divide-gray-800">
              {filtered.length === 0 ? (
                <p className="text-xs text-gray-400 text-center py-8">
                  {search.trim() ? 'No matches' : 'No snippets in this folder'}
                </p>
              ) : (
                filtered.map((snippet) => (
                  <SnippetCard
                    key={snippet.id}
                    snippet={snippet}
                    groups={groups}
                    isCopied={copiedId === snippet.id}
                    onRemove={onRemove}
                    onUpdate={onUpdate}
                    onLoadToConsole={onLoadToConsole}
                    onSendToChat={onSendToChat}
                    onCopy={handleCopy}
                  />
                ))
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

// ── Empty state ───────────────────────────────────────────────────────────────

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center h-full text-center px-6 py-12 gap-3">
      <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="text-gray-300">
        <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>
      </svg>
      <p className="text-xs text-gray-400 max-w-[180px]">
        No snippets yet. Save any query using the bookmark icon in a query block or the Console toolbar.
      </p>
    </div>
  )
}

// ── Group sidebar rows ────────────────────────────────────────────────────────

interface GroupRowProps {
  label: string
  count: number
  active: boolean
  icon: 'all' | 'default'
  onClick: () => void
}

function GroupRow({ label, count, active, icon, onClick }: GroupRowProps) {
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center gap-1.5 px-2 py-1.5 text-left transition-colors text-xs ${
        active ? 'bg-indigo-50 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-400 font-medium' : 'text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-900 dark:hover:text-gray-100'
      }`}
    >
      {icon === 'all' ? (
        <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0 opacity-60">
          <rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>
        </svg>
      ) : (
        <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0 opacity-60">
          <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>
        </svg>
      )}
      <span className="truncate flex-1">{label}</span>
      {count > 0 && (
        <span className={`text-[10px] rounded-full px-1.5 shrink-0 ${active ? 'bg-indigo-100 dark:bg-indigo-900/40 text-indigo-600 dark:text-indigo-400' : 'bg-gray-200 dark:bg-gray-700 text-gray-500 dark:text-gray-400'}`}>
          {count}
        </span>
      )}
    </button>
  )
}

interface EditableGroupRowProps {
  group: SnippetGroup
  count: number
  active: boolean
  onClick: () => void
  onRename: (id: string, name: string) => void
  onDelete: () => void
}

function EditableGroupRow({ group, count, active, onClick, onRename, onDelete }: EditableGroupRowProps) {
  const [editing, setEditing] = useState(false)
  const [editName, setEditName] = useState(group.name)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => { if (editing) inputRef.current?.focus() }, [editing])

  const commit = () => {
    if (editName.trim() && editName.trim() !== group.name) {
      onRename(group.id, editName.trim())
    } else {
      setEditName(group.name)
    }
    setEditing(false)
  }

  if (editing) {
    return (
      <div className="px-2 py-1" onClick={(e) => e.stopPropagation()}>
        <input
          ref={inputRef}
          value={editName}
          onChange={(e) => setEditName(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') commit(); if (e.key === 'Escape') { setEditName(group.name); setEditing(false) } }}
          onBlur={commit}
          className="w-full text-xs border border-indigo-300 rounded px-1.5 py-0.5 focus:outline-none bg-white dark:bg-gray-800 text-gray-800 dark:text-gray-200"
        />
      </div>
    )
  }

  return (
    <div
      className={`group w-full flex items-center gap-1 px-2 py-1.5 cursor-pointer transition-colors text-xs ${
        active ? 'bg-indigo-50 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-400 font-medium' : 'text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-900 dark:hover:text-gray-100'
      }`}
      onClick={onClick}
    >
      <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill={active ? '#c7d2fe' : 'none'} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0">
        <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
      </svg>
      <span className="truncate flex-1 text-[11px]">{group.name}</span>
      {count > 0 && (
        <span className={`text-[10px] rounded-full px-1.5 shrink-0 ${active ? 'bg-indigo-100 dark:bg-indigo-900/40 text-indigo-600 dark:text-indigo-400' : 'bg-gray-200 dark:bg-gray-700 text-gray-500 dark:text-gray-400'} group-hover:hidden`}>
          {count}
        </span>
      )}
      {/* Hover actions */}
      <div
        className="hidden group-hover:flex items-center gap-0.5 shrink-0"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={() => setEditing(true)}
          title="Rename"
          className="p-0.5 rounded hover:bg-white/80 text-gray-400 hover:text-gray-700 transition"
        >
          <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
          </svg>
        </button>
        <button
          onClick={onDelete}
          title="Delete folder (snippets moved to Default)"
          className="p-0.5 rounded hover:bg-white/80 text-gray-400 hover:text-red-500 transition"
        >
          <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
          </svg>
        </button>
      </div>
    </div>
  )
}

// ── Snippet card ──────────────────────────────────────────────────────────────

interface CardProps {
  snippet: QuerySnippet
  groups: SnippetGroup[]
  isCopied: boolean
  onRemove: (id: string) => void
  onUpdate: (id: string, updates: Partial<Omit<QuerySnippet, 'id' | 'createdAt'>>) => void
  onLoadToConsole: (engine: Engine, query: string) => void
  onSendToChat: (text: string) => void
  onCopy: (snippet: QuerySnippet) => void
}

function SnippetCard({ snippet, groups, isCopied, onRemove, onUpdate, onLoadToConsole, onSendToChat, onCopy }: CardProps) {
  const [expanded, setExpanded] = useState(false)
  const [editing, setEditing] = useState(false)
  const [editName, setEditName] = useState(snippet.name)
  const [editDesc, setEditDesc] = useState(snippet.description ?? '')
  const [showMoveMenu, setShowMoveMenu] = useState(false)
  const moveRef = useRef<HTMLDivElement>(null)

  // Close move menu on outside click
  useEffect(() => {
    if (!showMoveMenu) return
    const handler = (e: MouseEvent) => {
      if (moveRef.current && !moveRef.current.contains(e.target as Node)) setShowMoveMenu(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [showMoveMenu])

  const commitEdit = () => {
    if (editName.trim()) {
      onUpdate(snippet.id, { name: editName.trim(), description: editDesc.trim() || undefined })
    }
    setEditing(false)
  }

  const cancelEdit = () => {
    setEditName(snippet.name); setEditDesc(snippet.description ?? '')
    setEditing(false)
  }

  const groupName = groups.find((g) => g.id === snippet.groupId)?.name

  return (
    <div className="group px-3 py-2.5 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
      {/* Top row: engine badge + name + time */}
      <div className="flex items-start gap-1.5">
        <span className={`shrink-0 mt-0.5 inline-flex items-center text-[10px] font-semibold px-1.5 py-0.5 rounded-sm ${
          snippet.engine === 'blazer' ? 'bg-violet-100 text-violet-700' : 'bg-yellow-100 text-yellow-700'
        }`}>
          {snippet.engine === 'blazer' ? 'Blazer' : 'DuckDB'}
        </span>

        {editing ? (
          <div className="flex-1 flex flex-col gap-1.5" onClick={(e) => e.stopPropagation()}>
            <input
              autoFocus
              value={editName}
              onChange={(e) => setEditName(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') commitEdit(); if (e.key === 'Escape') cancelEdit() }}
              className="text-xs font-semibold text-gray-900 bg-white border border-gray-300 rounded px-1.5 py-0.5 focus:outline-none focus:border-indigo-400"
              placeholder="Snippet name"
            />
            <input
              value={editDesc}
              onChange={(e) => setEditDesc(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') commitEdit(); if (e.key === 'Escape') cancelEdit() }}
              className="text-xs text-gray-500 bg-white border border-gray-200 rounded px-1.5 py-0.5 focus:outline-none focus:border-indigo-400"
              placeholder="Description (optional)"
            />
            <div className="flex items-center gap-1.5">
              <button onClick={commitEdit} className="text-[10px] font-medium bg-indigo-600 text-white px-2 py-0.5 rounded hover:bg-indigo-700 transition">Save</button>
              <button onClick={cancelEdit} className="text-[10px] text-gray-500 hover:text-gray-700 px-2 py-0.5 rounded hover:bg-gray-200 transition">Cancel</button>
            </div>
          </div>
        ) : (
          <div className="flex-1 min-w-0">
            <div className="flex items-center justify-between gap-1">
              <span
                className="text-xs font-semibold text-gray-800 dark:text-gray-200 truncate cursor-pointer"
                onClick={() => setExpanded((v) => !v)}
                title={snippet.name}
              >
                {snippet.name}
              </span>
              <span className="text-[10px] text-gray-400 shrink-0">{timeAgo(snippet.createdAt)}</span>
            </div>
            <div className="flex items-center gap-1.5 mt-0.5">
              {snippet.description && (
                <p className="text-[10.5px] text-gray-500 truncate flex-1">{snippet.description}</p>
              )}
              {groupName && (
                <span className="shrink-0 text-[9px] font-medium bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded-full truncate max-w-[70px]" title={groupName}>
                  {groupName}
                </span>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Query preview / full */}
      {!editing && (
        <div
          className="font-mono text-[11px] text-gray-600 dark:text-gray-400 leading-relaxed mt-1.5 cursor-pointer"
          onClick={() => setExpanded((v) => !v)}
        >
          {expanded ? (
            <pre className="whitespace-pre-wrap break-words bg-gray-50 dark:bg-gray-800 rounded p-2 text-[10.5px]">
              {snippet.query}
            </pre>
          ) : (
            <span className="text-gray-500">{queryPreview(snippet.query)}</span>
          )}
        </div>
      )}

      {/* Action row */}
      {!editing && (
        <div
          className="flex items-center gap-1 mt-1.5 opacity-0 group-hover:opacity-100 transition-opacity"
          onClick={(e) => e.stopPropagation()}
        >
          <button
            onClick={() => onLoadToConsole(snippet.engine as Engine, snippet.query)}
            className="flex items-center gap-1 text-[10px] text-gray-500 hover:text-gray-900 px-1.5 py-0.5 rounded hover:bg-gray-200 transition"
            title="Load to Console"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/>
            </svg>
            Console
          </button>
          <button
            onClick={() => onSendToChat(`Run this query:\n\`\`\`${snippet.engine === 'blazer' ? 'json' : 'sql'}\n${snippet.query}\n\`\`\``)}
            className="flex items-center gap-1 text-[10px] text-gray-500 hover:text-gray-900 px-1.5 py-0.5 rounded hover:bg-gray-200 transition"
            title="Send to AI Chat"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
            </svg>
            Chat
          </button>
          <button
            onClick={() => onCopy(snippet)}
            className="flex items-center gap-1 text-[10px] text-gray-500 hover:text-gray-900 px-1.5 py-0.5 rounded hover:bg-gray-200 transition"
          >
            {isCopied ? (
              <><svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>Copied</>
            ) : (
              <><svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>Copy</>
            )}
          </button>

          {/* Move to folder */}
          {groups.length > 0 && (
            <div className="relative" ref={moveRef}>
              <button
                onClick={() => setShowMoveMenu((v) => !v)}
                className="flex items-center gap-1 text-[10px] text-gray-500 hover:text-gray-900 px-1.5 py-0.5 rounded hover:bg-gray-200 transition"
                title="Move to folder"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
                </svg>
                Move
              </button>
              {showMoveMenu && (
                <div className="absolute bottom-full left-0 mb-1 z-20 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg py-1 min-w-[130px]">
                  <button
                    onClick={() => { onUpdate(snippet.id, { groupId: undefined }); setShowMoveMenu(false) }}
                    className={`w-full text-left px-3 py-1 text-[11px] hover:bg-gray-50 dark:hover:bg-gray-700 transition ${!snippet.groupId ? 'font-semibold text-indigo-600' : 'text-gray-700 dark:text-gray-300'}`}
                  >
                    Default
                  </button>
                  {groups.map((g) => (
                    <button
                      key={g.id}
                      onClick={() => { onUpdate(snippet.id, { groupId: g.id }); setShowMoveMenu(false) }}
                      className={`w-full text-left px-3 py-1 text-[11px] hover:bg-gray-50 dark:hover:bg-gray-700 transition ${snippet.groupId === g.id ? 'font-semibold text-indigo-600' : 'text-gray-700 dark:text-gray-300'}`}
                    >
                      {g.name}
                    </button>
                  ))}
                </div>
              )}
            </div>
          )}

          <button
            onClick={() => { setEditName(snippet.name); setEditDesc(snippet.description ?? ''); setEditing(true) }}
            className="flex items-center gap-1 text-[10px] text-gray-500 hover:text-gray-900 px-1.5 py-0.5 rounded hover:bg-gray-200 transition"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
            </svg>
            Edit
          </button>
          <button
            onClick={() => onRemove(snippet.id)}
            className="ml-auto text-[10px] text-gray-400 hover:text-red-500 px-1.5 py-0.5 rounded hover:bg-gray-200 transition"
          >
            Delete
          </button>
        </div>
      )}
    </div>
  )
}
