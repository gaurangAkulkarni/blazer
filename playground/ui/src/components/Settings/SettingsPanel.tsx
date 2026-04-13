import React, { useState, useEffect, useCallback, useRef } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { ProviderToggle } from './ProviderToggle'
import { ApiKeyInput } from './ApiKeyInput'
import { ModelSelector } from './ModelSelector'
import { TemperatureSlider } from './TemperatureSlider'
import { ExtensionsPanel } from './ExtensionsPanel'
import { BUILT_IN_SKILLS } from '../../lib/skills'
import type { AppSettings, CustomSkill } from '../../lib/types'
import type { Skill } from '../../lib/skills'

const OPENAI_MODELS = ['gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo', 'gpt-3.5-turbo']
const CLAUDE_MODELS = ['claude-sonnet-4-20250514', 'claude-opus-4-20250514', 'claude-haiku-4-5-20251001']

type Section = 'provider' | 'skills' | 'engine' | 'extensions'

interface Props {
  settings: AppSettings
  onUpdate: (partial: Partial<AppSettings>) => void
  onClose: () => void
}

// ── Nav item ──────────────────────────────────────────────────────────────────

function NavItem({
  icon, label, active, onClick,
}: { icon: React.ReactNode; label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center gap-2.5 text-left px-3 py-2 rounded-lg text-sm transition-colors ${
        active
          ? 'bg-gray-900 text-white font-medium'
          : 'text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-900 dark:hover:text-gray-100'
      }`}
    >
      <span className="shrink-0 opacity-70">{icon}</span>
      {label}
    </button>
  )
}

// ── Query Engine section ──────────────────────────────────────────────────────

function QueryEngineSection() {
  const [duckdbAvailable, setDuckdbAvailable] = useState<boolean | null>(null)
  const [installing, setInstalling] = useState(false)
  const [installMsg, setInstallMsg] = useState<{ ok: boolean; text: string } | null>(null)

  const checkDuckdb = useCallback(async () => {
    const available = await invoke<boolean>('check_duckdb').catch(() => false)
    setDuckdbAvailable(available)
  }, [])

  useEffect(() => { checkDuckdb() }, [checkDuckdb])

  const installDuckdb = useCallback(async () => {
    setInstalling(true)
    setInstallMsg(null)
    try {
      const msg = await invoke<string>('install_duckdb')
      setInstallMsg({ ok: true, text: msg })
      await checkDuckdb()
    } catch (err) {
      setInstallMsg({ ok: false, text: String(err) })
    } finally {
      setInstalling(false)
    }
  }, [checkDuckdb])

  return (
    <div className="space-y-6">
      {/* Blazer */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">Blazer Engine</span>
          <span className="flex items-center gap-1 text-[11px] text-green-600 font-medium bg-green-50 border border-green-200 px-1.5 py-0.5 rounded-full">
            <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>
            Built-in
          </span>
        </div>
        <p className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed">
          Native Rust DataFrame engine. Accepts a JSON query DSL. Best for structured analytical queries on Parquet and CSV files.
        </p>
      </div>

      <hr className="border-gray-100" />

      {/* DuckDB */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">DuckDB</span>
          {duckdbAvailable === null ? (
            <span className="text-[11px] text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded-full">Checking…</span>
          ) : duckdbAvailable ? (
            <span className="flex items-center gap-1 text-[11px] text-green-600 font-medium bg-green-50 border border-green-200 px-1.5 py-0.5 rounded-full">
              <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>
              Available
            </span>
          ) : (
            <span className="flex items-center gap-1 text-[11px] text-orange-600 font-medium bg-orange-50 border border-orange-200 px-1.5 py-0.5 rounded-full">
              <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>
              Not installed
            </span>
          )}
          <button
            onClick={checkDuckdb}
            className="ml-auto text-[11px] text-gray-400 hover:text-gray-700 px-1.5 py-0.5 rounded hover:bg-gray-100 transition"
            title="Recheck"
          >
            ↻ Refresh
          </button>
        </div>
        <p className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed mb-3">
          Full SQL engine via DuckDB CLI. Supports standard SQL on Parquet, CSV, and JSON files with{' '}
          <code className="text-[11px] bg-gray-100 px-1 rounded">read_parquet()</code>,{' '}
          <code className="text-[11px] bg-gray-100 px-1 rounded">read_csv()</code>, and more.
        </p>

        {!duckdbAvailable && (
          <div className="space-y-3">
            <button
              onClick={installDuckdb}
              disabled={installing}
              className="flex items-center gap-2 text-sm font-medium px-4 py-2 rounded-lg bg-gray-900 text-white hover:bg-gray-700 disabled:opacity-50 transition"
            >
              {installing ? (
                <>
                  <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" className="animate-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
                  Installing via Homebrew…
                </>
              ) : (
                <>
                  <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                  Install via Homebrew
                </>
              )}
            </button>

            <div className="text-xs text-gray-400">
              Or install manually:{' '}
              <code className="text-[11px] bg-gray-100 px-1.5 py-0.5 rounded font-mono">brew install duckdb</code>
            </div>
          </div>
        )}

        {installMsg && (
          <div className={`mt-3 text-xs px-3 py-2 rounded-lg whitespace-pre-wrap font-mono ${
            installMsg.ok
              ? 'bg-green-50 text-green-700 border border-green-200'
              : 'bg-red-50 text-red-600 border border-red-200'
          }`}>
            {installMsg.text}
          </div>
        )}
      </div>

      <hr className="border-gray-100" />

      <div className="text-xs text-gray-400 leading-relaxed">
        Switch between engines in the <span className="font-medium text-gray-600">Console</span> tab using the Blazer / DuckDB toggle.
      </div>
    </div>
  )
}

// ── Skills section ────────────────────────────────────────────────────────────

function SkillsSection({
  settings, onUpdate, onEditSkill,
}: {
  settings: AppSettings
  onUpdate: (partial: Partial<AppSettings>) => void
  onEditSkill: (skill: Skill) => void
}) {
  const activeSkills = settings.active_skills ?? ['blazer-engine']
  const customSkills: CustomSkill[] = settings.custom_skills ?? []
  const [addingSkill, setAddingSkill] = useState(false)
  const [newSkill, setNewSkill] = useState({ name: '', description: '', prompt: '' })

  const toggleSkill = (id: string) => {
    const next = activeSkills.includes(id)
      ? activeSkills.filter((s) => s !== id)
      : [...activeSkills, id]
    onUpdate({ active_skills: next })
  }

  const saveCustomSkill = () => {
    if (!newSkill.name.trim() || !newSkill.prompt.trim()) return
    const skill: CustomSkill = {
      id: `custom-${Date.now()}`,
      name: newSkill.name.trim(),
      description: newSkill.description.trim(),
      prompt: newSkill.prompt.trim(),
    }
    onUpdate({ custom_skills: [...customSkills, skill], active_skills: [...activeSkills, skill.id] })
    setNewSkill({ name: '', description: '', prompt: '' })
    setAddingSkill(false)
  }

  const deleteCustomSkill = (id: string) => {
    onUpdate({
      custom_skills: customSkills.filter((s) => s.id !== id),
      active_skills: activeSkills.filter((s) => s !== id),
    })
  }

  const allSkills: Skill[] = [
    ...BUILT_IN_SKILLS,
    ...customSkills.map((s) => ({ ...s, builtIn: false as const })),
  ]

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-xs text-gray-500">Skills inject knowledge into the AI's system prompt.</p>
        <button
          onClick={() => setAddingSkill(true)}
          className="text-xs text-gray-500 hover:text-gray-900 px-2 py-0.5 rounded hover:bg-gray-100 transition shrink-0"
        >
          + Add skill
        </button>
      </div>

      <div className="space-y-2">
        {allSkills.map((skill) => (
          <div
            key={skill.id}
            className="flex items-start gap-2.5 p-2.5 rounded-lg border border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 group"
          >
            <input
              type="checkbox"
              checked={activeSkills.includes(skill.id)}
              onChange={() => toggleSkill(skill.id)}
              className="mt-0.5 accent-gray-900 shrink-0"
              onClick={(e) => e.stopPropagation()}
            />
            <div className="flex-1 min-w-0 cursor-pointer" onClick={() => onEditSkill(skill)}>
              <div className="flex items-center gap-1.5">
                <span className="text-xs font-medium text-gray-800 dark:text-gray-200">{skill.name}</span>
                {!skill.builtIn && (
                  <span className="text-[10px] text-gray-400 bg-gray-200 px-1 rounded">custom</span>
                )}
                <span className="text-[10px] text-gray-300 group-hover:text-gray-400 ml-auto">
                  {skill.builtIn ? 'view ›' : 'edit ›'}
                </span>
              </div>
              {skill.description && (
                <p className="text-[11px] text-gray-400 mt-0.5 leading-relaxed">{skill.description}</p>
              )}
            </div>
            {!skill.builtIn && (
              <button
                onClick={(e) => { e.stopPropagation(); deleteCustomSkill(skill.id) }}
                className="text-gray-300 hover:text-red-400 transition shrink-0 p-0.5 rounded"
                title="Delete skill"
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
              </button>
            )}
          </div>
        ))}
      </div>

      {addingSkill && (
        <div className="p-3 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 space-y-2">
          <input type="text" placeholder="Skill name" value={newSkill.name} onChange={(e) => setNewSkill((p) => ({ ...p, name: e.target.value }))} className="w-full text-xs border border-gray-200 dark:border-gray-600 rounded-md px-2.5 py-1.5 focus:outline-none focus:ring-1 focus:ring-gray-400 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" />
          <input type="text" placeholder="Short description (optional)" value={newSkill.description} onChange={(e) => setNewSkill((p) => ({ ...p, description: e.target.value }))} className="w-full text-xs border border-gray-200 dark:border-gray-600 rounded-md px-2.5 py-1.5 focus:outline-none focus:ring-1 focus:ring-gray-400 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" />
          <textarea placeholder="Skill prompt…" value={newSkill.prompt} onChange={(e) => setNewSkill((p) => ({ ...p, prompt: e.target.value }))} rows={5} className="w-full text-xs border border-gray-200 dark:border-gray-600 rounded-md px-2.5 py-1.5 font-mono focus:outline-none focus:ring-1 focus:ring-gray-400 resize-none bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" />
          <div className="flex gap-2">
            <button onClick={saveCustomSkill} disabled={!newSkill.name.trim() || !newSkill.prompt.trim()} className="flex-1 bg-gray-900 text-white text-xs rounded-md py-1.5 font-medium hover:bg-gray-700 disabled:opacity-30 transition">Save skill</button>
            <button onClick={() => { setAddingSkill(false); setNewSkill({ name: '', description: '', prompt: '' }) }} className="text-xs text-gray-400 hover:text-gray-600 px-3 py-1.5 rounded-md hover:bg-gray-100 transition">Cancel</button>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Ollama section with live model discovery ──────────────────────────────────

// ── OpenAI / OpenAI-compatible section ───────────────────────────────────────

function OpenAISection({
  settings, onUpdate,
}: {
  settings: AppSettings
  onUpdate: (partial: Partial<AppSettings>) => void
}) {
  const baseUrl = (settings.openai?.base_url ?? '').trim()
  const hasCustomUrl = baseUrl !== ''

  const [fetchedModels, setFetchedModels] = useState<string[]>([])
  const [fetchState, setFetchState] = useState<'idle' | 'loading' | 'ok' | 'error'>('idle')
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const fetchModels = useCallback(async (url: string) => {
    const base = url.replace(/\/$/, '')
    if (!base) return
    setFetchState('loading')
    try {
      // Route through Rust/reqwest to avoid WebView CORS/ATS restrictions
      const names = await invoke<string[]>('fetch_openai_models', {
        baseUrl: base,
        apiKey: settings.openai?.api_key || null,
      })
      setFetchedModels(names)
      setFetchState('ok')
      // Auto-select first if current model isn't in the list
      if (names.length > 0 && !names.includes(settings.openai?.model ?? '')) {
        onUpdate({ openai: { ...settings.openai, model: names[0] } })
      }
    } catch {
      setFetchedModels([])
      setFetchState('error')
    }
  }, [settings.openai, onUpdate])

  // Debounce-fetch when custom base URL changes
  useEffect(() => {
    if (!hasCustomUrl) { setFetchState('idle'); setFetchedModels([]); return }
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => fetchModels(baseUrl), 500)
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [baseUrl]) // eslint-disable-line react-hooks/exhaustive-deps

  // Models to show: fetched list if available, fallback to built-in list
  const modelList = fetchedModels.length > 0 ? fetchedModels : OPENAI_MODELS

  return (
    <div className="space-y-3">
      <ApiKeyInput
        value={settings.openai.api_key}
        onChange={(api_key) => onUpdate({ openai: { ...settings.openai, api_key } })}
        placeholder="sk-..."
      />

      {/* Model selector */}
      <div>
        <div className="flex items-center justify-between mb-1">
          <label className="text-[11px] font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Model</label>
          {hasCustomUrl && (
            <div className="flex items-center gap-1.5">
              {fetchState === 'loading' && (
                <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" className="animate-spin text-gray-400"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
              )}
              {fetchState === 'ok' && (
                <span className="flex items-center gap-1 text-[10px] text-green-600 font-medium">
                  <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>
                  {fetchedModels.length} model{fetchedModels.length !== 1 ? 's' : ''} found
                </span>
              )}
              {fetchState === 'error' && (
                <span className="flex items-center gap-1 text-[10px] text-red-500 font-medium">
                  <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>
                  Unreachable
                </span>
              )}
              <button
                onClick={() => fetchModels(baseUrl)}
                className="text-[11px] text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 px-1.5 py-0.5 rounded hover:bg-gray-100 dark:hover:bg-gray-700 transition"
                title="Refresh models from server"
              >
                ↻ Refresh
              </button>
            </div>
          )}
        </div>
        <select
          value={settings.openai?.model ?? ''}
          onChange={(e) => onUpdate({ openai: { ...settings.openai, model: e.target.value } })}
          className="w-full bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 rounded-lg px-3 py-2 text-sm border border-gray-200 dark:border-gray-600 focus:outline-none focus:ring-2 focus:ring-gray-900/10 dark:focus:ring-gray-100/10 transition"
        >
          {modelList.map((m) => <option key={m} value={m}>{m}</option>)}
          {/* Keep current value selectable even if not in fetched list */}
          {settings.openai?.model && !modelList.includes(settings.openai.model) && (
            <option value={settings.openai.model}>{settings.openai.model}</option>
          )}
        </select>
        {fetchState === 'error' && (
          <p className="text-[10px] text-red-400 mt-1">
            Could not reach <span className="font-mono">{baseUrl}/models</span> — check the URL and that the server is running.
          </p>
        )}
      </div>

      <TemperatureSlider
        value={settings.openai.temperature}
        onChange={(temperature) => onUpdate({ openai: { ...settings.openai, temperature } })}
      />

      {/* Base URL */}
      <div>
        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1.5">
          Base URL
          <span className="ml-1.5 font-normal text-gray-400 dark:text-gray-500">(optional — leave blank for api.openai.com)</span>
        </label>
        <input
          type="text"
          value={settings.openai.base_url ?? ''}
          onChange={(e) => onUpdate({ openai: { ...settings.openai, base_url: e.target.value } })}
          className="w-full text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 dark:focus:ring-gray-100/10 font-mono bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500"
          placeholder="https://api.openai.com/v1"
          spellCheck={false}
        />
        <p className="mt-1.5 text-[11px] text-gray-400 dark:text-gray-500 leading-relaxed">
          Compatible with LM Studio, Ollama, vLLM, Azure OpenAI, and any OpenAI-format API.
        </p>
      </div>
    </div>
  )
}

// ── Ollama section ─────────────────────────────────────────────────────────────

function OllamaSection({
  settings, onUpdate,
}: {
  settings: AppSettings
  onUpdate: (partial: Partial<AppSettings>) => void
}) {
  const baseUrl = settings.ollama?.base_url ?? 'http://localhost:11434'
  const [models, setModels] = useState<string[]>([])
  const [fetchState, setFetchState] = useState<'idle' | 'loading' | 'ok' | 'error'>('idle')
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const fetchModels = useCallback(async (url: string) => {
    setFetchState('loading')
    try {
      const res = await fetch(`${url.replace(/\/$/, '')}/api/tags`, { signal: AbortSignal.timeout(4000) })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json() as { models: { name: string }[] }
      const names = (data.models ?? []).map((m) => m.name).sort()
      setModels(names)
      setFetchState('ok')
      // Auto-select first model if current selection isn't in the list
      if (names.length > 0 && !names.includes(settings.ollama?.model ?? '')) {
        onUpdate({ ollama: { ...settings.ollama, model: names[0] } })
      }
    } catch {
      setModels([])
      setFetchState('error')
    }
  }, [settings.ollama, onUpdate])

  // Fetch on mount and when baseUrl changes (debounced)
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => fetchModels(baseUrl), 400)
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [baseUrl]) // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Ollama</h3>
        <span className="text-[10px] bg-green-50 text-green-600 border border-green-200 px-1.5 py-0.5 rounded-full font-medium">Local · Free</span>
      </div>
      <p className="text-[11px] text-gray-400 mb-3 leading-relaxed">
        Runs models locally via <span className="underline">ollama.com</span>. No API key needed.
        Start Ollama, then select from your installed models.
      </p>
      <div className="space-y-3">
        {/* Base URL */}
        <div>
          <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">Base URL</label>
          <input
            type="text"
            value={baseUrl}
            onChange={(e) => onUpdate({ ollama: { ...settings.ollama, base_url: e.target.value } })}
            className="w-full text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 dark:focus:ring-gray-100/10 font-mono bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
            placeholder="http://localhost:11434"
            spellCheck={false}
          />
        </div>

        {/* Model — dropdown if connected, text fallback if error */}
        <div>
          <div className="flex items-center justify-between mb-1">
            <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide">Model</label>
            <div className="flex items-center gap-1.5">
              {fetchState === 'loading' && (
                <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" className="animate-spin text-gray-400"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
              )}
              {fetchState === 'ok' && (
                <span className="flex items-center gap-1 text-[10px] text-green-600 font-medium">
                  <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>
                  Connected · {models.length} model{models.length !== 1 ? 's' : ''}
                </span>
              )}
              {fetchState === 'error' && (
                <span className="flex items-center gap-1 text-[10px] text-red-500 font-medium">
                  <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>
                  Ollama not running
                </span>
              )}
              <button
                onClick={() => fetchModels(baseUrl)}
                className="text-[11px] text-gray-400 hover:text-gray-700 px-1.5 py-0.5 rounded hover:bg-gray-100 transition"
                title="Refresh models"
              >
                ↻ Refresh
              </button>
            </div>
          </div>

          {fetchState !== 'error' && models.length > 0 ? (
            <select
              value={settings.ollama?.model ?? ''}
              onChange={(e) => onUpdate({ ollama: { ...settings.ollama, model: e.target.value } })}
              className="w-full bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 rounded-lg px-3 py-2 text-sm border border-gray-200 dark:border-gray-600 focus:outline-none focus:ring-2 focus:ring-gray-900/10 dark:focus:ring-gray-100/10 transition"
            >
              {models.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
          ) : (
            <div>
              <input
                type="text"
                value={settings.ollama?.model ?? ''}
                onChange={(e) => onUpdate({ ollama: { ...settings.ollama, model: e.target.value } })}
                className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10"
                placeholder="llama3.2"
                spellCheck={false}
              />
              {fetchState === 'error' && (
                <p className="text-[10px] text-gray-400 mt-1">
                  Ollama isn't reachable. Type a model name manually or start Ollama and click ↻ Refresh.
                </p>
              )}
              {fetchState === 'idle' || fetchState === 'loading' ? (
                <p className="text-[10px] text-gray-400 mt-1">Pull a model first: <code className="bg-gray-100 px-1 rounded">ollama pull llama3.2</code></p>
              ) : null}
            </div>
          )}
        </div>

        <TemperatureSlider
          value={settings.ollama?.temperature ?? 0.3}
          onChange={(temperature) => onUpdate({ ollama: { ...settings.ollama, temperature } })}
        />
      </div>
    </div>
  )
}

// ── Main Settings Panel ───────────────────────────────────────────────────────

export function SettingsPanel({ settings, onUpdate, onClose }: Props) {
  const [section, setSection] = useState<Section>('provider')
  const [editingSkill, setEditingSkill] = useState<Skill | null>(null)
  const [editDraft, setEditDraft] = useState({ name: '', description: '', prompt: '' })

  const openEdit = (skill: Skill) => {
    setEditingSkill(skill)
    setEditDraft({ name: skill.name, description: skill.description, prompt: skill.prompt })
  }

  const saveEdit = () => {
    if (!editingSkill || editingSkill.builtIn) { setEditingSkill(null); return }
    const customSkills = settings.custom_skills ?? []
    onUpdate({ custom_skills: customSkills.map((s) => s.id === editingSkill.id ? { ...s, ...editDraft } : s) })
    setEditingSkill(null)
  }

  const SECTIONS: { id: Section; label: string; icon: React.ReactNode }[] = [
    {
      id: 'provider',
      label: 'AI Provider',
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="8" r="4"/><path d="M4 20c0-4 3.6-7 8-7s8 3 8 7"/></svg>
      ),
    },
    {
      id: 'skills',
      label: 'AI Skills',
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>
      ),
    },
    {
      id: 'engine',
      label: 'Query Engine',
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
      ),
    },
    {
      id: 'extensions',
      label: 'Extensions',
      icon: (
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M20.24 12.24a6 6 0 0 0-8.49-8.49L5 10.5V19h8.5z"/><line x1="16" y1="8" x2="2" y2="22"/><line x1="17.5" y1="15" x2="9" y2="15"/></svg>
      ),
    },
  ]

  return (
    <>
      {/* Skill view/edit sub-modal */}
      {editingSkill && (
        <div className="fixed inset-0 z-[70] flex items-center justify-center">
          <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" onClick={() => setEditingSkill(null)} />
          <div className="relative bg-white dark:bg-gray-900 rounded-xl shadow-2xl w-[520px] max-h-[80vh] flex flex-col mx-4">
            <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100 dark:border-gray-800">
              <div className="flex items-center gap-2">
                <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">{editingSkill.builtIn ? 'View Skill' : 'Edit Skill'}</span>
                {editingSkill.builtIn && <span className="text-[10px] bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400 px-1.5 py-0.5 rounded font-medium">built-in · read only</span>}
              </div>
              <button onClick={() => setEditingSkill(null)} className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition">
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
              </button>
            </div>
            <div className="p-5 space-y-3 overflow-y-auto flex-1">
              {(['name', 'description'] as const).map((field) => (
                <div key={field}>
                  <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">{field}</label>
                  <input type="text" value={editDraft[field]} onChange={(e) => setEditDraft((p) => ({ ...p, [field]: e.target.value }))} readOnly={editingSkill.builtIn} className="w-full text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 read-only:bg-gray-50 read-only:text-gray-500 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100" />
                </div>
              ))}
              <div>
                <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">Prompt</label>
                <textarea value={editDraft.prompt} onChange={(e) => setEditDraft((p) => ({ ...p, prompt: e.target.value }))} readOnly={editingSkill.builtIn} rows={12} className="w-full text-xs font-mono border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 resize-none read-only:bg-gray-50 read-only:text-gray-500 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100" />
              </div>
            </div>
            {!editingSkill.builtIn && (
              <div className="flex gap-2 px-5 py-4 border-t border-gray-100 dark:border-gray-800">
                <button onClick={saveEdit} disabled={!editDraft.name.trim() || !editDraft.prompt.trim()} className="flex-1 bg-gray-900 text-white text-sm rounded-lg py-2 font-medium hover:bg-gray-700 disabled:opacity-30 transition">Save changes</button>
                <button onClick={() => setEditingSkill(null)} className="text-sm text-gray-400 hover:text-gray-600 px-4 py-2 rounded-lg hover:bg-gray-100 transition">Cancel</button>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Main settings modal — centered popup */}
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div className="absolute inset-0 bg-black/30 backdrop-blur-sm" onClick={onClose} />

        <div className="relative bg-white dark:bg-gray-900 rounded-2xl shadow-2xl w-full max-w-[740px] h-[620px] flex overflow-hidden">
          {/* Left nav sidebar */}
          <div className="w-44 shrink-0 bg-gray-50 dark:bg-gray-800 border-r border-gray-100 dark:border-gray-700 flex flex-col p-3 gap-1">
            <p className="text-[10px] font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wider px-3 pt-1 pb-2">
              Settings
            </p>
            {SECTIONS.map((s) => (
              <NavItem
                key={s.id}
                icon={s.icon}
                label={s.label}
                active={section === s.id}
                onClick={() => setSection(s.id)}
              />
            ))}
          </div>

          {/* Content area */}
          <div className="flex-1 flex flex-col min-h-0 min-w-0">
            <div className="shrink-0 flex items-center justify-between px-6 py-4 border-b border-gray-100 dark:border-gray-800">
              <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                {SECTIONS.find((s) => s.id === section)?.label}
              </h2>
              <button onClick={onClose} className="text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 p-1 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition">
                <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
              </button>
            </div>

            <div className="flex-1 overflow-y-auto overflow-x-hidden px-6 py-5">
              {section === 'provider' && (
                <div className="space-y-5">
                  {/* Provider selector */}
                  <ProviderToggle active={settings.active_provider} onChange={(p) => onUpdate({ active_provider: p })} />

                  {/* Context history limit */}
                  <div className="flex items-center justify-between py-1">
                    <div className="min-w-0">
                      <div className="text-xs font-medium text-gray-800 dark:text-gray-200">Context history</div>
                      <div className="text-[11px] text-gray-400 dark:text-gray-500 mt-0.5 leading-relaxed">
                        How many previous messages to send to the LLM. Fewer = faster &amp; cheaper.
                      </div>
                    </div>
                    <div className="ml-4 shrink-0 flex items-center gap-1 bg-gray-100 dark:bg-gray-700 rounded-lg p-0.5">
                      {([5, 10, 20, 50, 0] as const).map((n) => {
                        const label = n === 0 ? 'All' : String(n)
                        const cur = settings.context_history_limit ?? 20
                        const active = cur === n
                        return (
                          <button
                            key={n}
                            onClick={() => onUpdate({ context_history_limit: n })}
                            className={`text-[11px] font-medium px-2 py-0.5 rounded-md transition-all ${
                              active ? 'bg-white dark:bg-gray-600 text-gray-900 dark:text-gray-100 shadow-sm' : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'
                            }`}
                          >
                            {label}
                          </button>
                        )
                      })}
                    </div>
                  </div>

                  {/* Max output tokens */}
                  <div className="flex items-center justify-between py-1">
                    <div className="min-w-0">
                      <div className="text-xs font-medium text-gray-800 dark:text-gray-200">Max output tokens</div>
                      <div className="text-[11px] text-gray-400 dark:text-gray-500 mt-0.5 leading-relaxed">
                        Maximum tokens the LLM can generate per response.
                      </div>
                    </div>
                    <div className="ml-4 shrink-0 flex items-center gap-1 bg-gray-100 dark:bg-gray-700 rounded-lg p-0.5">
                      {([1024, 2048, 4096, 8192, 16000] as const).map((n) => {
                        const labels: Record<number, string> = { 1024: '1k', 2048: '2k', 4096: '4k', 8192: '8k', 16000: '16k' }
                        const label = labels[n] ?? String(n)
                        const cur = settings.max_output_tokens ?? 4096
                        const active = cur === n
                        return (
                          <button
                            key={n}
                            onClick={() => onUpdate({ max_output_tokens: n })}
                            className={`text-[11px] font-medium px-2 py-0.5 rounded-md transition-all ${
                              active ? 'bg-white dark:bg-gray-600 text-gray-900 dark:text-gray-100 shadow-sm' : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'
                            }`}
                          >
                            {label}
                          </button>
                        )
                      })}
                    </div>
                  </div>

                  {/* Follow-up chips toggle */}
                  <div className="flex items-center justify-between py-1">
                    <div className="min-w-0">
                      <div className="text-xs font-medium text-gray-800 dark:text-gray-200">Follow-up suggestion chips</div>
                      <div className="text-[11px] text-gray-400 dark:text-gray-500 mt-0.5 leading-relaxed">
                        Show clickable follow-up prompts after each AI response. Disable to save tokens.
                      </div>
                    </div>
                    <button
                      role="switch"
                      aria-checked={settings.show_follow_up_chips !== false}
                      onClick={() => onUpdate({ show_follow_up_chips: settings.show_follow_up_chips === false })}
                      className={`relative ml-4 shrink-0 inline-flex h-5 w-9 items-center rounded-full transition-colors focus:outline-none
                        ${settings.show_follow_up_chips !== false ? 'bg-gray-900' : 'bg-gray-200'}`}
                    >
                      <span
                        className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform
                          ${settings.show_follow_up_chips !== false ? 'translate-x-[18px]' : 'translate-x-0.5'}`}
                      />
                    </button>
                  </div>

                  <hr className="border-gray-100" />

                  {/* Per-provider settings — only the active provider is shown */}
                  {settings.active_provider === 'openai' && (
                    <OpenAISection settings={settings} onUpdate={onUpdate} />
                  )}

                  {settings.active_provider === 'claude' && (
                    <div className="space-y-3">
                      <ApiKeyInput value={settings.claude.api_key} onChange={(api_key) => onUpdate({ claude: { ...settings.claude, api_key } })} placeholder="sk-ant-..." />
                      <ModelSelector value={settings.claude.model} models={CLAUDE_MODELS} onChange={(model) => onUpdate({ claude: { ...settings.claude, model } })} />
                      <TemperatureSlider value={settings.claude.temperature} onChange={(temperature) => onUpdate({ claude: { ...settings.claude, temperature } })} />
                    </div>
                  )}

                  {settings.active_provider === 'ollama' && (
                    <OllamaSection settings={settings} onUpdate={onUpdate} />
                  )}
                </div>
              )}

              {section === 'skills' && (
                <SkillsSection settings={settings} onUpdate={onUpdate} onEditSkill={openEdit} />
              )}

              {section === 'engine' && <QueryEngineSection />}

              {section === 'extensions' && <ExtensionsPanel settings={settings} onUpdate={onUpdate} />}
            </div>
          </div>
        </div>
      </div>
    </>
  )
}
