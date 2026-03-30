import React, { useState } from 'react'
import { ProviderToggle } from './ProviderToggle'
import { ApiKeyInput } from './ApiKeyInput'
import { ModelSelector } from './ModelSelector'
import { TemperatureSlider } from './TemperatureSlider'
import { BUILT_IN_SKILLS } from '../../lib/skills'
import type { AppSettings, CustomSkill } from '../../lib/types'
import type { Skill } from '../../lib/skills'

const OPENAI_MODELS = ['gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo', 'gpt-3.5-turbo']
const CLAUDE_MODELS = ['claude-sonnet-4-20250514', 'claude-opus-4-20250514', 'claude-haiku-4-5-20251001']

interface Props {
  settings: AppSettings
  onUpdate: (partial: Partial<AppSettings>) => void
  onClose: () => void
}

export function SettingsPanel({ settings, onUpdate, onClose }: Props) {
  const activeSkills = settings.active_skills ?? ['blazer-engine']
  const customSkills: CustomSkill[] = settings.custom_skills ?? []
  const [newSkill, setNewSkill] = useState({ name: '', description: '', prompt: '' })
  const [addingSkill, setAddingSkill] = useState(false)
  const [editingSkill, setEditingSkill] = useState<Skill | null>(null)
  const [editDraft, setEditDraft] = useState({ name: '', description: '', prompt: '' })

  const toggleSkill = (id: string) => {
    const next = activeSkills.includes(id) ? activeSkills.filter((s) => s !== id) : [...activeSkills, id]
    onUpdate({ active_skills: next })
  }

  const saveCustomSkill = () => {
    if (!newSkill.name.trim() || !newSkill.prompt.trim()) return
    const skill: CustomSkill = { id: `custom-${Date.now()}`, name: newSkill.name.trim(), description: newSkill.description.trim(), prompt: newSkill.prompt.trim() }
    onUpdate({ custom_skills: [...customSkills, skill], active_skills: [...activeSkills, skill.id] })
    setNewSkill({ name: '', description: '', prompt: '' })
    setAddingSkill(false)
  }

  const deleteCustomSkill = (id: string) => {
    onUpdate({ custom_skills: customSkills.filter((s) => s.id !== id), active_skills: activeSkills.filter((s) => s !== id) })
  }

  const openEdit = (skill: Skill) => {
    setEditingSkill(skill)
    setEditDraft({ name: skill.name, description: skill.description, prompt: skill.prompt })
  }

  const saveEdit = () => {
    if (!editingSkill || editingSkill.builtIn) { setEditingSkill(null); return }
    onUpdate({ custom_skills: customSkills.map((s) => s.id === editingSkill.id ? { ...s, ...editDraft } : s) })
    setEditingSkill(null)
  }

  const allSkills: Skill[] = [...BUILT_IN_SKILLS, ...customSkills.map((s) => ({ ...s, builtIn: false as const }))]

  return (
    <>
      {editingSkill && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center">
          <div className="absolute inset-0 bg-black/30 backdrop-blur-sm" onClick={() => setEditingSkill(null)} />
          <div className="relative bg-white rounded-xl shadow-2xl w-[520px] max-h-[80vh] flex flex-col mx-4">
            <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100">
              <div className="flex items-center gap-2">
                <span className="text-sm font-semibold text-gray-900">{editingSkill.builtIn ? 'View Skill' : 'Edit Skill'}</span>
                {editingSkill.builtIn && <span className="text-[10px] bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded font-medium">built-in · read only</span>}
              </div>
              <button onClick={() => setEditingSkill(null)} className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition">
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
              </button>
            </div>
            <div className="p-5 space-y-3 overflow-y-auto flex-1">
              {(['name', 'description'] as const).map((field) => (
                <div key={field}>
                  <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">{field}</label>
                  <input type="text" value={editDraft[field]} onChange={(e) => setEditDraft((p) => ({ ...p, [field]: e.target.value }))} readOnly={editingSkill.builtIn} className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 read-only:bg-gray-50 read-only:text-gray-500" />
                </div>
              ))}
              <div>
                <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">Prompt</label>
                <textarea value={editDraft.prompt} onChange={(e) => setEditDraft((p) => ({ ...p, prompt: e.target.value }))} readOnly={editingSkill.builtIn} rows={12} className="w-full text-xs font-mono border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 resize-none read-only:bg-gray-50 read-only:text-gray-500" />
              </div>
            </div>
            {!editingSkill.builtIn && (
              <div className="flex gap-2 px-5 py-4 border-t border-gray-100">
                <button onClick={saveEdit} disabled={!editDraft.name.trim() || !editDraft.prompt.trim()} className="flex-1 bg-gray-900 text-white text-sm rounded-lg py-2 font-medium hover:bg-gray-700 disabled:opacity-30 transition">Save changes</button>
                <button onClick={() => setEditingSkill(null)} className="text-sm text-gray-400 hover:text-gray-600 px-4 py-2 rounded-lg hover:bg-gray-100 transition">Cancel</button>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="fixed inset-0 z-50 flex justify-end">
        <div className="absolute inset-0 bg-black/20 backdrop-blur-sm" onClick={onClose} />
        <div className="relative w-96 bg-white border-l border-gray-200 overflow-y-auto shadow-2xl">
          <div className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-base font-semibold text-gray-900">Settings</h2>
              <button onClick={onClose} className="text-gray-400 hover:text-gray-600 p-1 rounded-md hover:bg-gray-100 transition">
                <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
              </button>
            </div>

            <ProviderToggle active={settings.active_provider} onChange={(p) => onUpdate({ active_provider: p })} />

            <div className="mt-6">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">OpenAI</h3>
              <div className="space-y-3">
                <ApiKeyInput value={settings.openai.api_key} onChange={(api_key) => onUpdate({ openai: { ...settings.openai, api_key } })} placeholder="sk-..." />
                <ModelSelector value={settings.openai.model} models={OPENAI_MODELS} onChange={(model) => onUpdate({ openai: { ...settings.openai, model } })} />
                <TemperatureSlider value={settings.openai.temperature} onChange={(temperature) => onUpdate({ openai: { ...settings.openai, temperature } })} />
              </div>
            </div>

            <hr className="my-6 border-gray-100" />

            <div>
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Claude</h3>
              <div className="space-y-3">
                <ApiKeyInput value={settings.claude.api_key} onChange={(api_key) => onUpdate({ claude: { ...settings.claude, api_key } })} placeholder="sk-ant-..." />
                <ModelSelector value={settings.claude.model} models={CLAUDE_MODELS} onChange={(model) => onUpdate({ claude: { ...settings.claude, model } })} />
                <TemperatureSlider value={settings.claude.temperature} onChange={(temperature) => onUpdate({ claude: { ...settings.claude, temperature } })} />
              </div>
            </div>

            <hr className="my-6 border-gray-100" />

            <div>
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">AI Skills</h3>
                <button onClick={() => setAddingSkill(true)} className="text-xs text-gray-500 hover:text-gray-900 px-2 py-0.5 rounded hover:bg-gray-100 transition">+ Add skill</button>
              </div>
              <p className="text-xs text-gray-400 mb-3">Skills inject knowledge into the AI's system prompt.</p>
              <div className="space-y-2">
                {allSkills.map((skill) => (
                  <div key={skill.id} className="flex items-start gap-2.5 p-2.5 rounded-lg border border-gray-100 bg-gray-50 group">
                    <input type="checkbox" checked={activeSkills.includes(skill.id)} onChange={() => toggleSkill(skill.id)} className="mt-0.5 accent-gray-900 shrink-0" onClick={(e) => e.stopPropagation()} />
                    <div className="flex-1 min-w-0 cursor-pointer" onClick={() => openEdit(skill)}>
                      <div className="flex items-center gap-1.5">
                        <span className="text-xs font-medium text-gray-800">{skill.name}</span>
                        {!skill.builtIn && <span className="text-[10px] text-gray-400 bg-gray-200 px-1 rounded">custom</span>}
                        <span className="text-[10px] text-gray-300 group-hover:text-gray-400 ml-auto">{skill.builtIn ? 'view ›' : 'edit ›'}</span>
                      </div>
                      {skill.description && <p className="text-[11px] text-gray-400 mt-0.5 leading-relaxed">{skill.description}</p>}
                    </div>
                    {!skill.builtIn && (
                      <button onClick={(e) => { e.stopPropagation(); deleteCustomSkill(skill.id) }} className="text-gray-300 hover:text-red-400 transition shrink-0 p-0.5 rounded" title="Delete skill">
                        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
                      </button>
                    )}
                  </div>
                ))}
              </div>

              {addingSkill && (
                <div className="mt-3 p-3 rounded-lg border border-gray-200 bg-white space-y-2">
                  <input type="text" placeholder="Skill name" value={newSkill.name} onChange={(e) => setNewSkill((p) => ({ ...p, name: e.target.value }))} className="w-full text-xs border border-gray-200 rounded-md px-2.5 py-1.5 focus:outline-none focus:ring-1 focus:ring-gray-400" />
                  <input type="text" placeholder="Short description (optional)" value={newSkill.description} onChange={(e) => setNewSkill((p) => ({ ...p, description: e.target.value }))} className="w-full text-xs border border-gray-200 rounded-md px-2.5 py-1.5 focus:outline-none focus:ring-1 focus:ring-gray-400" />
                  <textarea placeholder="Skill prompt…" value={newSkill.prompt} onChange={(e) => setNewSkill((p) => ({ ...p, prompt: e.target.value }))} rows={5} className="w-full text-xs border border-gray-200 rounded-md px-2.5 py-1.5 font-mono focus:outline-none focus:ring-1 focus:ring-gray-400 resize-none" />
                  <div className="flex gap-2">
                    <button onClick={saveCustomSkill} disabled={!newSkill.name.trim() || !newSkill.prompt.trim()} className="flex-1 bg-gray-900 text-white text-xs rounded-md py-1.5 font-medium hover:bg-gray-700 disabled:opacity-30 transition">Save skill</button>
                    <button onClick={() => { setAddingSkill(false); setNewSkill({ name: '', description: '', prompt: '' }) }} className="text-xs text-gray-400 hover:text-gray-600 px-3 py-1.5 rounded-md hover:bg-gray-100 transition">Cancel</button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </>
  )
}
