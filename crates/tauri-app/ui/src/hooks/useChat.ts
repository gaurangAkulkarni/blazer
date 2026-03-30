import { useState, useCallback, useRef } from 'react'
import type { ChatMessage, AppSettings, AttachedFile, QueryResult } from '../lib/types'
import { resolveSkillPrompts } from '../lib/skills'

let msgCounter = 0
const nextId = () => `msg-${++msgCounter}-${Date.now()}`

export function useChat(settings: AppSettings) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [loadedFiles, setLoadedFiles] = useState<AttachedFile[]>([])
  const streamingRef = useRef('')

  const addFiles = useCallback((files: AttachedFile[]) => {
    setLoadedFiles((prev) => {
      const existing = new Set(prev.map((f) => f.path))
      return [...prev, ...files.filter((f) => !existing.has(f.path))]
    })
  }, [])

  const removeFile = useCallback((path: string) => {
    setLoadedFiles((prev) => prev.filter((f) => f.path !== path))
  }, [])

  const replaceFile = useCallback((oldPath: string, newFile: AttachedFile) => {
    setLoadedFiles((prev) => prev.map((f) => (f.path === oldPath ? newFile : f)))
  }, [])

  const addQueryResult = useCallback((messageId: string, result: QueryResult) => {
    setMessages((prev) =>
      prev.map((m) =>
        m.id === messageId
          ? { ...m, queryResults: [...(m.queryResults || []), result] }
          : m,
      ),
    )
  }, [])

  const sendMessage = useCallback(
    async (content: string, newAttachments?: AttachedFile[]) => {
      let allFiles = loadedFiles
      if (newAttachments && newAttachments.length > 0) {
        addFiles(newAttachments)
        const existing = new Set(loadedFiles.map((f) => f.path))
        allFiles = [...loadedFiles, ...newAttachments.filter((f) => !existing.has(f.path))]
      }

      const userMsg: ChatMessage = {
        id: nextId(), role: 'user', content, timestamp: Date.now(),
        attachedFiles: newAttachments,
      }
      const assistantMsg: ChatMessage = {
        id: nextId(), role: 'assistant', content: '', timestamp: Date.now(),
      }

      setMessages((prev) => [...prev, userMsg, assistantMsg])
      setIsStreaming(true)
      streamingRef.current = ''

      const provider = settings.active_provider
      const providerCfg = settings[provider]

      // Build file context for the LLM
      let fileContext = ''
      if (allFiles.length > 0) {
        fileContext = '## Attached Data Files\nUse EXACTLY these absolute paths in your query JSON:\n'
        for (const f of allFiles) {
          const kind =
            f.ext === 'csv' || f.ext === 'tsv' ? 'csv' :
            f.ext === 'parquet_dir' ? 'parquet_dir' : 'parquet'
          fileContext += `- Path: \`${f.path}\` → source type: \`"${kind}"\`\n`
          if (f.columns && f.columns.length > 0) {
            fileContext += `  Columns: ${f.columns.map((c) => `\`${c}\``).join(', ')}\n`
          }
        }
      }

      const skillPrompt = resolveSkillPrompts(
        settings.active_skills ?? ['blazer-engine'],
        (settings.custom_skills ?? []).map((s) => ({ ...s, builtIn: false as const })),
      )

      // Build messages for the API
      const apiMessages: { role: string; content: string }[] = []
      if (skillPrompt) apiMessages.push({ role: 'system', content: skillPrompt })
      if (fileContext) apiMessages.push({ role: 'system', content: fileContext })

      // Include previous messages
      for (const m of messages) {
        apiMessages.push({ role: m.role, content: m.content })
      }
      apiMessages.push({ role: 'user', content: fileContext ? `${content}\n\n${fileContext}` : content })

      try {
        if (provider === 'openai') {
          await streamOpenAI(apiMessages, providerCfg.api_key, providerCfg.model, providerCfg.temperature, (chunk) => {
            streamingRef.current += chunk
            setMessages((prev) => {
              const updated = [...prev]
              const last = updated[updated.length - 1]
              if (last?.role === 'assistant') {
                updated[updated.length - 1] = { ...last, content: streamingRef.current }
              }
              return updated
            })
          })
        } else {
          await streamClaude(apiMessages, providerCfg.api_key, providerCfg.model, providerCfg.temperature, (chunk) => {
            streamingRef.current += chunk
            setMessages((prev) => {
              const updated = [...prev]
              const last = updated[updated.length - 1]
              if (last?.role === 'assistant') {
                updated[updated.length - 1] = { ...last, content: streamingRef.current }
              }
              return updated
            })
          })
        }
      } catch (err: any) {
        setMessages((prev) => {
          const updated = [...prev]
          const last = updated[updated.length - 1]
          if (last?.role === 'assistant') {
            updated[updated.length - 1] = { ...last, content: `**Error:** ${err.message}` }
          }
          return updated
        })
      } finally {
        setIsStreaming(false)
      }
    },
    [messages, settings, loadedFiles, addFiles],
  )

  const clearMessages = useCallback(() => setMessages([]), [])

  return { messages, sendMessage, isStreaming, addQueryResult, clearMessages, loadedFiles, addFiles, replaceFile, removeFile }
}

// ── LLM streaming helpers ─────────────────────────────────────────────────────

async function streamOpenAI(
  messages: { role: string; content: string }[],
  apiKey: string,
  model: string,
  temperature: number,
  onChunk: (chunk: string) => void,
) {
  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({ model, messages, temperature, stream: true }),
  })
  if (!response.ok) {
    const err = await response.json().catch(() => ({}))
    throw new Error((err as any)?.error?.message ?? `OpenAI error ${response.status}`)
  }
  const reader = response.body!.getReader()
  const decoder = new TextDecoder()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    const text = decoder.decode(value)
    for (const line of text.split('\n')) {
      const trimmed = line.replace(/^data: /, '').trim()
      if (!trimmed || trimmed === '[DONE]') continue
      try {
        const json = JSON.parse(trimmed)
        const delta = json.choices?.[0]?.delta?.content
        if (delta) onChunk(delta)
      } catch {}
    }
  }
}

async function streamClaude(
  messages: { role: string; content: string }[],
  apiKey: string,
  model: string,
  temperature: number,
  onChunk: (chunk: string) => void,
) {
  const systemMessages = messages.filter((m) => m.role === 'system').map((m) => m.content).join('\n\n')
  const userMessages = messages.filter((m) => m.role !== 'system')

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model,
      max_tokens: 4096,
      temperature,
      system: systemMessages || undefined,
      messages: userMessages,
      stream: true,
    }),
  })
  if (!response.ok) {
    const err = await response.json().catch(() => ({}))
    throw new Error((err as any)?.error?.message ?? `Anthropic error ${response.status}`)
  }
  const reader = response.body!.getReader()
  const decoder = new TextDecoder()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    const text = decoder.decode(value)
    for (const line of text.split('\n')) {
      const trimmed = line.replace(/^data: /, '').trim()
      if (!trimmed) continue
      try {
        const json = JSON.parse(trimmed)
        if (json.type === 'content_block_delta' && json.delta?.text) {
          onChunk(json.delta.text)
        }
      } catch {}
    }
  }
}
