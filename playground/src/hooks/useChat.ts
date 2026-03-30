import { useState, useCallback, useRef } from 'react'
import type { ChatMessage, AppSettings, AttachedFile } from '../lib/types'
import { BUILT_IN_SKILLS, resolveSkillPrompts } from '../lib/skills'

let messageIdCounter = 0
function nextId() {
  return `msg-${++messageIdCounter}-${Date.now()}`
}

export function useChat(settings: AppSettings) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [loadedFiles, setLoadedFiles] = useState<AttachedFile[]>([])
  const streamingRef = useRef('')

  const addFiles = useCallback((files: AttachedFile[]) => {
    setLoadedFiles((prev) => {
      const existing = new Set(prev.map((f) => f.path))
      const newFiles = files.filter((f) => !existing.has(f.path))
      return [...prev, ...newFiles]
    })
  }, [])

  const removeFile = useCallback((path: string) => {
    setLoadedFiles((prev) => prev.filter((f) => f.path !== path))
  }, [])

  const replaceFile = useCallback((oldPath: string, newFile: AttachedFile) => {
    setLoadedFiles((prev) => prev.map((f) => (f.path === oldPath ? newFile : f)))
  }, [])

  const sendMessage = useCallback(
    async (content: string, newAttachments?: AttachedFile[]) => {
      // Merge any new attachments
      let allFiles = loadedFiles
      if (newAttachments && newAttachments.length > 0) {
        addFiles(newAttachments)
        const existing = new Set(loadedFiles.map((f) => f.path))
        const added = newAttachments.filter((f) => !existing.has(f.path))
        allFiles = [...loadedFiles, ...added]
      }

      const userMsg: ChatMessage = {
        id: nextId(),
        role: 'user',
        content,
        timestamp: Date.now(),
        attachedFiles: newAttachments,
      }

      const assistantMsg: ChatMessage = {
        id: nextId(),
        role: 'assistant',
        content: '',
        timestamp: Date.now(),
      }

      setMessages((prev) => [...prev, userMsg, assistantMsg])
      setIsStreaming(true)
      streamingRef.current = ''

      // Set up streaming listeners
      window.blazerAPI.removeStreamListeners()
      window.blazerAPI.onStreamChunk((chunk) => {
        streamingRef.current += chunk
        setMessages((prev) => {
          const updated = [...prev]
          const last = updated[updated.length - 1]
          if (last.role === 'assistant') {
            updated[updated.length - 1] = { ...last, content: streamingRef.current }
          }
          return updated
        })
      })

      window.blazerAPI.onStreamEnd(() => {
        setIsStreaming(false)
      })

      const provider = settings.activeProvider
      const providerSettings = settings[provider]

      // Build file context for the LLM
      let fileContext = ''
      if (allFiles.length > 0) {
        fileContext = '## Loaded Data Files\nIMPORTANT: The user has attached the following files. You MUST use EXACTLY these full absolute paths in your code — never use just the filename:\n'
        for (const f of allFiles) {
          const reader =
            f.ext === 'csv' || f.ext === 'tsv' ? 'readCsv' :
            f.ext === 'parquet_dir' ? 'scanParquet' : 'readParquet'
          const note = f.ext === 'parquet_dir'
            ? ' [partitioned folder → returns LazyFrame, do NOT call .lazy(), chain directly]'
            : ''
          fileContext += `- Full path: \`${f.path}\` → call \`${reader}("${f.path}")\`${note}\n`
          if (f.columns && f.columns.length > 0) {
            fileContext += `  Columns (case-sensitive, use EXACT names): ${f.columns.map((c) => `\`${c}\``).join(', ')}\n`
          }
        }
        fileContext += '\nDo NOT shorten these paths or alter column names. Copy them verbatim into your code.\n'
      }

      // Embed full file paths directly in the user message so the LLM cannot miss them
      const enrichedContent = fileContext
        ? `${content}\n\n${fileContext}`
        : content

      const apiMessages = messages
        .concat({ ...userMsg, content: enrichedContent })
        .map((m) => ({ role: m.role, content: m.content }))

      // Inject file context as a system-level hint
      if (fileContext) {
        apiMessages.unshift({ role: 'system', content: fileContext })
      }

      // Inject active skill prompts as a system message
      const skillPrompt = resolveSkillPrompts(
        settings.activeSkills ?? ['blazer-engine'],
        (settings.customSkills ?? []).map((s) => ({ ...s, builtIn: false })),
      )
      if (skillPrompt) {
        apiMessages.unshift({ role: 'system', content: skillPrompt })
      }

      try {
        await window.blazerAPI.sendMessage(apiMessages, {
          provider,
          apiKey: providerSettings.apiKey,
          model: providerSettings.model,
          temperature: providerSettings.temperature,
        })
      } catch (err: any) {
        setMessages((prev) => {
          const updated = [...prev]
          const last = updated[updated.length - 1]
          if (last.role === 'assistant') {
            updated[updated.length - 1] = {
              ...last,
              content: `**Error:** ${err.message}`,
            }
          }
          return updated
        })
        setIsStreaming(false)
      }
    },
    [messages, settings, loadedFiles, addFiles],
  )

  const addExecutionResult = useCallback((messageId: string, result: any) => {
    setMessages((prev) =>
      prev.map((m) =>
        m.id === messageId
          ? { ...m, executionResults: [...(m.executionResults || []), result] }
          : m,
      ),
    )
  }, [])

  const clearMessages = useCallback(() => {
    setMessages([])
  }, [])

  return {
    messages,
    sendMessage,
    isStreaming,
    addExecutionResult,
    clearMessages,
    loadedFiles,
    addFiles,
    replaceFile,
    removeFile,
  }
}
