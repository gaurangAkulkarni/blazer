import { ipcMain, BrowserWindow } from 'electron'
import OpenAI from 'openai'
import Anthropic from '@anthropic-ai/sdk'
import { getSystemPrompt } from '../../src/lib/systemPrompt'

interface ChatMessage {
  role: 'user' | 'assistant' | 'system'
  content: string
}

interface LLMSettings {
  provider: 'openai' | 'claude'
  apiKey: string
  model: string
  temperature: number
}

export function registerLLMHandlers(getWindow: () => BrowserWindow | null) {
  ipcMain.handle('llm:send', async (_event, messages: ChatMessage[], settings: LLMSettings) => {
    const win = getWindow()
    const systemPrompt = getSystemPrompt('javascript')

    const fullMessages: ChatMessage[] = [
      { role: 'system', content: systemPrompt },
      ...messages,
    ]

    try {
      if (settings.provider === 'openai') {
        return await streamOpenAI(fullMessages, settings, win)
      } else {
        return await streamClaude(fullMessages, settings, win)
      }
    } catch (err: any) {
      throw new Error(`LLM Error: ${err.message}`)
    }
  })
}

async function streamOpenAI(
  messages: ChatMessage[],
  settings: LLMSettings,
  win: BrowserWindow | null,
): Promise<string> {
  const client = new OpenAI({ apiKey: settings.apiKey })

  const stream = await client.chat.completions.create({
    model: settings.model,
    temperature: settings.temperature,
    messages: messages.map((m) => ({ role: m.role as any, content: m.content })),
    stream: true,
  })

  let full = ''
  for await (const chunk of stream) {
    const delta = chunk.choices[0]?.delta?.content || ''
    if (delta) {
      full += delta
      win?.webContents.send('llm:chunk', delta)
    }
  }
  win?.webContents.send('llm:end')
  return full
}

async function streamClaude(
  messages: ChatMessage[],
  settings: LLMSettings,
  win: BrowserWindow | null,
): Promise<string> {
  const client = new Anthropic({ apiKey: settings.apiKey })

  // Separate system from conversation messages — combine ALL system messages
  const systemMsg = messages
    .filter((m) => m.role === 'system')
    .map((m) => m.content)
    .join('\n\n')
  const convMessages = messages
    .filter((m) => m.role !== 'system')
    .map((m) => ({ role: m.role as 'user' | 'assistant', content: m.content }))

  const stream = client.messages.stream({
    model: settings.model,
    max_tokens: 4096,
    temperature: settings.temperature,
    system: systemMsg,
    messages: convMessages,
  })

  let full = ''
  stream.on('text', (text) => {
    full += text
    win?.webContents.send('llm:chunk', text)
  })

  await stream.finalMessage()
  win?.webContents.send('llm:end')
  return full
}
