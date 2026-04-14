import { invoke } from '@tauri-apps/api/core'

export async function executeToolCall(name: string, args: Record<string, unknown>): Promise<unknown> {
  try {
    return await invoke('execute_tool_call', { name, arguments: args })
  } catch (error) {
    return { success: false, error: String(error) }
  }
}
