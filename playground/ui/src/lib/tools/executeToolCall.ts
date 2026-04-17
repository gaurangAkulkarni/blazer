import { invoke } from '@tauri-apps/api/core'
import type { AttachedFile } from '../types'
import { toAlias } from '../fileAlias'
import { readExpr } from '../readExpr'
import { appLog } from '../appLog'

export async function executeToolCall(
  name: string,
  args: Record<string, unknown>,
  /** Loaded files — injected so Rust recreates alias VIEWs in each fresh connection */
  files?: AttachedFile[],
): Promise<unknown> {
  try {
    // Always inject a `files` array so the Rust handler can:
    //   1. Recreate alias VIEWs (run_sql / get_sample_rows / column_stats)
    //   2. DESCRIBE actual file paths (describe_tables)
    // Each entry carries: alias, reader expression, path, ext
    const filesMeta = (files ?? []).map(f => ({
      alias:  f.alias  ?? toAlias(f),
      reader: f.readerExpr ?? readExpr(f),
      path:   f.path,
      ext:    f.ext,
    }))

    const augmentedArgs = filesMeta.length > 0
      ? { ...args, files: filesMeta }
      : args

    appLog.debug('tool', `invoke: ${name}`, { args: JSON.stringify(augmentedArgs).slice(0, 200) })
    const result = await invoke('execute_tool_call', { name, arguments: augmentedArgs })
    appLog.debug('tool', `result: ${name}`, { result: JSON.stringify(result).slice(0, 200) })
    return result
  } catch (error) {
    appLog.error('tool', `Tool invoke failed: ${name} — ${String(error)}`)
    return { success: false, error: String(error) }
  }
}
