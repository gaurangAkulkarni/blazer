import { useState, useCallback } from 'react'
import type { ExecutionResult, AttachedFile } from '../lib/types'

export function useCodeExecution(loadedFiles?: AttachedFile[]) {
  const [isExecuting, setIsExecuting] = useState(false)

  const execute = useCallback(
    async (code: string, language: string = 'javascript'): Promise<ExecutionResult> => {
      setIsExecuting(true)
      try {
        const files = loadedFiles?.map((f) => ({ path: f.path, ext: f.ext })) || []
        const result = await window.blazerAPI.executeCode(code, language, files)
        return result
      } catch (err: any) {
        return {
          success: false,
          stdout: '',
          stderr: err.message,
          durationMs: 0,
          dataframes: [],
        }
      } finally {
        setIsExecuting(false)
      }
    },
    [loadedFiles],
  )

  return { execute, isExecuting }
}
