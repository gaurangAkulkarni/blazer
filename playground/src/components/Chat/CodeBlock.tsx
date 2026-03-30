import React, { useState, useRef } from 'react'
import hljs from 'highlight.js/lib/core'
import javascript from 'highlight.js/lib/languages/javascript'
import python from 'highlight.js/lib/languages/python'
import 'highlight.js/styles/github.css'
import { useCodeExecution } from '../../hooks/useCodeExecution'
import type { AddResultFn, AttachedFile } from '../../lib/types'

hljs.registerLanguage('javascript', javascript)
hljs.registerLanguage('js', javascript)
hljs.registerLanguage('python', python)
hljs.registerLanguage('py', python)

interface Props {
  code: string
  language: string
  onAddResult: AddResultFn
  loadedFiles?: AttachedFile[]
}

export function CodeBlock({ code, language, onAddResult, loadedFiles }: Props) {
  const { execute, isExecuting } = useCodeExecution(loadedFiles)
  const [copied, setCopied] = useState(false)
  const [runStatus, setRunStatus] = useState<'idle' | 'success' | 'error'>('idle')
  const lastResultId = useRef<string | null>(null)

  const lang = hljs.getLanguage(language) ? language : 'plaintext'
  const highlighted = hljs.highlight(code, { language: lang }).value

  const handleRun = async () => {
    setRunStatus('idle')
    const result = await execute(code, language)
    setRunStatus(result.success ? 'success' : 'error')
    // Use first non-empty, non-comment line as the label; extract string from console.log(...)
    const rawLabel =
      code
        .trim()
        .split('\n')
        .find((l) => l.trim() && !l.trim().startsWith('//')) || code.slice(0, 60)
    const consoleMatch = rawLabel.trim().match(/^console\.\w+\s*\(\s*(['"`])(.*?)\1/)
    const label = consoleMatch ? consoleMatch[2] : rawLabel
    const id = onAddResult(label.trim(), code, result)
    lastResultId.current = id
  }

  const handleHeaderClick = () => {
    if (!lastResultId.current) return
    const el = document.getElementById(`result-${lastResultId.current}`)
    el?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }

  const handleCopy = () => {
    navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const isRunnable = ['javascript', 'js', 'python', 'py'].includes(language)

  return (
    <div className="code-block my-2 relative group">
      {/* Header bar */}
      <div
        className={`flex items-center justify-between px-3 py-1.5 bg-gray-100 border-b border-gray-200 text-xs text-gray-500 ${lastResultId.current ? 'cursor-pointer hover:bg-gray-200 transition-colors' : ''}`}
        onClick={handleHeaderClick}
        title={lastResultId.current ? 'Click to jump to result' : undefined}
      >
        <span className="font-medium flex items-center gap-1.5">
          {language}
          {lastResultId.current && (
            <span className="text-gray-400 text-[10px]">↗ jump to result</span>
          )}
        </span>
        <div className="flex items-center gap-1.5">
          <button
            onClick={handleCopy}
            className="px-2 py-0.5 rounded hover:bg-gray-200 transition text-gray-500 hover:text-gray-700"
          >
            {copied ? 'Copied!' : 'Copy'}
          </button>
          {isRunnable && (
            <button
              onClick={handleRun}
              disabled={isExecuting}
              className={`px-2.5 py-0.5 rounded font-medium transition ${
                isExecuting
                  ? 'bg-gray-200 text-gray-400 cursor-wait'
                  : runStatus === 'success'
                  ? 'bg-green-600 text-white hover:bg-green-700'
                  : runStatus === 'error'
                  ? 'bg-red-500 text-white hover:bg-red-600'
                  : 'bg-gray-900 text-white hover:bg-gray-700'
              }`}
            >
              {isExecuting ? 'Running...' : runStatus === 'success' ? '✓ Done' : runStatus === 'error' ? '✗ Error' : '▶ Run'}
            </button>
          )}
        </div>
      </div>
      <pre className="!m-0 !bg-[#f8f8fa]">
        <code
          className={`language-${lang} hljs`}
          dangerouslySetInnerHTML={{ __html: highlighted }}
        />
      </pre>
    </div>
  )
}
