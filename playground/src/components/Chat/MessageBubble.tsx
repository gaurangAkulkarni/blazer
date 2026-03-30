import React from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { CodeBlock } from './CodeBlock'
import type { ChatMessage, AddResultFn, AttachedFile } from '../../lib/types'

interface Props {
  message: ChatMessage
  onAddResult: AddResultFn
  preferredLanguage: string
  loadedFiles: AttachedFile[]
}

export function MessageBubble({ message, onAddResult, preferredLanguage, loadedFiles }: Props) {
  const isUser = message.role === 'user'

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-[92%] rounded-2xl px-4 py-3 ${
          isUser
            ? 'bg-gray-900 text-white'
            : 'bg-gray-50 text-gray-900 border border-gray-100'
        }`}
      >
        {/* Attached files on user messages */}
        {isUser && message.attachedFiles && message.attachedFiles.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mb-2">
            {message.attachedFiles.map((f) => (
              <span
                key={f.path}
                className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-md bg-white/20 text-white/90"
                title={f.path}
              >
                <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>
                </svg>
                {f.name}
              </span>
            ))}
          </div>
        )}

        {isUser ? (
          <p className="text-sm whitespace-pre-wrap">{message.content}</p>
        ) : (
          <div className="prose prose-sm max-w-none prose-gray">
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                code({ node, className, children, ...props }) {
                  const match = /language-(\w+)/.exec(className || '')
                  const language = match ? match[1] : ''
                  const codeString = String(children).replace(/\n$/, '')

                  if (language) {
                    return (
                      <CodeBlock
                        code={codeString}
                        language={language}
                        onAddResult={onAddResult}
                        loadedFiles={loadedFiles}
                      />
                    )
                  }

                  return (
                    <code className="bg-gray-100 text-gray-800 px-1.5 py-0.5 rounded text-xs font-mono" {...props}>
                      {children}
                    </code>
                  )
                },
                table({ children }) {
                  return (
                    <div className="overflow-x-auto my-2">
                      <table className="min-w-full text-xs border-collapse">{children}</table>
                    </div>
                  )
                },
                th({ children }) {
                  return <th className="border border-gray-200 px-2 py-1 bg-gray-50 text-left font-medium text-gray-700">{children}</th>
                },
                td({ children }) {
                  return <td className="border border-gray-200 px-2 py-1 text-gray-600">{children}</td>
                },
              }}
            >
              {message.content}
            </ReactMarkdown>
          </div>
        )}
      </div>
    </div>
  )
}
