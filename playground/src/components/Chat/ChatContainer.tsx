import React, { memo } from 'react'
import { MessageList } from './MessageList'
import { InputBar } from './InputBar'
import type { ChatMessage, AddResultFn, AttachedFile } from '../../lib/types'

interface Props {
  messages: ChatMessage[]
  isStreaming: boolean
  onSendMessage: (content: string, attachments?: AttachedFile[]) => void
  onAddResult: AddResultFn
  preferredLanguage: string
  loadedFiles: AttachedFile[]
  onRemoveFile: (path: string) => void
  onReplaceFile: (oldPath: string, newFile: AttachedFile) => void
}

export const ChatContainer = memo(function ChatContainer({
  messages,
  isStreaming,
  onSendMessage,
  onAddResult,
  preferredLanguage,
  loadedFiles,
  onRemoveFile,
  onReplaceFile,
}: Props) {
  return (
    <div className="flex-1 flex flex-col min-h-0">
      <MessageList
        messages={messages}
        isStreaming={isStreaming}
        onAddResult={onAddResult}
        preferredLanguage={preferredLanguage}
        loadedFiles={loadedFiles}
      />
      <InputBar
        onSend={onSendMessage}
        disabled={isStreaming}
        loadedFiles={loadedFiles}
        onRemoveFile={onRemoveFile}
        onReplaceFile={onReplaceFile}
      />
    </div>
  )
})
