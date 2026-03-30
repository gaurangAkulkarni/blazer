export interface TextSegment {
  type: 'text'
  content: string
}

export interface CodeSegment {
  type: 'code'
  language: string
  content: string
}

export type Segment = TextSegment | CodeSegment

/**
 * Parse an LLM markdown response into alternating text and code segments.
 */
export function parseResponse(markdown: string): Segment[] {
  const segments: Segment[] = []
  const codeBlockRegex = /```(\w+)?\n([\s\S]*?)```/g

  let lastIndex = 0
  let match: RegExpExecArray | null

  while ((match = codeBlockRegex.exec(markdown)) !== null) {
    // Text before this code block
    if (match.index > lastIndex) {
      const text = markdown.slice(lastIndex, match.index).trim()
      if (text) segments.push({ type: 'text', content: text })
    }

    segments.push({
      type: 'code',
      language: match[1] || 'javascript',
      content: match[2].trim(),
    })

    lastIndex = match.index + match[0].length
  }

  // Trailing text
  if (lastIndex < markdown.length) {
    const text = markdown.slice(lastIndex).trim()
    if (text) segments.push({ type: 'text', content: text })
  }

  return segments
}
