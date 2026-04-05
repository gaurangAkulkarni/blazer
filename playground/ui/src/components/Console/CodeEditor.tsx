/**
 * CodeEditor — CodeMirror 6 wrapper used by ConsoleEditor.
 *
 * Features:
 *  • SQL mode with schema-aware autocomplete (column/table names from loaded files)
 *  • JSON mode for Blazer queries
 *  • Syntax highlighting (custom light theme matching the app palette)
 *  • Line numbers, bracket matching, undo/redo, search
 *  • Cmd/Ctrl+Enter runs the query
 *  • Dynamic schema updates without recreating the editor (via Compartment)
 *  • Exposes focus() + setValue() to parent via ref
 */
import { forwardRef, useEffect, useImperativeHandle, useRef } from 'react'
import { EditorView, keymap, lineNumbers, highlightActiveLine, highlightActiveLineGutter, placeholder as cmPlaceholder } from '@codemirror/view'
import { EditorState, Compartment } from '@codemirror/state'
import { defaultKeymap, historyKeymap, history, indentWithTab } from '@codemirror/commands'
import { indentOnInput, bracketMatching, HighlightStyle, syntaxHighlighting, foldGutter } from '@codemirror/language'
import { autocompletion, closeBrackets, closeBracketsKeymap, completionKeymap } from '@codemirror/autocomplete'
import { sql, StandardSQL } from '@codemirror/lang-sql'
import { json } from '@codemirror/lang-json'
import { tags } from '@lezer/highlight'

// ── Public interface ──────────────────────────────────────────────────────────

export interface CodeEditorRef {
  focus: () => void
  setValue: (text: string) => void
}

export interface SqlSchema {
  [tableName: string]: string[]
}

interface Props {
  value: string
  onChange: (v: string) => void
  language: 'sql' | 'json'
  sqlSchema?: SqlSchema
  onRun?: () => void
  placeholder?: string
}

// ── Theme ─────────────────────────────────────────────────────────────────────

const appTheme = EditorView.theme({
  '&': {
    height: '100%',
    fontSize: '12.5px',
    fontFamily: "'JetBrains Mono', 'Cascadia Code', 'Fira Code', 'Menlo', monospace",
    backgroundColor: '#ffffff',
  },
  '&.cm-focused': { outline: 'none' },
  '.cm-scroller': { overflow: 'auto', lineHeight: '1.7' },
  '.cm-content': {
    padding: '14px 16px',
    caretColor: '#374151',
    minHeight: '100%',
  },
  '.cm-line': { padding: '0' },
  // Cursor
  '.cm-cursor, .cm-dropCursor': {
    borderLeftColor: '#374151',
    borderLeftWidth: '2px',
  },
  // Selection
  '&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground, .cm-selectionBackground, ::selection': {
    backgroundColor: '#dbeafe',
  },
  // Gutter
  '.cm-gutters': {
    backgroundColor: '#f9fafb',
    color: '#9ca3af',
    border: 'none',
    borderRight: '1px solid #f3f4f6',
    paddingRight: '4px',
    userSelect: 'none',
  },
  '.cm-lineNumbers .cm-gutterElement': { minWidth: '36px', textAlign: 'right' },
  '.cm-activeLineGutter': { backgroundColor: '#f0f1f3', color: '#6b7280' },
  // Active line
  '.cm-activeLine': { backgroundColor: '#fafafa' },
  // Fold gutter
  '.cm-foldGutter': { width: '16px' },
  '.cm-foldGutter .cm-gutterElement': { cursor: 'pointer' },
  // Bracket matching
  '.cm-matchingBracket': { backgroundColor: '#ddd6fe', color: '#4f46e5 !important' },
  '.cm-nonmatchingBracket': { backgroundColor: '#fee2e2' },
  // Placeholder
  '.cm-placeholder': { color: '#d1d5db', fontStyle: 'italic' },
  // ── Autocomplete tooltip ────────────────────────────────────────────────
  '.cm-tooltip': {
    border: '1px solid #e5e7eb',
    borderRadius: '10px',
    boxShadow: '0 8px 24px rgba(0,0,0,0.10)',
    backgroundColor: '#ffffff',
    overflow: 'hidden',
    padding: '4px 0',
    marginTop: '4px',
  },
  '.cm-tooltip-autocomplete': {
    '& > ul': {
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: '12px',
      maxHeight: '220px',
    },
    '& > ul > li': {
      padding: '4px 12px',
      color: '#374151',
    },
    '& > ul > li[aria-selected="true"]': {
      backgroundColor: '#eff6ff',
      color: '#1d4ed8',
    },
  },
  '.cm-completionLabel': { fontSize: '12px' },
  '.cm-completionDetail': {
    fontSize: '11px',
    color: '#9ca3af',
    marginLeft: '8px',
    fontStyle: 'normal',
  },
  '.cm-completionIcon': { paddingRight: '6px', opacity: '0.7' },
  // Tooltip for keyword/type completions
  '.cm-completionIcon-keyword::after': { content: "'k'" },
  '.cm-completionIcon-variable::after': { content: "'x'" },
  // Search
  '.cm-searchMatch': { backgroundColor: '#fef3c7' },
  '.cm-searchMatch-selected': { backgroundColor: '#fde68a' },
})

// ── Syntax highlight theme ────────────────────────────────────────────────────

const highlightStyle = HighlightStyle.define([
  { tag: tags.keyword,                    color: '#4f46e5', fontWeight: '600' },  // indigo — SQL keywords
  { tag: tags.string,                     color: '#0284c7' },                      // sky — string literals
  { tag: tags.comment,                    color: '#9ca3af', fontStyle: 'italic' }, // gray — comments
  { tag: tags.number,                     color: '#d97706' },                      // amber — numbers
  { tag: tags.bool,                       color: '#4f46e5' },                      // indigo — true/false
  { tag: tags.null,                       color: '#9ca3af' },                      // gray — null
  { tag: tags.operator,                   color: '#374151' },                      // gray-700 — operators
  { tag: tags.punctuation,               color: '#6b7280' },                      // gray-500 — commas, parens
  { tag: tags.function(tags.variableName), color: '#7c3aed' },                    // violet — function calls
  { tag: tags.special(tags.variableName), color: '#2563eb' },                     // blue — special vars
  { tag: tags.typeName,                   color: '#0f766e' },                      // teal — type names
  { tag: tags.propertyName,              color: '#2563eb' },                      // blue — JSON keys
  { tag: tags.variableName,              color: '#1f2937' },                      // gray-800 — identifiers
  { tag: tags.angleBracket,              color: '#6b7280' },
])

// ── Language factory (for Compartment reconfiguration) ────────────────────────

function buildLanguage(language: 'sql' | 'json', sqlSchema?: SqlSchema) {
  if (language === 'json') return json()
  return sql({
    dialect: StandardSQL,
    schema: sqlSchema ?? {},
    upperCaseKeywords: false,
  })
}

// ── Component ─────────────────────────────────────────────────────────────────

export const CodeEditor = forwardRef<CodeEditorRef, Props>(function CodeEditor(
  { value, onChange, language, sqlSchema, onRun, placeholder },
  ref,
) {
  const containerRef = useRef<HTMLDivElement>(null)
  const viewRef      = useRef<EditorView | null>(null)
  const langConf     = useRef(new Compartment())
  // Keep mutable callbacks out of extension closures so editor isn't recreated
  const onChangeRef  = useRef(onChange)
  const onRunRef     = useRef(onRun)
  useEffect(() => { onChangeRef.current = onChange }, [onChange])
  useEffect(() => { onRunRef.current = onRun }, [onRun])

  // ── Expose imperative API ────────────────────────────────────────────────
  useImperativeHandle(ref, () => ({
    focus: () => viewRef.current?.focus(),
    setValue: (text: string) => {
      const view = viewRef.current
      if (!view) return
      view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: text } })
    },
  }), [])

  // ── Create editor on mount ───────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return

    const runCmd = () => { onRunRef.current?.(); return true }

    const state = EditorState.create({
      doc: value,
      extensions: [
        // Core
        history(),
        indentOnInput(),
        bracketMatching(),
        closeBrackets(),
        autocompletion({ defaultKeymap: true }),
        foldGutter(),
        lineNumbers(),
        highlightActiveLine(),
        highlightActiveLineGutter(),

        // Key bindings
        keymap.of([
          { key: 'Mod-Enter', run: runCmd },
          ...closeBracketsKeymap,
          ...defaultKeymap,
          ...historyKeymap,
          ...completionKeymap,
          indentWithTab,
        ]),

        // Language (Compartment = can be reconfigured without recreating)
        langConf.current.of(buildLanguage(language, sqlSchema)),

        // Theme + highlighting
        appTheme,
        syntaxHighlighting(highlightStyle),

        // Placeholder text
        placeholder ? cmPlaceholder(placeholder) : [],

        // Change listener
        EditorView.updateListener.of(update => {
          if (update.docChanged) {
            onChangeRef.current(update.state.doc.toString())
          }
        }),
      ],
    })

    const view = new EditorView({ state, parent: containerRef.current })
    viewRef.current = view
    return () => { view.destroy(); viewRef.current = null }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps — only run on mount

  // ── Sync value from parent (replay / clear / format) ────────────────────
  useEffect(() => {
    const view = viewRef.current
    if (!view) return
    if (view.state.doc.toString() !== value) {
      view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: value } })
    }
  }, [value])

  // ── Update SQL schema dynamically ────────────────────────────────────────
  useEffect(() => {
    const view = viewRef.current
    if (!view) return
    view.dispatch({
      effects: langConf.current.reconfigure(buildLanguage(language, sqlSchema)),
    })
  }, [language, sqlSchema])

  return (
    <div
      ref={containerRef}
      className="w-full h-full overflow-hidden"
      // Prevent the browser context-menu from swallowing right-clicks inside
      onContextMenu={e => e.stopPropagation()}
    />
  )
})
