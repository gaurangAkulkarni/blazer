/**
 * markdownIcons.tsx
 *
 * Maps leading emoji in LLM-generated markdown headings / list items to
 * the equivalent lucide-react icon component so the UI renders proper
 * vector icons instead of emoji glyphs.
 *
 * ASCII art symbols (✓ ✗ ▲ ▼ → ← • ·) are intentionally NOT covered —
 * those pass through untouched.
 */
import React from 'react'
import {
  BarChart2, TrendingUp, TrendingDown, Search, Zap, Lightbulb,
  AlertTriangle, CheckCircle, XCircle, Info, Calendar, Clock,
  Hash, Tag, MapPin, Users, User, FileText, Folder, Package,
  Wrench, Star, Link2, RefreshCw, Download, Upload, Bell, Lock,
  Target, Key, Globe, Eye, Database, Filter, Table2, List,
  Activity, Award, Bookmark, MessageSquare, Settings, Layers,
  GitBranch, HelpCircle, Shield, Map, Percent, ArrowRight,
} from 'lucide-react'

// ── Emoji → lucide icon map ───────────────────────────────────────────────────
// Sorted longer entries first so the regex matches ⚠️ (2 chars) before ⚠ (1 char).
export const EMOJI_ICON_MAP: Array<[string, React.ElementType]> = [
  // Charts / data
  ['📊', BarChart2],
  ['📈', TrendingUp],
  ['📉', TrendingDown],
  ['📋', List],
  ['📃', FileText],
  ['📄', FileText],
  ['📝', FileText],

  // Search / insights
  ['🔍', Search],
  ['🔎', Search],
  ['👁️', Eye],
  ['👁',  Eye],

  // Status / alerts
  ['⚠️', AlertTriangle],
  ['⚠',  AlertTriangle],
  ['✅', CheckCircle],
  ['❌', XCircle],
  ['ℹ️', Info],
  ['ℹ',  Info],
  ['❓', HelpCircle],

  // Speed / energy / action
  ['⚡', Zap],
  ['🚀', Zap],
  ['🎯', Target],

  // Time / calendar
  ['📅', Calendar],
  ['📆', Calendar],
  ['🗓️', Calendar],
  ['🗓',  Calendar],
  ['🕐', Clock],
  ['⏰', Clock],
  ['⏱️', Clock],
  ['⏱',  Clock],

  // People
  ['👥', Users],
  ['👤', User],

  // Storage / files
  ['🗄️', Database],
  ['🗄',  Database],
  ['📁', Folder],
  ['📂', Folder],
  ['📦', Package],
  ['📥', Download],
  ['📤', Upload],

  // Tags / location
  ['🏷️', Tag],
  ['🏷',  Tag],
  ['📍', MapPin],
  ['📌', MapPin],
  ['🔖', Bookmark],

  // Numbers / math
  ['🔢', Hash],
  ['%',  Percent],

  // Tools / settings
  ['🔧', Wrench],
  ['🛠️', Wrench],
  ['🛠',  Wrench],
  ['⚙️', Settings],
  ['⚙',  Settings],

  // Awards / stars
  ['🏆', Award],
  ['⭐️', Star],
  ['⭐', Star],
  ['🌟', Star],

  // Navigation / links
  ['🔗', Link2],
  ['➡️', ArrowRight],
  ['➡',  ArrowRight],
  ['🔄', RefreshCw],

  // Comms / alerts
  ['🔔', Bell],
  ['💬', MessageSquare],

  // Idea / misc
  ['💡', Lightbulb],
  ['🔑', Key],
  ['🔒', Lock],
  ['🔓', Lock],
  ['🛡️', Shield],
  ['🛡',  Shield],

  // Geo
  ['🌍', Globe],
  ['🌎', Globe],
  ['🌏', Globe],
  ['🗺️', Map],
  ['🗺',  Map],

  // Structure
  ['📐', Layers],
  ['🔀', GitBranch],
  ['🔽', Filter],
  ['🔼', Activity],
  ['📻', Activity],
]

// Build a regex that matches only these exact emoji at the start of a string
const PATTERN = EMOJI_ICON_MAP
  .map(([e]) => e.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
  .join('|')
export const LEADING_EMOJI_RE = new RegExp(`^(${PATTERN})\\s*`)

// ── Parse helper ─────────────────────────────────────────────────────────────

export interface EmojiParseResult {
  /** The resolved lucide icon, or null if no leading emoji / emoji not mapped */
  Icon: React.ElementType | null
  /** Children with the leading emoji (and its trailing space) removed */
  rest: React.ReactNode
}

/**
 * If `children` starts with a mapped emoji, return the corresponding lucide
 * icon component + the remaining text.  If the emoji is not in the map the
 * emoji is still stripped (no icon returned).  ASCII symbols are untouched.
 */
export function parseLeadingEmoji(children: React.ReactNode): EmojiParseResult {
  // Pull out the leading string from children (string or array-of-nodes)
  const first =
    typeof children === 'string'
      ? children
      : Array.isArray(children) && typeof (children as React.ReactNode[])[0] === 'string'
        ? (children as string[])[0]
        : null

  if (!first) return { Icon: null, rest: children }

  const match = LEADING_EMOJI_RE.exec(first)
  if (!match) return { Icon: null, rest: children }

  const emoji   = match[1]
  const trimmed = first.slice(match[0].length)

  // Look up lucide icon (null = unknown emoji, still stripped)
  const entry = EMOJI_ICON_MAP.find(([e]) => e === emoji)
  const Icon  = entry ? entry[1] : null

  // Reconstruct children without the leading emoji
  const rest: React.ReactNode =
    typeof children === 'string'
      ? trimmed
      : Array.isArray(children)
        ? [trimmed, ...(children as React.ReactNode[]).slice(1)]
        : children

  return { Icon, rest }
}

// ── Convenience wrapper ───────────────────────────────────────────────────────

interface HeadingChildrenProps { children?: React.ReactNode }

/**
 * Renders children with the leading emoji swapped for a lucide icon.
 * Wrap your h1/h2/h3/li custom renderers with this.
 */
export function WithLeadingIcon({ children, iconSize = 13, iconClass = 'text-gray-500 dark:text-gray-400 shrink-0' }: HeadingChildrenProps & { iconSize?: number; iconClass?: string }) {
  const { Icon, rest } = parseLeadingEmoji(children)
  if (!Icon) return <>{children}</>
  return (
    <span className="inline-flex items-center gap-1.5">
      <Icon size={iconSize} className={iconClass} />
      {rest}
    </span>
  )
}
