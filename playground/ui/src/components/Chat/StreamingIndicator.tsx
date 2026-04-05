import React, { useState, useEffect, useRef } from 'react'

const MESSAGES = [
  // Analytical
  'Thinking about your data…',
  'Analyzing the schema…',
  'Crafting the perfect query…',
  'Scanning through columns…',
  'Reading between the rows…',
  'Reasoning over your files…',
  'Building the query plan…',
  'Weighing query strategies…',
  'Deciphering the schema…',
  'Cross-referencing columns…',
  'Checking the data landscape…',
  'Mapping your intent to a query…',
  'Considering all the angles…',
  'Connecting the dots…',
  'Parsing your request…',
  'Formulating a response…',
  'Checking edge cases…',
  'Filtering the noise…',
  'Aggregating thoughts…',

  // Progress
  'Loading the mental model…',
  'Processing at full speed…',
  'Running through possibilities…',
  'Deep in thought…',
  'Thinking very hard…',
  'Almost there…',
  'Making sense of it all…',
  'Your answer is taking shape…',
  'Running the mental benchmark…',
  'This one is interesting…',
  'Almost done thinking…',

  // Blazer / data flavoured
  'Staring at the Parquet files…',
  'Counting rows in my head…',
  'Drafting the perfect JSON…',
  'Summoning query wisdom…',
  'Blazing through the data…',
  'Joining the right tables mentally…',

  // SQL humour
  'SELECT * FROM brain…',
  'WHERE patience IS NOT NULL…',
  'GROUP BY common sense…',
  'ORDER BY quality DESC…',
  'LIMIT 1 perfect answer…',
  'No NULL results allowed…',
  'Optimising the approach…',
  'Running EXPLAIN ANALYZE on thoughts…',
]

function shuffle<T>(arr: T[]): T[] {
  const a = [...arr]
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]]
  }
  return a
}

const INTERVAL_MS = 2200
const FADE_MS = 320

/**
 * Hook that returns two text slots (A and B) and which is currently
 * the active (visible) one. The inactive slot pre-loads the next message
 * so both can crossfade simultaneously with no layout jump.
 */
export function useRotatingMessage() {
  const queueRef = useRef<string[]>(shuffle(MESSAGES))
  const idxRef   = useRef(Math.floor(Math.random() * MESSAGES.length))

  const [slotA, setSlotA] = useState<string>(() => queueRef.current[idxRef.current])
  const [slotB, setSlotB] = useState<string>('')
  const [active, setActive] = useState<'a' | 'b'>('a')

  useEffect(() => {
    const tick = setInterval(() => {
      idxRef.current = (idxRef.current + 1) % queueRef.current.length
      if (idxRef.current === 0) queueRef.current = shuffle(MESSAGES)
      const next = queueRef.current[idxRef.current]

      // Write the next message into the INACTIVE slot, then flip active
      setActive((prev) => {
        if (prev === 'a') { setSlotB(next); return 'b' }
        else               { setSlotA(next); return 'a' }
      })
    }, INTERVAL_MS)
    return () => clearInterval(tick)
  }, [])

  return { slotA, slotB, active, FADE_MS }
}

// ── Crossfade text container ──────────────────────────────────────────────────
//
// Renders two absolutely-positioned spans on top of each other.
// An invisible spacer (the active text) drives the container's natural width
// so there is never a layout shift — only an opacity crossfade.

interface CrossfadeTextProps {
  className?: string
}

function CrossfadeText({ className = '' }: CrossfadeTextProps) {
  const { slotA, slotB, active, FADE_MS } = useRotatingMessage()

  const fadeStyle = (isActive: boolean): React.CSSProperties => ({
    position: 'absolute',
    inset: 0,
    whiteSpace: 'nowrap',
    opacity: isActive ? 1 : 0,
    transition: `opacity ${FADE_MS}ms ease-in-out`,
    pointerEvents: 'none',
  })

  return (
    <span className="relative inline-block">
      {/* Invisible spacer — active text drives the layout width */}
      <span className={className} style={{ opacity: 0, whiteSpace: 'nowrap' }} aria-hidden>
        {active === 'a' ? slotA : slotB}
      </span>
      {/* Slot A */}
      <span className={className} style={fadeStyle(active === 'a')}>{slotA}</span>
      {/* Slot B */}
      <span className={className} style={fadeStyle(active === 'b')}>{slotB}</span>
    </span>
  )
}

// ── Bouncing wave indicator (3 bars) ─────────────────────────────────────────

function BouncingWave({ size = 'md' }: { size?: 'sm' | 'md' }) {
  // All bars same height; the bounce animation moves them up/down uniformly
  const h = size === 'sm' ? 10 : 12
  const w = 3
  return (
    <span
      className="flex items-center gap-[3px] shrink-0"
      style={{ height: h + 4 }} // +4 gives room for the bounce travel
    >
      {[0, 1, 2].map((i) => (
        <span
          key={i}
          className="rounded-full bg-gray-400 animate-bounce"
          style={{
            width: w,
            height: h,
            animationDelay: `${i * 120}ms`,
            animationDuration: '0.7s',
          }}
        />
      ))}
    </span>
  )
}

// ── Full bubble — shown while waiting for the very first chunk ────────────────

export function StreamingIndicator() {
  return (
    <div className="flex justify-start">
      <div className="bg-gray-50 border border-gray-100 rounded-2xl px-4 py-3 max-w-[72%]">
        <div className="flex items-center gap-2.5">
          <BouncingWave size="md" />
          <CrossfadeText className="text-xs text-gray-500" />
        </div>
      </div>
    </div>
  )
}

// ── Slim strip — shown while content is already streaming in ─────────────────

export function StreamingBar() {
  return (
    <div className="flex items-center gap-2 pl-1 select-none">
      <BouncingWave size="sm" />
      <CrossfadeText className="text-[11px] text-gray-400" />
    </div>
  )
}
