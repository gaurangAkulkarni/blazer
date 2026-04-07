/**
 * Runtime OS detection.
 * The Blazer JSON engine is only available on macOS — all other platforms
 * are DuckDB-only.
 */
export const isMac =
  /Mac/.test(navigator.platform) || /Mac OS X/.test(navigator.userAgent)
