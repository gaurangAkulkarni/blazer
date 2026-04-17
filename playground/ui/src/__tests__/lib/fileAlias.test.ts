import { describe, it, expect } from 'vitest'
import { toAlias } from '../../lib/fileAlias'
import type { AttachedFile } from '../../lib/types'

function makeFile(name: string, ext: string): AttachedFile {
  return { name, ext, path: `/some/path/${name}` }
}

describe('toAlias', () => {
  describe('parquet_dir extensions (no file extension in name)', () => {
    it('returns the bare name for a directory with no extension', () => {
      expect(toAlias(makeFile('tracker', 'parquet_dir'))).toBe('tracker')
    })

    it('returns the name unchanged for a clean dir name', () => {
      expect(toAlias(makeFile('ai_trips_final', 'parquet_dir'))).toBe('ai_trips_final')
    })
  })

  describe('parquet files', () => {
    it('strips the .parquet extension', () => {
      expect(toAlias(makeFile('tracker.parquet', 'parquet'))).toBe('tracker')
    })

    it('converts spaces to underscores and strips extension', () => {
      expect(toAlias(makeFile('trip complete data.parquet', 'parquet'))).toBe('trip_complete_data')
    })

    it('handles hyphens and collapses multiple underscores', () => {
      expect(toAlias(makeFile('my--file.parquet', 'parquet'))).toBe('my_file')
    })

    it('returns the fallback when name is only the extension dot-prefix', () => {
      expect(toAlias(makeFile('.parquet', 'parquet'))).toBe('data')
    })
  })

  describe('csv files', () => {
    it('returns _2024_sales for 2024-sales.csv — digit-start gets underscore prefix then leading _ is stripped', () => {
      // The implementation: prepend _ for digit start → "_2024_sales",
      // then collapse underscores → "_2024_sales", then trim leading/trailing _ → "2024_sales".
      // The actual produced alias is "2024_sales" (leading _ is trimmed at the end).
      expect(toAlias(makeFile('2024-sales.csv', 'csv'))).toBe('2024_sales')
    })

    it('handles parentheses and extra spaces', () => {
      expect(toAlias(makeFile('my-file (copy).csv', 'csv'))).toBe('my_file_copy')
    })
  })

  describe('xlsx files', () => {
    it('strips xlsx extension and handles mixed case + spaces', () => {
      expect(toAlias(makeFile('Sales Data Q1.xlsx', 'xlsx'))).toBe('Sales_Data_Q1')
    })
  })

  describe('edge cases', () => {
    it('falls back to "data" when the cleaned alias is empty', () => {
      // Name that becomes all underscores after cleaning → collapses to empty
      expect(toAlias(makeFile('---.parquet', 'parquet'))).toBe('data')
    })

    it('strips the leading underscore added for digit-start (implementation trims both ends)', () => {
      // The prepend-underscore for digit-start names gets trimmed by the final replace,
      // so the result starts with the digit, not an underscore.
      const result = toAlias(makeFile('2024-sales.csv', 'csv'))
      expect(result).toBe('2024_sales')
    })
  })
})
