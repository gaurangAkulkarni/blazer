import { describe, it, expect } from 'vitest'
import { readExpr, readerForPath } from '../../lib/readExpr'
import type { AttachedFile } from '../../lib/types'

function makeFile(path: string, ext: string, name = 'file'): AttachedFile {
  return { path, ext, name }
}

describe('readExpr', () => {
  describe('CSV / TSV files', () => {
    it('produces read_csv_auto for a .csv file', () => {
      const f = makeFile('/path/data.csv', 'csv')
      expect(readExpr(f)).toBe("read_csv_auto('/path/data.csv')")
    })

    it('produces read_csv_auto for a .tsv file', () => {
      const f = makeFile('/path/data.tsv', 'tsv')
      expect(readExpr(f)).toBe("read_csv_auto('/path/data.tsv')")
    })
  })

  describe('Parquet files', () => {
    it('produces read_parquet for a .parquet file', () => {
      const f = makeFile('/path/data.parquet', 'parquet')
      expect(readExpr(f)).toBe("read_parquet('/path/data.parquet')")
    })

    it('produces a glob pattern for parquet_dir', () => {
      const f = makeFile('/path/tracker', 'parquet_dir', 'tracker')
      expect(readExpr(f)).toBe("read_parquet('/path/tracker/**/*.parquet')")
    })
  })

  describe('XLSX files', () => {
    it('produces read_xlsx with all_varchar=true for a single xlsx', () => {
      const f = makeFile('/path/data.xlsx', 'xlsx')
      expect(readExpr(f)).toBe("read_xlsx('/path/data.xlsx', all_varchar=true)")
    })

    it('produces a glob pattern for xlsx_dir', () => {
      const f = makeFile('/path/data', 'xlsx_dir')
      expect(readExpr(f)).toBe("read_xlsx('/path/data/*.xlsx', all_varchar=true)")
    })
  })

  describe('JSON files', () => {
    it('produces read_json_auto for .json', () => {
      const f = makeFile('/path/data.json', 'json')
      expect(readExpr(f)).toBe("read_json_auto('/path/data.json')")
    })

    it('produces read_json_auto for .ndjson', () => {
      const f = makeFile('/path/data.ndjson', 'ndjson')
      expect(readExpr(f)).toBe("read_json_auto('/path/data.ndjson')")
    })
  })

  describe('CSV dir', () => {
    it('produces a glob pattern for csv_dir', () => {
      const f = makeFile('/path/data', 'csv_dir')
      expect(readExpr(f)).toBe("read_csv_auto('/path/data/*.csv')")
    })
  })

  describe('Unknown extension', () => {
    it('defaults to read_parquet for an unknown extension', () => {
      const f = makeFile('/path/data.xyz', 'xyz')
      expect(readExpr(f)).toBe("read_parquet('/path/data.xyz')")
    })
  })

  describe('Single-quote escaping', () => {
    it("escapes single quotes in the path using SQL '' convention", () => {
      const f = makeFile("/Users/john o'brien/data.parquet", 'parquet')
      expect(readExpr(f)).toBe("read_parquet('/Users/john o''brien/data.parquet')")
    })

    it("escapes single quotes in a csv path", () => {
      const f = makeFile("/data/it's.csv", 'csv')
      expect(readExpr(f)).toBe("read_csv_auto('/data/it''s.csv')")
    })
  })
})

describe('readerForPath', () => {
  it('returns read_csv_auto for a .csv path', () => {
    expect(readerForPath('/data/file.csv')).toBe("read_csv_auto('/data/file.csv')")
  })

  it('returns read_csv_auto for a .tsv path', () => {
    expect(readerForPath('/data/file.tsv')).toBe("read_csv_auto('/data/file.tsv')")
  })

  it('returns read_parquet for a .parquet path', () => {
    expect(readerForPath('/data/file.parquet')).toBe("read_parquet('/data/file.parquet')")
  })

  it('returns read_xlsx with all_varchar for a .xlsx path', () => {
    expect(readerForPath('/data/file.xlsx')).toBe("read_xlsx('/data/file.xlsx', all_varchar=true)")
  })

  it('returns read_json_auto for a .json path', () => {
    expect(readerForPath('/data/file.json')).toBe("read_json_auto('/data/file.json')")
  })

  it('returns read_json_auto for a .ndjson path', () => {
    expect(readerForPath('/data/file.ndjson')).toBe("read_json_auto('/data/file.ndjson')")
  })

  it('falls back to read_parquet for an unknown extension', () => {
    expect(readerForPath('/data/file.abc')).toBe("read_parquet('/data/file.abc')")
  })

  it("escapes single quotes in the path", () => {
    expect(readerForPath("/data/o'brien.parquet")).toBe("read_parquet('/data/o''brien.parquet')")
  })
})
