import { ipcMain } from 'electron'
import { execFile, fork } from 'child_process'
import { writeFileSync, unlinkSync } from 'fs'
import { join } from 'path'
import { randomUUID } from 'crypto'
import { resolve } from 'path'
import { tmpdir } from 'os'

interface ExecutionResult {
  success: boolean
  stdout: string
  stderr: string
  durationMs: number
  dataframes: Array<{
    data: Record<string, unknown>[]
    columns: string[]
    shape: [number, number]
  }>
}

function getNativeAddonPath(): string {
  const candidates = [
    resolve(__dirname, '../../..', 'crates/node-bindings'),
    resolve(__dirname, '../../../..', 'crates/node-bindings'),
    resolve(__dirname, '../../../../..', 'crates/node-bindings'),
  ]
  for (const c of candidates) {
    try {
      if (require('fs').existsSync(join(c, 'index.js'))) return c
    } catch {}
  }
  return candidates[0]
}

const NATIVE_ADDON_PATH = getNativeAddonPath()

// ---------------------------------------------------------------------------
// Worker script — runs in a forked child process for crash isolation.
// A native Rust/addon crash (OOM, SIGTRAP) kills only the child, not Electron.
// Receives: { addonPath, userCode, filesPreamble } via process.on('message').
// Returns:  ExecutionResult via process.send().
// ---------------------------------------------------------------------------
const WORKER_SCRIPT = /* js */ `
const vm = require('vm');
const start = Date.now();

process.on('message', ({ addonPath, userCode, filesPreamble }) => {
  // ── Load native addon ──────────────────────────────────────────────────────
  let col, litInt, litFloat, litStr, litBool,
      readCsv, readParquet, scanParquet, writeParquet,
      DataFrame, LazyFrame, Expr;
  try {
    const blazer = require(addonPath);
    ({ col, litInt, litFloat, litStr, litBool,
       readCsv, readParquet, scanParquet, writeParquet,
       DataFrame, LazyFrame, Expr } = blazer);
  } catch (e) {
    process.send({ success: false, stdout: '', stderr: 'Failed to load Blazer addon: ' + e.message, durationMs: 0, dataframes: [] });
    process.exit(0);
    return;
  }

  // ── Prototype patches ──────────────────────────────────────────────────────
  const _origGroupBy = LazyFrame.prototype.groupBy;
  LazyFrame.prototype.groupBy = function(keys, aggs) {
    if (aggs !== undefined) return _origGroupBy.call(this, keys, aggs);
    const self = this;
    return { agg: (a) => _origGroupBy.call(self, keys, a) };
  };
  if (!Expr.prototype.truediv) Expr.prototype.truediv = Expr.prototype.div;
  if (!Expr.prototype.mode)    Expr.prototype.mode    = function() { return this.first(); };

  // ── Helper functions ───────────────────────────────────────────────────────
  function lit(v) {
    if (typeof v === 'number') return Number.isInteger(v) ? litInt(v) : litFloat(v);
    if (typeof v === 'string') return litStr(v);
    if (typeof v === 'boolean') return litBool(v);
    throw new Error('lit() accepts number, string, or boolean');
  }
  function convertCsvToParquet(csvPath, outPath) {
    writeParquet(readCsv(csvPath), outPath);
    return outPath;
  }
  function unionAll(...frames) {
    if (!frames.length) throw new Error('unionAll requires at least one DataFrame');
    let result = null;
    for (const f of frames) {
      const df = (f && typeof f.collect === 'function') ? f.collect() : f;
      result = result ? result.vstack(df) : df;
    }
    return result;
  }

  // ── Output capture ─────────────────────────────────────────────────────────
  const stdoutParts = [];
  const stderrParts = [];
  const resultDataframes = [];

  function tryCaptureDf(val) {
    if (val && typeof val === 'object' &&
        typeof val.toJSON === 'function' && typeof val.columns === 'function') {
      try {
        resultDataframes.push({
          data: JSON.parse(val.toJSON()),
          columns: val.columns(),
          shape: [val.height(), val.width()],
        });
      } catch (e) {
        stderrParts.push('DataFrame render error: ' + e.message + '\\n');
      }
      return true;
    }
    return false;
  }

  // ── vm sandbox ─────────────────────────────────────────────────────────────
  const sandbox = vm.createContext({
    col, litInt, litFloat, litStr, litBool,
    readCsv, readParquet, scanParquet, writeParquet,
    DataFrame, LazyFrame, Expr,
    lit, convertCsvToParquet, unionAll,
    __blazer_last_result: undefined,
    console: {
      log: (...args) => {
        const text = [];
        for (const a of args) {
          if (!tryCaptureDf(a))
            text.push(typeof a === 'string' ? a : JSON.stringify(a, null, 2));
        }
        if (text.length) stdoutParts.push(text.join(' ') + '\\n');
      },
      info:  (...args) => stdoutParts.push(args.map(String).join(' ') + '\\n'),
      warn:  (...args) => stdoutParts.push('[warn] ' + args.map(String).join(' ') + '\\n'),
      error: (...args) => stderrParts.push(args.map(String).join(' ') + '\\n'),
    },
    process: { stdout: { write: (s) => stdoutParts.push(s) }, env: {} },
  });

  // ── Execute ────────────────────────────────────────────────────────────────
  const fullCode = (filesPreamble || '') + '\\n' + userCode;
  try {
    vm.runInContext(fullCode, sandbox, { timeout: 290000 });

    const r = sandbox.__blazer_last_result;
    if (r !== undefined && r !== null) {
      if (!tryCaptureDf(r)) {
        stdoutParts.push((typeof r === 'string' ? r : JSON.stringify(r, null, 2)) + '\\n');
      }
    }

    process.send({
      success: true,
      stdout: stdoutParts.join('').trim(),
      stderr: '',
      durationMs: Date.now() - start,
      dataframes: resultDataframes,
    });
  } catch (e) {
    const stack = e.stack ? e.stack.split('\\n').slice(1, 4).join('\\n') : '';
    process.send({
      success: false,
      stdout: stdoutParts.join('').trim(),
      stderr: e.message + (stack ? '\\n' + stack : ''),
      durationMs: Date.now() - start,
      dataframes: resultDataframes,
    });
  }
  process.exit(0);
});
`

// ---------------------------------------------------------------------------
// Write worker script to a temp file once at module load.
// fork() requires a file on disk (unlike worker_threads eval:true).
// ---------------------------------------------------------------------------
let _workerScriptPath: string | null = null

function getWorkerScriptPath(): string {
  if (!_workerScriptPath) {
    _workerScriptPath = join(tmpdir(), `blazer-worker-${randomUUID()}.cjs`)
    writeFileSync(_workerScriptPath, WORKER_SCRIPT, 'utf-8')
  }
  return _workerScriptPath
}

// ---------------------------------------------------------------------------
// Code transformation: find last expression and prefix with assignment
// ---------------------------------------------------------------------------
function transformCode(code: string): string {
  const lines = code.trim().split('\n')
  let depth = 0
  let exprStart = lines.length - 1
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i]
    for (let j = line.length - 1; j >= 0; j--) {
      const ch = line[j]
      if (ch === ')' || ch === ']' || ch === '}') depth++
      else if (ch === '(' || ch === '[' || ch === '{') depth = Math.max(0, depth - 1)
    }
    exprStart = i
    if (depth === 0 && !line.trimStart().startsWith('.')) break
  }
  const startLine = lines[exprStart]
  const isExpression = !startLine.match(
    /^\s*(const|let|var|if|for|while|function|class|return|try|switch|import|export)\b/,
  )
  if (isExpression) {
    lines[exprStart] = `__blazer_last_result = ${startLine.trimStart()}`
  }
  return lines.join('\n')
}

// ---------------------------------------------------------------------------
// Build auto-injection preamble for loaded files
// ---------------------------------------------------------------------------
function buildFilesPreamble(code: string, loadedFiles?: { path: string; ext: string }[]): string {
  if (!loadedFiles || loadedFiles.length === 0) return ''
  let preamble = ''
  for (let i = 0; i < loadedFiles.length; i++) {
    const f = loadedFiles[i]
    const varName = i === 0 ? 'df' : `df${i + 1}`
    const reader =
      f.ext === 'csv' || f.ext === 'tsv' ? 'readCsv' :
      f.ext === 'parquet_dir' ? 'scanParquet' : 'readParquet'
    const alreadyDefined = new RegExp(`\\b(const|let|var)\\s+${varName}\\b`).test(code)
    if (!alreadyDefined) {
      preamble += `var ${varName} = ${reader}(${JSON.stringify(f.path)});\n`
    }
  }
  return preamble
}

// ---------------------------------------------------------------------------
// IPC handler registration
// ---------------------------------------------------------------------------
export function registerExecutorHandlers() {
  ipcMain.handle(
    'executor:run',
    async (
      _event,
      code: string,
      language: string,
      loadedFiles?: { path: string; ext: string }[],
    ): Promise<ExecutionResult> => {
      if (language === 'javascript' || language === 'js') {
        return executeJavaScript(code, loadedFiles)
      } else if (language === 'python' || language === 'py') {
        return executePython(code)
      }
      return { success: false, stdout: '', stderr: `Unsupported language: ${language}`, durationMs: 0, dataframes: [] }
    },
  )
}

// ---------------------------------------------------------------------------
// JavaScript execution via child_process.fork — crash isolated.
// If the Rust addon crashes (OOM, SIGTRAP, infinite recursion), only the
// child process dies. Electron receives the exit signal and shows an error.
// ---------------------------------------------------------------------------
function executeJavaScript(
  code: string,
  loadedFiles?: { path: string; ext: string }[],
): Promise<ExecutionResult> {
  return new Promise((resolve) => {
    const start = Date.now()
    const userCode = transformCode(code)
    const filesPreamble = buildFilesPreamble(code, loadedFiles)
    let settled = false

    const done = (result: ExecutionResult) => {
      if (settled) return
      settled = true
      clearTimeout(timeoutHandle)
      resolve(result)
    }

    const scriptPath = getWorkerScriptPath()
    const child = fork(scriptPath, [], {
      execArgv: ['--max-old-space-size=8192'],
      silent: true, // don't inherit stdio — all data comes via IPC
    })

    const timeoutHandle = setTimeout(() => {
      child.kill('SIGKILL')
      done({
        success: false, stdout: '', stderr: 'Execution timeout (5 minutes)',
        durationMs: Date.now() - start, dataframes: [],
      })
    }, 300_000)

    child.send({ addonPath: NATIVE_ADDON_PATH, userCode, filesPreamble })

    child.on('message', (result: ExecutionResult) => {
      done(result)
    })

    child.on('error', (err) => {
      done({ success: false, stdout: '', stderr: err.message, durationMs: Date.now() - start, dataframes: [] })
    })

    child.on('exit', (code, signal) => {
      if (signal) {
        done({
          success: false, stdout: '',
          stderr: `Engine crashed (signal ${signal}). This is usually an out-of-memory or parquet format issue. Try using scanParquet + .limit() to read a smaller slice, or convert to a simpler parquet first.`,
          durationMs: Date.now() - start, dataframes: [],
        })
      } else if (code !== 0 && code !== null) {
        done({
          success: false, stdout: '',
          stderr: `Engine exited with code ${code}. The file may be corrupted or use an unsupported parquet encoding.`,
          durationMs: Date.now() - start, dataframes: [],
        })
      }
    })
  })
}

// ---------------------------------------------------------------------------
// Python execution (unchanged — still uses child process)
// ---------------------------------------------------------------------------
function executePython(code: string): Promise<ExecutionResult> {
  return new Promise((resolve) => {
    const { join: pjoin } = require('path')
    const tmpFile = pjoin(tmpdir(), `blazer-exec-${randomUUID()}.py`)
    const start = Date.now()
    try {
      writeFileSync(tmpFile, `import sys\nsys.path.insert(0, '.')\n` + code, 'utf-8')
    } catch (err: any) {
      return resolve({ success: false, stdout: '', stderr: `Failed to write temp file: ${err.message}`, durationMs: 0, dataframes: [] })
    }
    execFile('python3', [tmpFile], { timeout: 30000, maxBuffer: 10 * 1024 * 1024 },
      (error, stdout, stderr) => {
        const durationMs = Date.now() - start
        try { unlinkSync(tmpFile) } catch {}
        resolve({ success: !error, stdout: stdout.trim(), stderr: stderr || (error ? error.message : ''), durationMs, dataframes: [] })
      },
    )
  })
}
