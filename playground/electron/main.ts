import { app, BrowserWindow, ipcMain, dialog } from 'electron'
import path from 'path'
import { readFileSync } from 'fs'
import { registerLLMHandlers } from './ipc/llm'
import { registerExecutorHandlers } from './ipc/executor'
import { registerSettingsHandlers } from './ipc/settings'

let mainWindow: BrowserWindow | null = null

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 800,
    minHeight: 600,
    titleBarStyle: 'hiddenInset',
    webPreferences: {
      preload: path.join(__dirname, '../preload/preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  })

  if (process.env.NODE_ENV === 'development') {
    mainWindow.loadURL('http://localhost:5173')
  } else {
    mainWindow.loadFile(path.join(__dirname, '../renderer/index.html'))
  }

  mainWindow.on('closed', () => {
    mainWindow = null
  })
}

app.whenReady().then(() => {
  registerSettingsHandlers()
  registerLLMHandlers(() => mainWindow)
  registerExecutorHandlers()

  // DevTools toggle
  ipcMain.handle('devtools:toggle', () => {
    const win = mainWindow
    if (!win) return
    if (win.webContents.isDevToolsOpened()) {
      win.webContents.closeDevTools()
    } else {
      win.webContents.openDevTools()
    }
  })

  // Convert CSV → Parquet
  ipcMain.handle('file:convertToParquet', async (_event, csvPath: string) => {
    const outPath = csvPath.replace(/\.(csv|tsv)$/i, '.parquet')
    const nativeAddonPath = (() => {
      const candidates = [
        path.resolve(__dirname, '../../..', 'crates/node-bindings'),
        path.resolve(__dirname, '../../../..', 'crates/node-bindings'),
      ]
      for (const c of candidates) {
        if (require('fs').existsSync(path.join(c, 'index.js'))) return c
      }
      return candidates[0]
    })()
    const script = `
const { readCsv, writeParquet } = require(${JSON.stringify(nativeAddonPath)});
const df = readCsv(${JSON.stringify(csvPath)});
writeParquet(df, ${JSON.stringify(outPath)});
process.stdout.write(${JSON.stringify(outPath)});
`
    const os = require('os')
    const cp = require('child_process')
    const tmpFile = path.join(os.tmpdir(), `blazer-convert-${Date.now()}.cjs`)
    require('fs').writeFileSync(tmpFile, script)
    return new Promise((resolve, reject) => {
      cp.execFile(
        'node',
        ['--max-old-space-size=8192', tmpFile],
        { timeout: 600000 },
        (err: any, stdout: string, stderr: string) => {
          try { require('fs').unlinkSync(tmpFile) } catch {}
          if (err) reject(new Error(stderr || err.message))
          else resolve(outPath)
        }
      )
    })
  })

  // Folder picker for partitioned Parquet datasets
  ipcMain.handle('dialog:openFolder', async () => {
    const result = await dialog.showOpenDialog({
      properties: ['openDirectory'],
      title: 'Select Partitioned Parquet Folder',
    })
    if (result.canceled || result.filePaths.length === 0) return null
    const folderPath = result.filePaths[0]
    const name = path.basename(folderPath)
    return { path: folderPath, name, ext: 'parquet_dir' }
  })

  // File picker for data files
  ipcMain.handle('dialog:openFiles', async () => {
    const result = await dialog.showOpenDialog({
      properties: ['openFile', 'multiSelections'],
      filters: [
        { name: 'Data Files', extensions: ['csv', 'parquet', 'tsv', 'json', 'arrow', 'ipc'] },
        { name: 'All Files', extensions: ['*'] },
      ],
    })
    if (result.canceled) return []
    return result.filePaths.map((filePath) => {
      const ext = path.extname(filePath).slice(1).toLowerCase()
      let columns: string[] | undefined
      if (ext === 'csv' || ext === 'tsv') {
        try {
          // Read only first 4KB to get the header line
          const buf = Buffer.alloc(4096)
          const fd = require('fs').openSync(filePath, 'r')
          const bytesRead = require('fs').readSync(fd, buf, 0, 4096, 0)
          require('fs').closeSync(fd)
          const firstLine = buf.slice(0, bytesRead).toString('utf-8').split(/\r?\n/)[0]
          const sep = ext === 'tsv' ? '\t' : ','
          columns = firstLine.split(sep).map((c: string) => c.trim().replace(/^["']|["']$/g, ''))
        } catch {}
      }
      return { path: filePath, columns }
    })
  })

  createWindow()
})

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})

app.on('activate', () => {
  if (mainWindow === null) createWindow()
})
