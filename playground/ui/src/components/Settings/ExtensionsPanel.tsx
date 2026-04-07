import React, { useState, useEffect, useCallback } from 'react'
import { invoke } from '@tauri-apps/api/core'
import type { AppSettings, AzureAuthMethod, ConnectionAlias } from '../../lib/types'

// ── Known extension catalogue ─────────────────────────────────────────────────

interface ExtCatalogue {
  id: string
  name: string
  description: string
  category: 'format' | 'database' | 'cloud' | 'analytics' | 'storage'
  needsConnection: boolean
  autoloaded?: boolean
  /** Label shown above the connection/path field in the form */
  connLabel?: string
  connPlaceholder?: string
  /** Short one-line hint shown below the field */
  connHelp?: string
  /** Longer usage tip shown in a callout — supports line breaks with \n */
  connTip?: string
}

const CATALOGUE: ExtCatalogue[] = [
  { id: 'httpfs', name: 'HTTP / S3', description: 'Read files over HTTP, HTTPS, S3, GCS, and R2', category: 'storage', needsConnection: false },
  { id: 'parquet', name: 'Parquet', description: 'Read and write Parquet files', category: 'format', needsConnection: false, autoloaded: true },
  { id: 'json', name: 'JSON', description: 'JSON read/write and extraction functions', category: 'format', needsConnection: false, autoloaded: true },
  { id: 'excel', name: 'Excel', description: 'Read .xlsx files directly with read_xlsx()', category: 'format', needsConnection: false },
  {
    id: 'delta',
    name: 'Delta Lake',
    description: 'Query Delta Lake tables natively — works with Databricks, local Delta tables, S3 and Azure Data Lake paths',
    category: 'format',
    needsConnection: true,
    connLabel: 'Table Path / URL',
    connPlaceholder: 's3://my-bucket/tables/my_delta_table/',
    connHelp: 'Path to the root of a Delta Lake table (S3, ADLS, GCS, or local)',
    connTip: 'Databricks (Azure): abfss://container@account.dfs.core.windows.net/path/to/table\nDatabricks (AWS):  s3://bucket/path/to/table\nLocal:             /absolute/path/to/delta_table\n\nLoad the azure or aws extension first to supply credentials, then use delta_scan(\'<path>\') or just SELECT * FROM \'<path>\'.',
  },
  {
    id: 'iceberg',
    name: 'Apache Iceberg',
    description: 'Query Apache Iceberg tables on S3, ADLS, GCS, or local storage',
    category: 'format',
    needsConnection: true,
    connLabel: 'Table Path / URL',
    connPlaceholder: 's3://my-bucket/warehouse/my_iceberg_table/',
    connHelp: 'Path to the root of an Iceberg table (must contain a metadata/ directory)',
    connTip: 'S3:    s3://bucket/warehouse/table_name\nADLS:  abfss://container@account.dfs.core.windows.net/warehouse/table\nLocal: /absolute/path/to/iceberg_table\n\nLoad the httpfs and aws/azure extensions first for cloud paths.',
  },
  { id: 'postgres', name: 'PostgreSQL', description: 'Attach and query Postgres databases directly', category: 'database', needsConnection: true, connLabel: 'Connection String', connPlaceholder: 'postgresql://user:pass@host:5432/dbname', connHelp: 'Standard PostgreSQL connection string' },
  { id: 'mysql', name: 'MySQL', description: 'Attach and query MySQL databases directly', category: 'database', needsConnection: true, connLabel: 'Connection String', connPlaceholder: 'mysql://user:pass@host:3306/dbname', connHelp: 'Standard MySQL connection string' },
  { id: 'sqlite', name: 'SQLite', description: 'Read and write SQLite database files', category: 'database', needsConnection: true, connLabel: 'File Path', connPlaceholder: '/absolute/path/to/database.db', connHelp: 'Absolute path to your .db or .sqlite file' },
  { id: 'spatial', name: 'Spatial', description: 'Geospatial types and PostGIS-style functions', category: 'analytics', needsConnection: false },
  { id: 'fts', name: 'Full-Text Search', description: 'Full-text search indexes via FTS5', category: 'analytics', needsConnection: false },
  { id: 'vss', name: 'Vector Search', description: 'Vector similarity search — cosine, L2 distance', category: 'analytics', needsConnection: false },
  { id: 'aws', name: 'AWS', description: 'AWS credential chain for S3 access', category: 'cloud', needsConnection: false },
  { id: 'azure', name: 'Azure', description: 'Azure Blob Storage / ADLS Gen2 access', category: 'cloud', needsConnection: false },
]

const CATEGORY_COLORS: Record<string, string> = {
  format: 'bg-blue-50 text-blue-700 border-blue-200',
  database: 'bg-violet-50 text-violet-700 border-violet-200',
  cloud: 'bg-sky-50 text-sky-700 border-sky-200',
  analytics: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  storage: 'bg-orange-50 text-orange-700 border-orange-200',
}

// ── Runtime extension status from DuckDB ─────────────────────────────────────

interface ExtensionStatus {
  name: string
  loaded: boolean
  installed: boolean
  description: string
}

// ── Connection Form ───────────────────────────────────────────────────────────

const AZURE_AUTH_OPTIONS: { value: AzureAuthMethod; label: string; hint: string }[] = [
  { value: 'none',               label: 'None (local / S3)',        hint: 'No Azure credentials — for local paths or S3 URIs.' },
  { value: 'service_principal',  label: 'Service Principal (Entra ID)', hint: 'App registration with client secret — recommended for Databricks / production.' },
  { value: 'account_key',        label: 'Storage Account Key',      hint: 'Paste the full Azure storage connection string from the Azure portal (Access keys → Connection string).' },
  { value: 'sas',                label: 'SAS Token / URL',          hint: 'Paste a full SAS connection string: BlobEndpoint=https://…;SharedAccessSignature=sv=…' },
  { value: 'azure_cli',          label: 'Azure CLI (az login)',      hint: 'Uses the credential from `az login` on this machine — zero secrets to paste.' },
]

function LabeledInput({ label, value, onChange, placeholder, mono = false, type = 'text' }: {
  label: string; value: string; onChange: (v: string) => void
  placeholder?: string; mono?: boolean; type?: string
}) {
  return (
    <div>
      <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">{label}</label>
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        autoComplete="off"
        className={`w-full text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 bg-white${mono ? ' font-mono' : ''}`}
        spellCheck={false}
      />
    </div>
  )
}

function ConnectionForm({
  initial,
  onSave,
  onCancel,
}: {
  initial?: ConnectionAlias
  onSave: (c: ConnectionAlias) => void
  onCancel: () => void
}) {
  const [name, setName] = useState(initial?.name ?? '')
  const [extType, setExtType] = useState(initial?.ext_type ?? 'postgres')
  const [connStr, setConnStr] = useState(initial?.connection_string ?? '')
  const [description, setDescription] = useState(initial?.description ?? '')

  // Azure credential state
  const [azureAuth, setAzureAuth] = useState<AzureAuthMethod>(initial?.azure_auth ?? 'none')
  const [azureTenantId, setAzureTenantId] = useState(initial?.azure_tenant_id ?? '')
  const [azureClientId, setAzureClientId] = useState(initial?.azure_client_id ?? '')
  const [azureClientSecret, setAzureClientSecret] = useState(initial?.azure_client_secret ?? '')
  const [azureStorageConnStr, setAzureStorageConnStr] = useState(initial?.azure_storage_connection_string ?? '')

  const cat = CATALOGUE.find((c) => c.id === extType)
  const isPathScan = ['delta', 'iceberg'].includes(extType)
  const authHint = AZURE_AUTH_OPTIONS.find((o) => o.value === azureAuth)?.hint

  const save = () => {
    if (!name.trim()) return
    const base: ConnectionAlias = {
      id: initial?.id ?? crypto.randomUUID(),
      name: name.trim(),
      ext_type: extType,
      connection_string: connStr.trim(),
      description: description.trim() || undefined,
    }
    if (isPathScan && azureAuth !== 'none') {
      base.azure_auth = azureAuth
      if (azureAuth === 'service_principal') {
        base.azure_tenant_id = azureTenantId.trim()
        base.azure_client_id = azureClientId.trim()
        base.azure_client_secret = azureClientSecret.trim()
      } else if (azureAuth === 'account_key' || azureAuth === 'sas') {
        base.azure_storage_connection_string = azureStorageConnStr.trim()
      }
    }
    onSave(base)
  }

  const isValid = name.trim() && (!cat?.needsConnection || connStr.trim()) &&
    !(isPathScan && azureAuth === 'service_principal' && (!azureTenantId.trim() || !azureClientId.trim() || !azureClientSecret.trim())) &&
    !(isPathScan && (azureAuth === 'account_key' || azureAuth === 'sas') && !azureStorageConnStr.trim())

  return (
    <div className="border border-gray-200 rounded-xl p-4 space-y-3 bg-gray-50">
      <div className="grid grid-cols-2 gap-3">
        <LabeledInput label="Name" value={name} onChange={setName} placeholder="e.g. prod-delta" />
        <div>
          <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">Extension</label>
          <select
            value={extType}
            onChange={(e) => { setExtType(e.target.value); setAzureAuth('none') }}
            className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 bg-white"
          >
            {CATALOGUE.map((c) => (
              <option key={c.id} value={c.id}>{c.name}</option>
            ))}
          </select>
        </div>
      </div>

      {cat?.needsConnection && (
        <div className="space-y-2">
          <div>
            <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">
              {cat.connLabel ?? 'Connection String'}
            </label>
            <input
              type="text"
              value={connStr}
              onChange={(e) => setConnStr(e.target.value)}
              placeholder={cat.connPlaceholder}
              className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 font-mono bg-white"
              spellCheck={false}
            />
            {cat.connHelp && (
              <p className="text-[11px] text-gray-400 mt-1">{cat.connHelp}</p>
            )}
          </div>
          {cat.connTip && (
            <div className="rounded-lg bg-blue-50 border border-blue-100 px-3 py-2.5">
              <p className="text-[10px] font-semibold text-blue-600 uppercase tracking-wide mb-1">Examples</p>
              <pre className="text-[11px] text-blue-800 whitespace-pre-wrap leading-relaxed font-mono">{cat.connTip}</pre>
            </div>
          )}
        </div>
      )}

      {/* ── Azure credentials (delta / iceberg only) ──────────────────────── */}
      {isPathScan && (
        <div className="space-y-2.5 border-t border-gray-200 pt-3">
          <div className="flex items-center gap-2">
            <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="text-gray-400 shrink-0"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
            <span className="text-[11px] font-semibold text-gray-600 uppercase tracking-wide">Azure Credentials</span>
            <span className="text-[10px] text-gray-400">(for ADLS Gen2 / abfss:// paths)</span>
          </div>

          <div>
            <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">Auth Method</label>
            <select
              value={azureAuth}
              onChange={(e) => setAzureAuth(e.target.value as AzureAuthMethod)}
              className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 bg-white"
            >
              {AZURE_AUTH_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
            {authHint && azureAuth !== 'none' && (
              <p className="text-[11px] text-gray-400 mt-1 leading-relaxed">{authHint}</p>
            )}
          </div>

          {azureAuth === 'service_principal' && (
            <div className="space-y-2 rounded-lg bg-amber-50 border border-amber-100 p-3">
              <p className="text-[10px] font-semibold text-amber-700 uppercase tracking-wide">Service Principal (Entra ID)</p>
              <LabeledInput label="Tenant ID" value={azureTenantId} onChange={setAzureTenantId}
                placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono />
              <LabeledInput label="Client ID (App ID)" value={azureClientId} onChange={setAzureClientId}
                placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono />
              <LabeledInput label="Client Secret" value={azureClientSecret} onChange={setAzureClientSecret}
                placeholder="your-client-secret-value" type="password" mono />
              <p className="text-[10px] text-amber-600 leading-relaxed">
                In Azure Portal → App registrations → your app → Certificates &amp; secrets → New client secret.
                Assign <strong>Storage Blob Data Reader</strong> role on the storage account.
              </p>
            </div>
          )}

          {(azureAuth === 'account_key' || azureAuth === 'sas') && (
            <div className="space-y-2 rounded-lg bg-amber-50 border border-amber-100 p-3">
              <p className="text-[10px] font-semibold text-amber-700 uppercase tracking-wide">
                {azureAuth === 'account_key' ? 'Storage Connection String' : 'SAS Connection String'}
              </p>
              <div>
                <input
                  type="password"
                  value={azureStorageConnStr}
                  onChange={(e) => setAzureStorageConnStr(e.target.value)}
                  placeholder={azureAuth === 'account_key'
                    ? 'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net'
                    : 'BlobEndpoint=https://account.blob.core.windows.net;SharedAccessSignature=sv=...'}
                  autoComplete="off"
                  className="w-full text-xs border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 bg-white font-mono"
                  spellCheck={false}
                />
              </div>
              <p className="text-[10px] text-amber-600 leading-relaxed">
                {azureAuth === 'account_key'
                  ? 'Find in Azure Portal → Storage account → Access keys → Connection string.'
                  : 'Generate in Azure Portal → Storage account → Shared access signature. Select Blob service.'}
              </p>
            </div>
          )}

          {azureAuth === 'azure_cli' && (
            <div className="rounded-lg bg-green-50 border border-green-100 px-3 py-2.5">
              <p className="text-[10px] font-semibold text-green-700 uppercase tracking-wide mb-1">Azure CLI Auth</p>
              <p className="text-[11px] text-green-700 leading-relaxed">
                Run <code className="font-mono bg-green-100 px-1 rounded">az login</code> in your terminal before querying.
                No secrets stored — DuckDB will use your local Azure CLI session automatically.
              </p>
            </div>
          )}
        </div>
      )}

      <div>
        <label className="text-[11px] font-medium text-gray-500 uppercase tracking-wide mb-1 block">Description <span className="normal-case font-normal">(optional)</span></label>
        <input
          type="text"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          placeholder="e.g. Databricks bronze layer — read-only"
          className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-gray-900/10 bg-white"
        />
      </div>

      <div className="flex gap-2 pt-1">
        <button
          onClick={save}
          disabled={!isValid}
          className="flex-1 bg-gray-900 text-white text-sm rounded-lg py-2 font-medium hover:bg-gray-700 disabled:opacity-30 transition"
        >
          {initial ? 'Save changes' : 'Add connection'}
        </button>
        <button
          onClick={onCancel}
          className="text-sm text-gray-400 hover:text-gray-600 px-4 py-2 rounded-lg hover:bg-gray-100 transition"
        >
          Cancel
        </button>
      </div>
    </div>
  )
}

// ── Main ExtensionsPanel ──────────────────────────────────────────────────────

interface Props {
  settings: AppSettings
  onUpdate: (partial: Partial<AppSettings>) => void
}

type Tab = 'extensions' | 'connections'

export function ExtensionsPanel({ settings, onUpdate }: Props) {
  const [tab, setTab] = useState<Tab>('extensions')
  const [statuses, setStatuses] = useState<Record<string, ExtensionStatus>>({})
  const [installing, setInstalling] = useState<Record<string, boolean>>({})
  const [installResults, setInstallResults] = useState<Record<string, { ok: boolean; msg: string }>>({})
  const [loadingStatuses, setLoadingStatuses] = useState(false)
  const [editingConn, setEditingConn] = useState<string | null>(null)
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null)

  const connections = settings.connections ?? []

  const loadStatuses = useCallback(async () => {
    setLoadingStatuses(true)
    try {
      const list = await invoke<ExtensionStatus[]>('list_duckdb_extensions')
      const map: Record<string, ExtensionStatus> = {}
      for (const e of list) map[e.name] = e
      setStatuses(map)
    } finally {
      setLoadingStatuses(false)
    }
  }, [])

  useEffect(() => {
    if (tab === 'extensions') loadStatuses()
  }, [tab, loadStatuses])

  const installExt = async (name: string) => {
    setInstalling((p) => ({ ...p, [name]: true }))
    setInstallResults((p) => { const n = { ...p }; delete n[name]; return n })
    try {
      const msg = await invoke<string>('install_duckdb_extension', { name })
      setInstallResults((p) => ({ ...p, [name]: { ok: true, msg } }))
      await loadStatuses()
    } catch (err) {
      setInstallResults((p) => ({ ...p, [name]: { ok: false, msg: String(err) } }))
    } finally {
      setInstalling((p) => ({ ...p, [name]: false }))
    }
  }

  const saveConnection = (conn: ConnectionAlias) => {
    const existing = connections.find((c) => c.id === conn.id)
    const updated = existing
      ? connections.map((c) => (c.id === conn.id ? conn : c))
      : [...connections, conn]
    onUpdate({ connections: updated })
    setEditingConn(null)
  }

  const deleteConnection = (id: string) => {
    onUpdate({ connections: connections.filter((c) => c.id !== id) })
    setDeleteConfirm(null)
  }

  return (
    <div className="space-y-4 w-full min-w-0">
      {/* Tab switcher */}
      <div className="flex gap-1 bg-gray-100 p-1 rounded-lg w-full">
        {(['extensions', 'connections'] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`flex-1 text-xs font-medium py-1.5 rounded-md transition-all capitalize ${
              tab === t
                ? 'bg-white text-gray-900 shadow-sm'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            {t === 'extensions' ? 'Core Extensions' : `Connections ${connections.length > 0 ? `(${connections.length})` : ''}`}
          </button>
        ))}
      </div>

      {/* ── Extensions tab ──────────────────────────────────────────────────── */}
      {tab === 'extensions' && (
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <p className="text-[11px] text-gray-400 leading-relaxed">
              Extensions are downloaded and cached locally. Auto-loaded ones activate automatically when querying compatible file types.
            </p>
            <button
              onClick={loadStatuses}
              disabled={loadingStatuses}
              className="shrink-0 ml-3 text-[11px] text-gray-400 hover:text-gray-700 px-1.5 py-0.5 rounded hover:bg-gray-100 transition"
              title="Refresh status"
            >
              {loadingStatuses ? (
                <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" className="animate-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
              ) : '↻ Refresh'}
            </button>
          </div>

          <div className="space-y-2">
            {CATALOGUE.map((ext) => {
              const status = statuses[ext.id]
              const isInstalled = status?.installed ?? false
              const isLoaded = status?.loaded ?? false
              const isInstalling = installing[ext.id] ?? false
              const result = installResults[ext.id]

              return (
                <div key={ext.id} className="flex items-start gap-3 p-3 border border-gray-100 rounded-xl hover:border-gray-200 transition bg-white">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-sm font-semibold text-gray-900">{ext.name}</span>
                      <span className={`text-[10px] px-1.5 py-0.5 rounded border font-medium capitalize ${CATEGORY_COLORS[ext.category]}`}>
                        {ext.category}
                      </span>
                      {ext.autoloaded && (
                        <span className="text-[10px] px-1.5 py-0.5 rounded border bg-gray-50 text-gray-500 border-gray-200 font-medium">
                          auto-loaded
                        </span>
                      )}
                      {isLoaded && (
                        <span className="flex items-center gap-1 text-[10px] text-green-600 font-medium bg-green-50 border border-green-200 px-1.5 py-0.5 rounded-full">
                          <svg xmlns="http://www.w3.org/2000/svg" width="7" height="7" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>
                          Loaded
                        </span>
                      )}
                      {isInstalled && !isLoaded && (
                        <span className="flex items-center gap-1 text-[10px] text-blue-600 font-medium bg-blue-50 border border-blue-200 px-1.5 py-0.5 rounded-full">
                          Installed
                        </span>
                      )}
                    </div>
                    <p className="text-[11px] text-gray-400 mt-0.5 leading-relaxed">{ext.description}</p>
                    {result && (
                      <p className={`text-[11px] mt-1 ${result.ok ? 'text-green-600' : 'text-red-500'}`}>
                        {result.ok ? '✓' : '✗'} {result.msg}
                      </p>
                    )}
                  </div>
                  <div className="shrink-0 flex flex-col items-end gap-1.5">
                    {!isInstalled ? (
                      <button
                        onClick={() => installExt(ext.id)}
                        disabled={isInstalling}
                        className="flex items-center gap-1 text-xs font-medium px-3 py-1.5 rounded-lg bg-gray-900 text-white hover:bg-gray-700 disabled:opacity-50 transition"
                      >
                        {isInstalling ? (
                          <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" className="animate-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
                        ) : (
                          <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                        )}
                        {isInstalling ? 'Installing…' : 'Install'}
                      </button>
                    ) : (
                      <button
                        onClick={() => installExt(ext.id)}
                        disabled={isInstalling}
                        className="flex items-center gap-1 text-xs font-medium px-3 py-1.5 rounded-lg border border-gray-200 text-gray-500 hover:bg-gray-50 disabled:opacity-50 transition"
                        title="Reinstall"
                      >
                        {isInstalling ? (
                          <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" className="animate-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
                        ) : '↻'}
                        {isInstalling ? 'Updating…' : 'Update'}
                      </button>
                    )}
                    {ext.needsConnection && (
                      <button
                        onClick={() => { setTab('connections'); setEditingConn('new') }}
                        className="text-[11px] text-violet-600 hover:text-violet-800 underline underline-offset-2"
                      >
                        + Add connection
                      </button>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* ── Connections tab ──────────────────────────────────────────────────── */}
      {tab === 'connections' && (
        <div className="space-y-3 min-w-0 w-full">
          <p className="text-[11px] text-gray-400 leading-relaxed">
            Named connection aliases let you attach the same extension to multiple instances — e.g. separate "prod" and "staging" Postgres databases.
            Select connections per-chat using the <span className="font-medium text-gray-600">⊕</span> button in the chat input.
          </p>

          {connections.length === 0 && editingConn !== 'new' && (
            <div className="text-center py-8 text-gray-400">
              <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="mx-auto mb-2 opacity-40"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
              <p className="text-sm font-medium text-gray-500">No connections yet</p>
              <p className="text-[11px] mt-1">Add a PostgreSQL, MySQL, or SQLite connection to query databases directly from chat.</p>
            </div>
          )}

          {connections.map((conn) => {
            const cat = CATALOGUE.find((c) => c.id === conn.ext_type)
            return (
              <div key={conn.id} className="min-w-0">
                {editingConn === conn.id ? (
                  <ConnectionForm
                    initial={conn as ConnectionAlias}
                    onSave={saveConnection}
                    onCancel={() => setEditingConn(null)}
                  />
                ) : (
                  <div className="flex items-start gap-3 p-3 border border-gray-100 rounded-xl hover:border-gray-200 transition bg-white">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-sm font-semibold text-gray-900">{conn.name}</span>
                        <span className={`text-[10px] px-1.5 py-0.5 rounded border font-medium ${CATEGORY_COLORS[cat?.category ?? 'database']}`}>
                          {cat?.name ?? conn.ext_type}
                        </span>
                      </div>
                      {conn.description && (
                        <p className="text-[11px] text-gray-400 mt-0.5">{conn.description}</p>
                      )}
                      {conn.connection_string && (
                        <p className="text-[11px] font-mono text-gray-400 mt-0.5 truncate">
                          {conn.connection_string.replace(/:([^:@]+)@/, ':●●●●@')}
                        </p>
                      )}
                      {conn.azure_auth && conn.azure_auth !== 'none' && (
                        <p className="text-[10px] text-amber-600 mt-0.5 flex items-center gap-1">
                          <svg xmlns="http://www.w3.org/2000/svg" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
                          {AZURE_AUTH_OPTIONS.find((o) => o.value === conn.azure_auth)?.label ?? conn.azure_auth}
                        </p>
                      )}
                    </div>
                    <div className="shrink-0 flex items-center gap-1">
                      <button
                        onClick={() => setEditingConn(conn.id)}
                        className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition"
                        title="Edit"
                      >
                        <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
                      </button>
                      {deleteConfirm === conn.id ? (
                        <div className="flex items-center gap-1">
                          <button
                            onClick={() => deleteConnection(conn.id)}
                            className="text-[11px] px-2 py-1 bg-red-600 text-white rounded-lg hover:bg-red-700 transition"
                          >
                            Delete
                          </button>
                          <button
                            onClick={() => setDeleteConfirm(null)}
                            className="text-[11px] px-2 py-1 text-gray-400 hover:text-gray-600 rounded-lg hover:bg-gray-100 transition"
                          >
                            Cancel
                          </button>
                        </div>
                      ) : (
                        <button
                          onClick={() => setDeleteConfirm(conn.id)}
                          className="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition"
                          title="Delete"
                        >
                          <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>
                        </button>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )
          })}

          {editingConn === 'new' ? (
            <ConnectionForm
              onSave={saveConnection}
              onCancel={() => setEditingConn(null)}
            />
          ) : (
            <button
              onClick={() => setEditingConn('new')}
              className="w-full flex items-center justify-center gap-2 py-2.5 border-2 border-dashed border-gray-200 rounded-xl text-sm text-gray-400 hover:text-gray-600 hover:border-gray-300 transition"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
              Add connection
            </button>
          )}
        </div>
      )}
    </div>
  )
}
