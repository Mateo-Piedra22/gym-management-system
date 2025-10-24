# Run bidirectional replication orchestrator
$ErrorActionPreference = 'Stop'

# Resolve repo root
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)

# Resolve Python
$pythonCandidates = @(
    'C:\Python313\python.exe',
    "$env:LocalAppData\Programs\Python\Python313\python.exe",
    "$env:LocalAppData\Programs\Python\Python312\python.exe",
    "$env:LocalAppData\Programs\Python\Python311\python.exe",
    "$env:ProgramFiles\Python313\python.exe",
    "$env:ProgramFiles\Python312\python.exe",
    "$env:ProgramFiles\Python311\python.exe"
)
$python = $null
foreach ($p in $pythonCandidates) { if ($p -and (Test-Path $p)) { $python = $p; break } }
if (-not $python) { $python = 'python' }

# Prefer pythonw.exe if available to avoid console (optional)
$exe = $python
try {
  $cmdw = Get-Command pythonw.exe -ErrorAction SilentlyContinue
  if ($cmdw) { $exe = $cmdw.Path }
} catch {}

# Run orchestrator
$cfgPath = Join-Path $repoRoot 'config\config.json'
Write-Host "Orquestando replicación bidireccional con: $cfgPath" -ForegroundColor Cyan
& $exe "$repoRoot\scripts\setup_bidirectional_replication.py" "$cfgPath"