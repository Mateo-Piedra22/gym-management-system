# Wrapper para ejecutar limpieza de retención y registrar logs
# Uso: programado mediante Task Scheduler o manual.

$ErrorActionPreference = 'Stop'

# Raíz del repo y carpeta de logs
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$logsDir = Join-Path $repoRoot 'backups'
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}
$logFile = Join-Path $logsDir 'cleanup_retention.log'

# Resolver Python
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
foreach ($p in $pythonCandidates) {
    if ($p -and (Test-Path $p)) { $python = $p; break }
}
if (-not $python) { $python = 'python' }

# Preferir pythonw.exe si disponible para ejecución sin consola
$pythonw = $null
try {
    $cmdw = Get-Command pythonw.exe -ErrorAction SilentlyContinue
    if ($cmdw) { $pythonw = $cmdw.Path }
} catch {}
if (-not $pythonw -and $python -ne 'python') {
    $guess = Join-Path (Split-Path $python -Parent) 'pythonw.exe'
    if (Test-Path $guess) { $pythonw = $guess }
}
$exe = if ($pythonw) { $pythonw } else { $python }

# Ejecutar script oculto, esperar y redirigir a logs
$env:PYTHONUNBUFFERED = '1'
Set-Location $repoRoot
Add-Content -Path $logFile -Value "[INFO] Ejecutando scripts\\cleanup_data_retention.py con $exe..."

$tempOut = Join-Path $logsDir 'cleanup_retention.out.tmp'
$tempErr = Join-Path $logsDir 'cleanup_retention.err.tmp'
if (Test-Path $tempOut) { Remove-Item $tempOut -Force }
if (Test-Path $tempErr) { Remove-Item $tempErr -Force }

$proc = Start-Process -FilePath $exe -ArgumentList @('scripts\\cleanup_data_retention.py') -WorkingDirectory $repoRoot -WindowStyle Hidden -RedirectStandardOutput $tempOut -RedirectStandardError $tempErr -Wait -PassThru

if (Test-Path $tempOut) { Get-Content $tempOut | Add-Content -Path $logFile; Remove-Item $tempOut -Force }
if (Test-Path $tempErr) { Get-Content $tempErr | Add-Content -Path $logFile; Remove-Item $tempErr -Force }

Add-Content -Path $logFile -Value "[INFO] Finalizado scripts\\cleanup_data_retention.py ExitCode=$($proc.ExitCode)"
exit $proc.ExitCode