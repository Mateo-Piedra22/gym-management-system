# Wrapper para ejecutar el uploader de sincronización manualmente y registrar logs
# Uso directo: clic derecho > Run with PowerShell, o llamado desde Task Scheduler

$ErrorActionPreference = 'Stop'

# Raíz del repo y preparación de carpeta de logs
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$logsDir = Join-Path $repoRoot 'backups'
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}
$logFile = Join-Path $logsDir 'sync_uploader.log'
$statusFile = Join-Path $logsDir 'job_status.jsonl'

# Rotación simple si el log supera ~10MB
try {
  if (Test-Path $logFile) {
    $sizeMB = [math]::Round(((Get-Item $logFile).Length / 1MB),2)
    if ($sizeMB -gt 10) { Move-Item -Force $logFile ($logFile + '.1') }
  }
  # Retención: mantener sólo .1, eliminar archivos rotados adicionales
  try {
    $base = Split-Path $logFile -Leaf
    $archives = Get-ChildItem -Path $logsDir -Filter "$base.*" -ErrorAction SilentlyContinue
    foreach ($a in $archives) { if ($a.Name -ne "$base.1") { Remove-Item -Force $a.FullName } }
  } catch {}
} catch {}

# Anti-reentradas: si ya está corriendo o si se ejecutó hace poco, salir
$thresholdMinutes = 10
try {
    $existing = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'run_sync_uploader\.py' }
} catch {
    $existing = $null
}
if ($existing) {
    $pids = ($existing | Select-Object -ExpandProperty ProcessId) -join ','
    Add-Content -Path $logFile -Value "[SKIP] Ya en ejecución (PID: $pids). Se omite."
    try {
      $json = @{ ts = (Get-Date).ToString('s'); job = 'sync_uploader'; event = 'skipped'; reason = "running:$pids" } | ConvertTo-Json -Compress
      Add-Content -Path $statusFile -Value $json
    } catch {}
    exit 0
}
if (Test-Path $logFile) {
    $lastWrite = (Get-Item $logFile).LastWriteTime
    $age = (New-TimeSpan -Start $lastWrite -End (Get-Date)).TotalMinutes
    if ($age -lt $thresholdMinutes) {
        $ageRounded = [math]::Round($age, 1)
        Add-Content -Path $logFile -Value "[SKIP] Gate activo: última ejecución hace $ageRounded min (< $thresholdMinutes)."
        try {
          $json = @{ ts = (Get-Date).ToString('s'); job = 'sync_uploader'; event = 'skipped'; reason = "gate:$ageRounded" } | ConvertTo-Json -Compress
          Add-Content -Path $statusFile -Value $json
        } catch {}
        exit 0
    }
}

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
if (-not $pythonw) {
    $pythonwCandidates = @(
        "$env:LocalAppData\Programs\Python\Python313\pythonw.exe",
        "$env:LocalAppData\Programs\Python\Python312\pythonw.exe",
        "$env:LocalAppData\Programs\Python\Python311\pythonw.exe",
        "$env:ProgramFiles\Python313\pythonw.exe",
        "$env:ProgramFiles\Python312\pythonw.exe",
        "$env:ProgramFiles\Python311\pythonw.exe",
        'C:\Python313\pythonw.exe',
        'C:\Python312\pythonw.exe',
        'C:\Python311\pythonw.exe'
    )
    foreach ($pw in $pythonwCandidates) { if (Test-Path $pw) { $pythonw = $pw; break } }
}
if (-not $pythonw -and $python -ne 'python') {
    $guess = Join-Path (Split-Path $python -Parent) 'pythonw.exe'
    if (Test-Path $guess) { $pythonw = $guess }
}
$exe = if ($pythonw) { $pythonw } else { $python }

# Ejecutar uploader oculto, esperar y redirigir a logs
$env:PYTHONUNBUFFERED = '1'
$env:PYTHONIOENCODING = 'utf-8'
Set-Location $repoRoot
Add-Content -Path $logFile -Value "[INFO] Ejecutando run_sync_uploader.py con $exe..."

$tempOut = Join-Path $logsDir 'sync_uploader.out.tmp'
$tempErr = Join-Path $logsDir 'sync_uploader.err.tmp'
if (Test-Path $tempOut) { Remove-Item $tempOut -Force }
if (Test-Path $tempErr) { Remove-Item $tempErr -Force }

$proc = Start-Process -FilePath $exe -ArgumentList @('scripts\\run_sync_uploader.py') -WorkingDirectory $repoRoot -WindowStyle Hidden -RedirectStandardOutput $tempOut -RedirectStandardError $tempErr -Wait -PassThru

$stdoutText = ''
$stderrText = ''
if (Test-Path $tempOut) { $stdoutText = Get-Content $tempOut -Raw; Get-Content $tempOut | Add-Content -Path $logFile; Remove-Item $tempOut -Force }
if (Test-Path $tempErr) { $stderrText = Get-Content $tempErr -Raw; Get-Content $tempErr | Add-Content -Path $logFile; Remove-Item $tempErr -Force }

Add-Content -Path $logFile -Value "[INFO] Finalizado run_sync_uploader.py ExitCode=$($proc.ExitCode)"
if ($proc.ExitCode -ne 0) {
  try {
    $snippet = if ($stderrText) { $stderrText } else { $stdoutText }
    if ($snippet.Length -gt 500) { $snippet = $snippet.Substring($snippet.Length - 500) }
    $json = @{ ts = (Get-Date).ToString('s'); job = 'sync_uploader'; event = 'error'; exitCode = $proc.ExitCode; reason = $snippet } | ConvertTo-Json -Compress
    Add-Content -Path $statusFile -Value $json
  } catch {}
} else {
  try {
    $json = @{ ts = (Get-Date).ToString('s'); job = 'sync_uploader'; event = 'finished'; exitCode = $proc.ExitCode } | ConvertTo-Json -Compress
    Add-Content -Path $statusFile -Value $json
  } catch {}
}
exit $proc.ExitCode