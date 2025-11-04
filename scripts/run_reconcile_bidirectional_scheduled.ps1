# Wrapper para ejecutar reconciliación bidireccional (Local→Remoto y Remoto→Local)
# Uso: programado mediante Task Scheduler o manual.

$ErrorActionPreference = 'Stop'

# Raíz del repo y carpeta de logs
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$logsDir = Join-Path $repoRoot 'backups'
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}
$logFile = Join-Path $logsDir 'reconcile_bidirectional_once.log'
$statusFile = Join-Path $logsDir 'job_status.jsonl'

# Rotación simple si el log supera ~10MB
try {
  if (Test-Path $logFile) {
    $sizeMB = [math]::Round(((Get-Item $logFile).Length / 1MB),2)
    if ($sizeMB -gt 10) { Move-Item -Force $logFile ($logFile + '.1') }
  }
  # Retención: mantener sólo el archivo rotado .1 y eliminar otros
  try {
    $baseName = Split-Path $logFile -Leaf
    $rotated = Get-ChildItem -Path $logsDir -Filter "$baseName.*" -ErrorAction SilentlyContinue
    foreach ($f in $rotated) { if ($f.Name -ne "$baseName.1") { Remove-Item -Force $f.FullName } }
  } catch {}
} catch {}

# Anti-reentradas: si ya están corriendo las reconciliaciones individuales, salir
$thresholdMinutes = 120
try {
  $existing = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'reconcile_local_remote_once\.py' -or $_.CommandLine -match 'reconcile_remote_to_local_once\.py' }
} catch {
  $existing = $null
}
if ($existing) {
  $pids = ($existing | Select-Object -ExpandProperty ProcessId) -join ','
  Add-Content -Path $logFile -Value "[SKIP] Ya en ejecución (PID: $pids). Se omite bidireccional."
  try {
    $json = @{ ts = (Get-Date).ToString('s'); job = 'reconcile_bidirectional'; event = 'skipped'; reason = "running:$pids" } | ConvertTo-Json -Compress
    Add-Content -Path $statusFile -Value $json
  } catch {}
  exit 0
}

# Gate si se ejecutó hace poco
if (Test-Path $logFile) {
  $lastWrite = (Get-Item $logFile).LastWriteTime
  $age = (New-TimeSpan -Start $lastWrite -End (Get-Date)).TotalMinutes
  if ($age -lt $thresholdMinutes) {
    $ageRounded = [math]::Round($age, 1)
    Add-Content -Path $logFile -Value "[SKIP] Gate activo: última ejecución hace $ageRounded min (< $thresholdMinutes)."
    try {
      $json = @{ ts = (Get-Date).ToString('s'); job = 'reconcile_bidirectional'; event = 'skipped'; reason = "gate:$ageRounded" } | ConvertTo-Json -Compress
      Add-Content -Path $statusFile -Value $json
    } catch {}
    exit 0
  }
}

# Chequeo de conectividad remota (si hay config)
try {
  $cfgPath = Join-Path $repoRoot 'config\config.json'
  if (Test-Path $cfgPath) {
    $cfg = Get-Content -Path $cfgPath -Raw | ConvertFrom-Json
    $remote = $cfg.db_remote
    if ($remote -and $remote.host -and $remote.port) {
      $ok = Test-NetConnection -ComputerName $remote.host -Port [int]$remote.port -InformationLevel Quiet
      if (-not $ok) {
        $reason = "remote_unreachable:$($remote.host):$($remote.port)"
        Add-Content -Path $logFile -Value "[SKIP] Conectividad remota fallida ($reason)."
        try {
          $json = @{ ts = (Get-Date).ToString('s'); job = 'reconcile_bidirectional'; event = 'skipped'; reason = $reason } | ConvertTo-Json -Compress
          Add-Content -Path $statusFile -Value $json
        } catch {}
        exit 0
      }
    }
  }
} catch {}

# Resolver Python
$pythonCandidates = @(
    'C:\\Python313\\python.exe',
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
try { $cmdw = Get-Command pythonw.exe -ErrorAction SilentlyContinue; if ($cmdw) { $pythonw = $cmdw.Path } } catch {}
if (-not $pythonw) {
    $pythonwCandidates = @(
        "$env:LocalAppData\Programs\Python\Python313\pythonw.exe",
        "$env:LocalAppData\Programs\Python\Python312\pythonw.exe",
        "$env:LocalAppData\Programs\Python\Python311\pythonw.exe",
        "$env:ProgramFiles\Python313\pythonw.exe",
        "$env:ProgramFiles\Python312\pythonw.exe",
        "$env:ProgramFiles\Python311\pythonw.exe",
        'C:\\Python313\\pythonw.exe',
        'C:\\Python312\\pythonw.exe',
        'C:\\Python311\\pythonw.exe'
    )
    foreach ($pw in $pythonwCandidates) { if (Test-Path $pw) { $pythonw = $pw; break } }
}
if (-not $pythonw -and $python -ne 'python') {
    $guess = Join-Path (Split-Path $python -Parent) 'pythonw.exe'
    if (Test-Path $guess) { $pythonw = $guess }
}
$exe = if ($pythonw) { $pythonw } else { $python }

# Ejecutar pasos ocultos, redirigiendo a logs
$env:PYTHONUNBUFFERED = '1'
$env:PYTHONIOENCODING = 'utf-8'
Set-Location $repoRoot
Add-Content -Path $logFile -Value "[INFO] Iniciando reconciliación bidireccional con $exe..."

# Paso 1: Local→Remoto
$tempOut1 = Join-Path $logsDir 'reconcile_bidirectional_l2r.out.tmp'
$tempErr1 = Join-Path $logsDir 'reconcile_bidirectional_l2r.err.tmp'
if (Test-Path $tempOut1) { Remove-Item $tempOut1 -Force }
if (Test-Path $tempErr1) { Remove-Item $tempErr1 -Force }

Add-Content -Path $logFile -Value "[INFO] Ejecutando scripts\\reconcile_local_remote_once.py..."
$proc1 = Start-Process -FilePath $exe -ArgumentList @('scripts\\reconcile_local_remote_once.py') -WorkingDirectory $repoRoot -WindowStyle Hidden -RedirectStandardOutput $tempOut1 -RedirectStandardError $tempErr1 -Wait -PassThru

$stdoutText1 = ''
$stderrText1 = ''
if (Test-Path $tempOut1) { $stdoutText1 = Get-Content $tempOut1 -Raw; Get-Content $tempOut1 | Add-Content -Path $logFile; Remove-Item $tempOut1 -Force }
if (Test-Path $tempErr1) { $stderrText1 = Get-Content $tempErr1 -Raw; Get-Content $tempErr1 | Add-Content -Path $logFile; Remove-Item $tempErr1 -Force }
Add-Content -Path $logFile -Value "[INFO] Finalizado L2R ExitCode=$($proc1.ExitCode)"
try {
  $json = @{ ts = (Get-Date).ToString('s'); job = 'reconcile_bidirectional'; event = 'step_finished'; step = 'local_to_remote'; exitCode = $proc1.ExitCode } | ConvertTo-Json -Compress
  Add-Content -Path $statusFile -Value $json
} catch {}

# Paso 2: Remoto→Local
$tempOut2 = Join-Path $logsDir 'reconcile_bidirectional_r2l.out.tmp'
$tempErr2 = Join-Path $logsDir 'reconcile_bidirectional_r2l.err.tmp'
if (Test-Path $tempOut2) { Remove-Item $tempOut2 -Force }
if (Test-Path $tempErr2) { Remove-Item $tempErr2 -Force }

Add-Content -Path $logFile -Value "[INFO] Ejecutando scripts\\reconcile_remote_to_local_once.py..."
$proc2 = Start-Process -FilePath $exe -ArgumentList @('scripts\\reconcile_remote_to_local_once.py') -WorkingDirectory $repoRoot -WindowStyle Hidden -RedirectStandardOutput $tempOut2 -RedirectStandardError $tempErr2 -Wait -PassThru

$stdoutText2 = ''
$stderrText2 = ''
if (Test-Path $tempOut2) { $stdoutText2 = Get-Content $tempOut2 -Raw; Get-Content $tempOut2 | Add-Content -Path $logFile; Remove-Item $tempOut2 -Force }
if (Test-Path $tempErr2) { $stderrText2 = Get-Content $tempErr2 -Raw; Get-Content $tempErr2 | Add-Content -Path $logFile; Remove-Item $tempErr2 -Force }
Add-Content -Path $logFile -Value "[INFO] Finalizado R2L ExitCode=$($proc2.ExitCode)"
try {
  $json = @{ ts = (Get-Date).ToString('s'); job = 'reconcile_bidirectional'; event = 'step_finished'; step = 'remote_to_local'; exitCode = $proc2.ExitCode } | ConvertTo-Json -Compress
  Add-Content -Path $statusFile -Value $json
} catch {}

# Resultado global
$overallExit = 0
if ($proc1.ExitCode -ne 0 -or $proc2.ExitCode -ne 0) { $overallExit = 1 }
Add-Content -Path $logFile -Value "[INFO] Bidireccional finalizado. L2R=$($proc1.ExitCode) R2L=$($proc2.ExitCode) OverallExit=$overallExit"
try {
  $eventName = if ($overallExit -eq 0) { 'finished' } else { 'finished_error' }
  $json = @{ ts = (Get-Date).ToString('s'); job = 'reconcile_bidirectional'; event = $eventName; exitCode = $overallExit } | ConvertTo-Json -Compress
  Add-Content -Path $statusFile -Value $json
} catch {}

exit $overallExit