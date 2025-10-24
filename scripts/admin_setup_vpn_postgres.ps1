# Admin setup for WireGuard VPN + PostgreSQL network access + quick verification
# Run this script with elevation; it will self-elevate if needed.

$ErrorActionPreference = 'Stop'

function Ensure-Elevation {
    $isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if (-not $isAdmin) {
        Write-Host 'Elevating to Administrator...' -ForegroundColor Yellow
        Start-Process -FilePath 'powershell' -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
        exit 0
    }
}

Ensure-Elevation

# Resolve repo root and logs directory
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$logsDir = Join-Path $repoRoot 'backups'
if (-not (Test-Path $logsDir)) { New-Item -ItemType Directory -Path $logsDir | Out-Null }
$logFile = Join-Path $logsDir 'admin_setup_vpn_postgres.log'

try {
    Start-Transcript -Path $logFile -Append -ErrorAction SilentlyContinue
} catch {}

# Resolve python
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

if (-not $python) { $python = 'python' }

Write-Host "Repo root: $repoRoot" -ForegroundColor Cyan
Write-Host "Python: $python" -ForegroundColor Cyan

# Optional: run WireGuard client setup script only if we have server data or no placeholders
$wgSetup = Join-Path (Join-Path $repoRoot 'scripts') 'setup_wireguard_client.ps1'
$confPath = Join-Path (Join-Path $repoRoot 'config') 'vpn\\gymms.conf'
$wgPub = $env:WG_SERVER_PUBLIC
$wgEp = $env:WG_ENDPOINT
$needsPlaceholders = $false
if (Test-Path $confPath) {
    try {
        $content = Get-Content -Raw -Path $confPath
        if ($content -match '<SERVER_PUBLIC_KEY>' -or $content -match '<SERVER_ENDPOINT>') { $needsPlaceholders = $true }
    } catch {}
}
$canRunWG = ($wgPub -and $wgEp) -or (-not $needsPlaceholders)
if ((Test-Path $wgSetup) -and $canRunWG) {
    Write-Host 'Running setup_wireguard_client.ps1 (Admin)...' -ForegroundColor Yellow
    try {
        & powershell -NoProfile -ExecutionPolicy Bypass -File $wgSetup
    } catch {
        Write-Warning "WireGuard setup script failed: $($_.Exception.Message)"
    }
} else {
    Write-Host 'Skipping WireGuard (no server data). Usando acceso remoto sin VPN.' -ForegroundColor Yellow
}

# Step 1: Ensure VPN connectivity (auto provider from config)
Write-Host 'Ensuring VPN connectivity (auto)...' -ForegroundColor Yellow
$code1 = "import json; import utils_modules.prerequisites as p; from utils_modules.vpn_setup import ensure_vpn_connectivity; cfg = p._load_cfg(); print(json.dumps(ensure_vpn_connectivity(cfg, 'auto'), ensure_ascii=False))"
$vpnResJson = & $python -c $code1
Write-Host $vpnResJson
$vpnRes = $null
try { $vpnRes = $vpnResJson | ConvertFrom-Json } catch {}

# Step 2: Ensure PostgreSQL network access (listen_addresses, pg_hba, firewall)
Write-Host 'Ensuring PostgreSQL network access...' -ForegroundColor Yellow
$code2 = "import json; import utils_modules.prerequisites as p; cfg = p._load_cfg(); print(json.dumps(p.ensure_postgres_network_access(cfg), ensure_ascii=False))"
$netResJson = & $python -c $code2
Write-Host $netResJson

# Step 3: Verify replication health
Write-Host 'Verifying replication health...' -ForegroundColor Yellow
$verifyScript = Join-Path (Join-Path $repoRoot 'scripts') 'verify_replication_health.py'
if (Test-Path $verifyScript) {
    & $python $verifyScript
}

try { Stop-Transcript } catch {}

Write-Host "Done. Log saved to: $logFile" -ForegroundColor Green