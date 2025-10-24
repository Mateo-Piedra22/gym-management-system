#requires -version 5.1
# GymMS WireGuard client setup: install WG, generate keys, write config, start service
param(
  [string]$ConfigPath = "$PSScriptRoot/../config/vpn/gymms.conf",
  [string]$WGServerPublic = $env:WG_SERVER_PUBLIC,
  [string]$WGEndpoint = $env:WG_ENDPOINT
)

$ErrorActionPreference = 'Stop'

function Ensure-WireGuardInstalled {
  Write-Host "Verificando WireGuard..."
  $wgExe = "C:\\Program Files\\WireGuard\\wireguard.exe"
  if (-not (Test-Path $wgExe)) {
    Write-Host "Instalando WireGuard via winget..."
    winget install -e --id WireGuard.WireGuard --silent --accept-package-agreements --accept-source-agreements | Out-Null
  }
  if (-not (Test-Path $wgExe)) { throw "WireGuard no se detecta tras la instalación." }
}

function New-WGKeys {
  $wgBin = "C:\\Program Files\\WireGuard\\wg.exe"
  if (-not (Test-Path $wgBin)) { throw "wg.exe no encontrado. Asegura que WireGuard esté instalado." }
  $priv = & $wgBin genkey
  $pub = $priv | & $wgBin pubkey
  return @{ Private=$priv; Public=$pub }
}

function Write-WGConfig($privKey, $serverPub, $endpoint, $path) {
  $content = @"
[Interface]
PrivateKey = $privKey
Address = 10.10.10.2/32
DNS = 1.1.1.1

[Peer]
PublicKey = $serverPub
AllowedIPs = 10.10.10.0/24
Endpoint = $endpoint
PersistentKeepalive = 25
"@
  $dir = Split-Path -Parent $path
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  Set-Content -Path $path -Value $content -Encoding ASCII
}

function Start-WGService($path) {
  $wgExe = "C:\\Program Files\\WireGuard\\wireguard.exe"
  if (-not (Test-Path $wgExe)) { throw "wireguard.exe no encontrado." }
  & $wgExe /installtunnelservice $path | Out-Null
}

# Main
Ensure-WireGuardInstalled
$keys = New-WGKeys

if (-not $WGServerPublic -or -not $WGEndpoint) {
  Write-Warning "Falta WG_SERVER_PUBLIC y/o WG_ENDPOINT. No se puede conectar sin estos datos."
  Write-Host "Tu clave pública de cliente (compártela con el servidor):" -ForegroundColor Cyan
  Write-Host $keys.Public
  # Guardar clave pública en archivo para facilitar aprovisionamiento del servidor
  try {
    $pubPath = Join-Path $PSScriptRoot '..\\config\\vpn\\client_public.txt'
    New-Item -ItemType Directory -Force -Path (Split-Path $pubPath -Parent) | Out-Null
    Set-Content -Path $pubPath -Value $keys.Public -Encoding ASCII
    Write-Host "Clave pública guardada en: $pubPath" -ForegroundColor Green
  } catch {}
  Write-Host "Configura variables de entorno y re-ejecuta:" -ForegroundColor Yellow
  Write-Host "$Env:WG_SERVER_PUBLIC='<SERVER_PUBLIC>'; $Env:WG_ENDPOINT='<SERVER_IP>:51820'; PowerShell -ExecutionPolicy Bypass -File scripts\\setup_wireguard_client.ps1" -ForegroundColor Yellow
  # Aún escribimos el config con placeholders para que puedas editar
  $serverPub = $WGServerPublic
  if (-not $serverPub) { $serverPub = '<SERVER_PUBLIC_KEY>' }
  $endpoint = $WGEndpoint
  if (-not $endpoint) { $endpoint = '<SERVER_ENDPOINT>:51820' }
  Write-WGConfig -privKey $keys.Private -serverPub $serverPub -endpoint $endpoint -path $ConfigPath
  Write-Host "Config generado en: $ConfigPath"
  return
}

Write-WGConfig -privKey $keys.Private -serverPub $WGServerPublic -endpoint $WGEndpoint -path $ConfigPath
Start-WGService -path $ConfigPath

# Mostrar IP de la interfaz
Get-NetIPConfiguration | Where-Object { $_.InterfaceAlias -like '*WireGuard*' } | Format-List InterfaceAlias,IPv4Address