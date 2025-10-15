# PowerShell: aplica los scripts SQL de SymmetricDS usando psql
# Requisitos: tener psql en PATH (PostgreSQL client)

param(
  [string]$RailwayHost = "shuttle.proxy.rlwy.net",
  [int]$RailwayPort = 5432,
  [string]$RailwayDb = "railway",
  [string]$RailwayUser = "postgres",
  [string]$RailwayPassword = $env:RAILWAY_PG_PASSWORD,

  [string]$LocalHost = "localhost",
  [int]$LocalPort = 5432,
  [string]$LocalDb = "gimnasio",
  [string]$LocalUser = "postgres",
  [string]$LocalPassword = $env:LOCAL_PG_PASSWORD
)

function Require-Tool($name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    Write-Error "No se encontró '$name' en PATH. Instala el cliente de PostgreSQL (psql).";
    exit 1;
  }
}

Require-Tool psql

$scriptsDir = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "."
$railwaySql = Join-Path $scriptsDir "railway_setup.sql"
$localSql = Join-Path $scriptsDir "local_setup.sql"
$initialLoadSql = Join-Path $scriptsDir "initial_load_all_clients.sql"

Write-Host "Aplicando configuración en Railway…"
if (-not $RailwayPassword) {
  $RailwayPassword = Read-Host -AsSecureString "Password Railway (postgres)" | \
    ForEach-Object { [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($_)) }
}

$env:PGPASSWORD = $RailwayPassword
psql -h $RailwayHost -p $RailwayPort -U $RailwayUser -d $RailwayDb -v ON_ERROR_STOP=1 -f $railwaySql
if ($LASTEXITCODE -ne 0) { Write-Error "Falló la configuración en Railway"; exit 1 }

Write-Host "Aplicando configuración en Local…"
if (-not $LocalPassword) {
  $LocalPassword = Read-Host -AsSecureString "Password Local (postgres)" | \
    ForEach-Object { [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($_)) }
}

$env:PGPASSWORD = $LocalPassword
psql -h $LocalHost -p $LocalPort -U $LocalUser -d $LocalDb -v ON_ERROR_STOP=1 -f $localSql
if ($LASTEXITCODE -ne 0) { Write-Error "Falló la configuración en Local"; exit 1 }

Write-Host "Solicitando carga inicial para todos los clientes (en Railway)…"
$env:PGPASSWORD = $RailwayPassword
psql -h $RailwayHost -p $RailwayPort -U $RailwayUser -d $RailwayDb -v ON_ERROR_STOP=1 -f $initialLoadSql
if ($LASTEXITCODE -ne 0) { Write-Warning "La carga inicial opcional tuvo errores; revisa si ya existen solicitudes previas." }

Write-Host "Listo. Configuración aplicada."