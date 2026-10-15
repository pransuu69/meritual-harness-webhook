# Meritual Harness - PowerShell Test Runner
param(
    [int]$TotalPayloads = 10000,
    [double]$AdversaryRate = 0.30,
    [int]$Concurrency = 50,
    [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
Write-Host ">>> Building server and harness binaries..." -ForegroundColor Cyan

& "$env:GOROOT\bin\go.exe" build -o bin/server.exe ./cmd/server
& "$env:GOROOT\bin\go.exe" build -o bin/harness.exe ./cmd/harness

Write-Host ">>> Launching Ingestion Engine on port $Port..." -ForegroundColor Cyan
$serverProcess = Start-Process -FilePath ".\bin\server.exe" -ArgumentList "-port $Port" -PassThru

try {
    Start-Sleep -Milliseconds 800
    Write-Host ">>> Executing Adversarial Burst Harness..." -ForegroundColor Green
    & ".\bin\harness.exe" -server "http://127.0.0.1:$Port" -total $TotalPayloads -adversary-rate $AdversaryRate -concurrency $Concurrency
}
finally {
    Write-Host ">>> Stopping Ingestion Engine..." -ForegroundColor Yellow
    if ($serverProcess -and -not $serverProcess.HasExited) {
        Stop-Process -Id $serverProcess.Id -Force
    }
}
