param(
    [Parameter(Mandatory = $true)]
    [string]$Destino,
    [int]$TimeoutMs = 1500
)

$ErrorActionPreference = 'Stop'
$pingExe = Join-Path $env:SystemRoot 'System32\ping.exe'

function Test-Payload([int]$PayloadBytes) {
    $output = (& $pingExe -4 -f -n 1 -w $TimeoutMs -l $PayloadBytes $Destino 2>&1 | Out-String)
    $exitCode = $LASTEXITCODE
    if ($exitCode -eq 0) {
        return $true
    }
    if ($output -match '(?i)fragment') {
        return $false
    }
    throw "Sonda inconclusiva para $Destino com payload $PayloadBytes bytes. O destino precisa responder ICMP e a rede precisa retornar a mensagem de fragmentação. Saída: $output"
}

# Primeiro confirma que o destino responde; sem isso, timeout não pode ser
# distinguido de um pacote descartado por exceder a MTU do caminho.
if (-not (Test-Payload 32)) {
    throw "O destino $Destino não aceitou a sonda mínima."
}

$low = 32
$high = 65500
while ($low + 1 -lt $high) {
    $mid = [int][Math]::Floor(($low + $high) / 2)
    if (Test-Payload $mid) {
        $low = $mid
    } else {
        $high = $mid
    }
}

$mtu = $low + 28 # cabeçalho IPv4 e cabeçalho ICMP; também equivale ao overhead IPv4/UDP.
[pscustomobject]@{
    Destination = $Destino
    AddressFamily = 'IPv4'
    LargestICMPPayloadBytes = $low
    PathMTUBytes = $mtu
    LargestUDPPayloadBytesWithoutFragmentation = $mtu - 28
} | Format-List
