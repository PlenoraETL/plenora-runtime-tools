[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("Start", "Stop", "Restart", "Status", "Logs", "Cleanup")]
    [string] $Action,

    [ValidatePattern("^[a-f0-9]{32}$")]
    [string] $RunId,

    [ValidateRange(1, 120)]
    [int] $HealthTimeoutSeconds = 30,

    [ValidateRange(1, 60)]
    [int] $StopTimeoutSeconds = 5,

    [ValidateRange(1, 10000)]
    [int] $LogTail = 500
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$NatsImage = "nats:2.14.5-alpine3.22@sha256:d4ac35882ac65aff236cd65b9d3fa4d24332c681e1a85f94eedccd3cdd65b1da"
$OwnerLabel = "io.plenora.runtime-tools.test-run"
$ContainerPrefix = "plenora-runtime-tools-nats-"
$DockerExecutable = (Get-Command docker -ErrorAction Stop).Source

if ([string]::IsNullOrWhiteSpace($RunId)) {
    if ($Action -ne "Start") {
        throw "RunId is required for every action except Start."
    }
    $RunId = [guid]::NewGuid().ToString("N")
}

$ContainerName = "$ContainerPrefix$RunId"

function Invoke-Docker {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    $PreviousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = @(
            & $DockerExecutable @Arguments 2>&1 | ForEach-Object { $_.ToString() }
        )
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    if ($ExitCode -ne 0) {
        $Details = $Output -join [Environment]::NewLine
        throw "Docker command failed with exit code $ExitCode. $Details"
    }
    return $Output
}

function Get-ContainerInspect {
    $PreviousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        $Output = @(& $DockerExecutable container inspect $ContainerName 2>$null)
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    if ($ExitCode -ne 0) {
        # A missing container is an expected probe result; do not leak Docker's
        # non-zero inspect status into the script process exit code.
        $global:LASTEXITCODE = 0
        return $null
    }

    $Decoded = ($Output -join [Environment]::NewLine) | ConvertFrom-Json
    if ($Decoded -is [array]) {
        return $Decoded[0]
    }
    return $Decoded
}

function Assert-ContainerOwned {
    $Inspect = Get-ContainerInspect
    if ($null -eq $Inspect) {
        throw "Container $ContainerName does not exist."
    }
    if ($null -eq $Inspect.Config.Labels) {
        throw "Container $ContainerName has no ownership labels."
    }

    $OwnerProperty = $Inspect.Config.Labels.PSObject.Properties[$OwnerLabel]
    if ($null -eq $OwnerProperty -or $OwnerProperty.Value -ne $RunId) {
        throw "Container $ContainerName is not owned by test run $RunId."
    }
    return $Inspect
}

function Get-PublishedPort {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Inspect,

        [Parameter(Mandatory = $true)]
        [string] $ContainerPort
    )

    $PortProperty = $Inspect.NetworkSettings.Ports.PSObject.Properties[$ContainerPort]
    if ($null -eq $PortProperty -or $null -eq $PortProperty.Value) {
        throw "Container port $ContainerPort is not published."
    }

    $Bindings = @($PortProperty.Value)
    if ($Bindings.Count -ne 1) {
        throw "Container port $ContainerPort does not have exactly one host binding."
    }

    $HostIp = [string] $Bindings[0].HostIp
    if ($HostIp -ne "127.0.0.1") {
        throw "Container port $ContainerPort is not restricted to the loopback interface."
    }

    $HostPort = 0
    if (-not [int]::TryParse([string] $Bindings[0].HostPort, [ref] $HostPort)) {
        throw "Container port $ContainerPort has an invalid host port."
    }
    if ($HostPort -lt 1 -or $HostPort -gt 65535) {
        throw "Container port $ContainerPort is outside the valid TCP port range."
    }
    return $HostPort
}

function Wait-NatsHealthy {
    param(
        [Parameter(Mandatory = $true)]
        [int] $MonitorPort
    )

    $HealthUri = "http://127.0.0.1:$MonitorPort/healthz?js-enabled-only=true"
    $Deadline = [DateTime]::UtcNow.AddSeconds($HealthTimeoutSeconds)
    $RequestParameters = @{
        Uri = $HealthUri
        Method = "Get"
        TimeoutSec = 1
        UseBasicParsing = $true
        ErrorAction = "Stop"
    }

    while ([DateTime]::UtcNow -lt $Deadline) {
        try {
            $Response = Invoke-WebRequest @RequestParameters
            if ($Response.StatusCode -eq 200) {
                return
            }
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    }

    throw "NATS JetStream did not become healthy within $HealthTimeoutSeconds seconds."
}

function Get-StatusPayload {
    $Inspect = Assert-ContainerOwned
    $Running = [bool] $Inspect.State.Running
    $ClientUrl = $null
    $MonitorUrl = $null
    if ($Running) {
        $ClientPort = Get-PublishedPort -Inspect $Inspect -ContainerPort "4222/tcp"
        $MonitorPort = Get-PublishedPort -Inspect $Inspect -ContainerPort "8222/tcp"
        $ClientUrl = "nats://127.0.0.1:$ClientPort"
        $MonitorUrl = "http://127.0.0.1:$MonitorPort"
    }

    return [ordered]@{
        run_id = $RunId
        container = $ContainerName
        image = $NatsImage
        running = $Running
        client_url = $ClientUrl
        monitor_url = $MonitorUrl
        plaintext_test_transport = $true
    }
}

function Write-StatusPayload {
    Get-StatusPayload | ConvertTo-Json -Compress | Write-Output
}

function Remove-OwnedContainer {
    $Inspect = Get-ContainerInspect
    if ($null -eq $Inspect) {
        return
    }

    $null = Assert-ContainerOwned
    $null = Invoke-Docker -Arguments @("container", "rm", "--force", $ContainerName)
    if ($null -ne (Get-ContainerInspect)) {
        throw "Container $ContainerName still exists after cleanup."
    }
}

switch ($Action) {
    "Start" {
        $Existing = Get-ContainerInspect
        if ($null -ne $Existing) {
            $null = Assert-ContainerOwned
            throw "Owned container $ContainerName already exists; use Restart or Cleanup."
        }

        $PreviousErrorActionPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = "SilentlyContinue"
            $ImageInspect = @(& $DockerExecutable image inspect $NatsImage 2>$null)
            $ImageExitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $PreviousErrorActionPreference
        }
        if ($ImageExitCode -ne 0) {
            $null = Invoke-Docker -Arguments @("pull", $NatsImage)
        }

        $Created = $false
        try {
            $null = Invoke-Docker -Arguments @(
                "run",
                "--detach",
                "--pull", "never",
                "--name", $ContainerName,
                "--label", "$OwnerLabel=$RunId",
                "--publish", "127.0.0.1::4222",
                "--publish", "127.0.0.1::8222",
                "--stop-timeout", [string] $StopTimeoutSeconds,
                $NatsImage,
                "-js",
                "-sd", "/data",
                "-m", "8222"
            )
            $Created = $true

            $Inspect = Assert-ContainerOwned
            $MonitorPort = Get-PublishedPort -Inspect $Inspect -ContainerPort "8222/tcp"
            Wait-NatsHealthy -MonitorPort $MonitorPort
            Write-StatusPayload
        }
        catch {
            if ($Created) {
                $PreviousErrorActionPreference = $ErrorActionPreference
                try {
                    $ErrorActionPreference = "Continue"
                    $Logs = @(
                        & $DockerExecutable logs --timestamps --tail $LogTail $ContainerName 2>&1 |
                            ForEach-Object { $_.ToString() }
                    )
                }
                finally {
                    $ErrorActionPreference = $PreviousErrorActionPreference
                }
                if ($Logs.Count -gt 0) {
                    Write-Warning ($Logs -join [Environment]::NewLine)
                }
                Remove-OwnedContainer
            }
            throw
        }
    }
    "Stop" {
        $Inspect = Assert-ContainerOwned
        if ([bool] $Inspect.State.Running) {
            $null = Invoke-Docker -Arguments @(
                "container",
                "stop",
                "--time", [string] $StopTimeoutSeconds,
                $ContainerName
            )
        }

        $Verified = Assert-ContainerOwned
        if ([bool] $Verified.State.Running) {
            throw "Container $ContainerName is still running after Stop."
        }
        Write-StatusPayload
    }
    "Restart" {
        $null = Assert-ContainerOwned
        $null = Invoke-Docker -Arguments @(
            "container",
            "restart",
            "--time", [string] $StopTimeoutSeconds,
            $ContainerName
        )

        $Inspect = Assert-ContainerOwned
        $MonitorPort = Get-PublishedPort -Inspect $Inspect -ContainerPort "8222/tcp"
        Wait-NatsHealthy -MonitorPort $MonitorPort
        Write-StatusPayload
    }
    "Status" {
        Write-StatusPayload
    }
    "Logs" {
        $null = Assert-ContainerOwned
        $Logs = Invoke-Docker -Arguments @(
            "container",
            "logs",
            "--timestamps",
            "--tail", [string] $LogTail,
            $ContainerName
        )
        $Logs | Write-Output
    }
    "Cleanup" {
        Remove-OwnedContainer
        [ordered]@{
            run_id = $RunId
            container = $ContainerName
            removed = $true
        } | ConvertTo-Json -Compress | Write-Output
    }
}
