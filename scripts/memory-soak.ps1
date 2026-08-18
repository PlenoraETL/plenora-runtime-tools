[CmdletBinding()]
param(
    [ValidateRange(4, 10000)]
    [int] $Iterations = 40,

    [ValidateRange(0, 1000)]
    [int] $WarmupIterations = 5,

    [ValidateRange(1, 2048)]
    [int] $AllocationMiB = 64,

    [ValidateRange(1, 4096)]
    [int] $MaxInFlight = 4,

    [ValidateRange(1, 60000)]
    [int] $SettleMilliseconds = 25,

    [ValidateRange(1, 2048)]
    [int] $GrowthLimitMiB = 16,

    [ValidatePattern("^[1-9][0-9]*[mMgG]$")]
    [string] $ContainerMemory = "1g",

    [string] $OutputDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$DockerExecutable = (Get-Command docker -ErrorAction Stop).Source
$WorkspacePath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$WorkspaceMount = "type=bind,source=$WorkspacePath,target=/workspace"
$RegistryVolume = "runtime-tools-cargo-registry"
$TargetVolume = "runtime-tools-target-rust-1-97-1-bookworm"
$BuildImage = "rust:1.97.1-bookworm"
$RunImage = "debian:bookworm-slim"
$ProbeBinary = "/workspace/target/release/plenora-memory-probe"

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $Timestamp = [DateTimeOffset]::UtcNow.ToString("yyyyMMddTHHmmssZ")
    $OutputDirectory = Join-Path $WorkspacePath "target/memory-soak/$Timestamp"
}
elseif (-not [IO.Path]::IsPathRooted($OutputDirectory)) {
    $OutputDirectory = Join-Path $WorkspacePath $OutputDirectory
}

$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
$TargetRoot = [IO.Path]::GetFullPath((Join-Path $WorkspacePath "target"))
$TargetPrefix = $TargetRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
if (-not $OutputDirectory.StartsWith($TargetPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "OutputDirectory must remain below the workspace target directory."
}

$MemoryUnit = $ContainerMemory.Substring($ContainerMemory.Length - 1).ToLowerInvariant()
$MemoryValue = [int64]::Parse($ContainerMemory.Substring(0, $ContainerMemory.Length - 1))
$ContainerMemoryMiB = if ($MemoryUnit -eq "g") { $MemoryValue * 1024 } else { $MemoryValue }
$ConcurrentWorkingSetMiB = [int64] $AllocationMiB * [int64] $MaxInFlight
if ($ConcurrentWorkingSetMiB -gt [math]::Floor($ContainerMemoryMiB * 0.70)) {
    throw "AllocationMiB * MaxInFlight must stay within 70% of ContainerMemory."
}

New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    $PreviousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = @(& $DockerExecutable @Arguments)
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    return [pscustomobject]@{
        Output = @($Output | ForEach-Object { $_.ToString() })
        ExitCode = $ExitCode
    }
}

function Write-Utf8Lines {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [string[]] $Lines
    )

    [IO.File]::WriteAllLines($Path, $Lines, [Text.UTF8Encoding]::new($false))
}

function Invoke-Probe {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("plateau", "fragmentation", "concurrent", "error", "cancellation", "leak-control")]
        [string] $Scenario,

        [Parameter(Mandatory = $true)]
        [int] $ScenarioIterations,

        [Parameter(Mandatory = $true)]
        [int] $ScenarioWarmupIterations,

        [Parameter(Mandatory = $true)]
        [int] $ScenarioAllocationMiB,

        [Parameter(Mandatory = $true)]
        [int] $ScenarioGrowthLimitMiB,

        [Parameter(Mandatory = $true)]
        [int] $ExpectedExitCode
    )

    $Arguments = @(
        "run", "--rm",
        "--network", "none",
        "--read-only",
        "--pids-limit", "128",
        "--memory", $ContainerMemory,
        "--memory-swap", $ContainerMemory,
        "--mount", "type=volume,source=$TargetVolume,target=/workspace/target,readonly",
        "--env", "PLENORA_MEMORY_SCENARIO=$Scenario",
        "--env", "PLENORA_MEMORY_ITERATIONS=$ScenarioIterations",
        "--env", "PLENORA_MEMORY_WARMUP_ITERATIONS=$ScenarioWarmupIterations",
        "--env", "PLENORA_MEMORY_ALLOCATION_MIB=$ScenarioAllocationMiB",
        "--env", "PLENORA_MEMORY_MAX_IN_FLIGHT=$MaxInFlight",
        "--env", "PLENORA_MEMORY_SETTLE_MILLIS=$SettleMilliseconds",
        "--env", "PLENORA_MEMORY_GROWTH_LIMIT_MIB=$ScenarioGrowthLimitMiB",
        $RunImage,
        $ProbeBinary
    )
    $Result = Invoke-Native -Arguments $Arguments
    $OutputPath = Join-Path $OutputDirectory "$Scenario.csv"
    Write-Utf8Lines -Path $OutputPath -Lines $Result.Output
    if ($Result.ExitCode -ne $ExpectedExitCode) {
        throw "Memory scenario '$Scenario' exited $($Result.ExitCode), expected $ExpectedExitCode. See $OutputPath."
    }
    return $OutputPath
}

$Build = Invoke-Native -Arguments @(
    "run", "--rm",
    "--mount", $WorkspaceMount,
    "--mount", "type=volume,source=$RegistryVolume,target=/usr/local/cargo/registry",
    "--mount", "type=volume,source=$TargetVolume,target=/workspace/target",
    "--workdir", "/workspace",
    $BuildImage,
    "bash", "-c",
    "apt-get update -qq && apt-get install -y -qq build-essential >/dev/null && /usr/local/cargo/bin/cargo build -p plenora-runtime-memory-tests --release --locked"
)
if ($Build.ExitCode -ne 0) {
    throw "Memory probe build failed with exit code $($Build.ExitCode)."
}

$Outputs = @()
foreach ($Scenario in @("plateau", "fragmentation", "concurrent", "error", "cancellation")) {
    $Outputs += Invoke-Probe `
        -Scenario $Scenario `
        -ScenarioIterations $Iterations `
        -ScenarioWarmupIterations $WarmupIterations `
        -ScenarioAllocationMiB $AllocationMiB `
        -ScenarioGrowthLimitMiB $GrowthLimitMiB `
        -ExpectedExitCode 0
}

$LeakIterations = [math]::Max(8, [math]::Min(16, $Iterations))
$LeakAllocationMiB = [math]::Min(8, $AllocationMiB)
$LeakGrowthLimitMiB = [math]::Max(
    1,
    [math]::Min($GrowthLimitMiB, [math]::Floor(($LeakAllocationMiB * $LeakIterations) / 4))
)
$Outputs += Invoke-Probe `
    -Scenario "leak-control" `
    -ScenarioIterations $LeakIterations `
    -ScenarioWarmupIterations 2 `
    -ScenarioAllocationMiB $LeakAllocationMiB `
    -ScenarioGrowthLimitMiB $LeakGrowthLimitMiB `
    -ExpectedExitCode 2

$RunMetadata = [ordered]@{
    completed_at_utc = [DateTimeOffset]::UtcNow.ToString("O")
    rust_image = $BuildImage
    runtime_image = $RunImage
    container_memory = $ContainerMemory
    iterations = $Iterations
    warmup_iterations = $WarmupIterations
    allocation_mib = $AllocationMiB
    max_in_flight = $MaxInFlight
    settle_milliseconds = $SettleMilliseconds
    growth_limit_mib = $GrowthLimitMiB
    leak_control_growth_limit_mib = $LeakGrowthLimitMiB
    leak_control_expected_exit = 2
    outputs = @($Outputs | ForEach-Object { [IO.Path]::GetFileName($_) })
}
$MetadataPath = Join-Path $OutputDirectory "run.json"
[IO.File]::WriteAllText(
    $MetadataPath,
    ($RunMetadata | ConvertTo-Json -Depth 3),
    [Text.UTF8Encoding]::new($false)
)

[pscustomobject]@{
    status = "passed"
    output_directory = $OutputDirectory
    scenarios = $Outputs.Count
    leak_detector_validated = $true
} | ConvertTo-Json -Compress
