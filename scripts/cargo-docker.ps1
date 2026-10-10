param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $CargoArguments
)

$ErrorActionPreference = "Stop"

if ($CargoArguments.Count -eq 0) {
    $CargoArguments = @("check", "--workspace")
}

$WorkspacePath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Mount = "type=bind,source=$WorkspacePath,target=/workspace"

# Content-pinned by multi-architecture index digest, matching nats-docker.ps1, so the same commit
# always validates against the same toolchain files.
$ToolchainImage = "rust:1.98.1-slim-bookworm@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730"

if ($CargoArguments[0] -eq "clippy") {
    docker run --rm --env "RUSTFLAGS=-D warnings" --mount $Mount -w /workspace $ToolchainImage cargo @CargoArguments
}
else {
    docker run --rm --mount $Mount -w /workspace $ToolchainImage cargo @CargoArguments
}

exit $LASTEXITCODE
