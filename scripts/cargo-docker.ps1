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
$ToolchainImage = "rust:1.97.1-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3"

if ($CargoArguments[0] -eq "clippy") {
    docker run --rm --env "RUSTFLAGS=-D warnings" --mount $Mount -w /workspace $ToolchainImage cargo @CargoArguments
}
else {
    docker run --rm --mount $Mount -w /workspace $ToolchainImage cargo @CargoArguments
}

exit $LASTEXITCODE
