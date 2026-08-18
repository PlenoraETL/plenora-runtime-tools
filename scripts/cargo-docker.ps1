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

if ($CargoArguments[0] -eq "clippy") {
    docker run --rm --env "RUSTFLAGS=-D warnings" --mount $Mount -w /workspace rust:1.97.1-slim-bookworm cargo @CargoArguments
}
else {
    docker run --rm --mount $Mount -w /workspace rust:1.97.1-slim-bookworm cargo @CargoArguments
}

exit $LASTEXITCODE
