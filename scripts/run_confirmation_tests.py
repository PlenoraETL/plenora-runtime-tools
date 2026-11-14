"""Run the entire Rust suite and retain only actual libtest success observations."""
import json
import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

from boundary_confirmation import identity


def passed_tests(output):
    return sorted(re.findall(r'^test ([\w:]+) \.\.\. ok\s*$', output, re.M))


def capture_command(command):
    # libtest nocapture can interleave diagnostics inside its own outcome line.
    # show-output preserves successful diagnostics after canonical result lines.
    return ['--show-output' if arg == '--nocapture' else arg for arg in command]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', default='workspace-tests')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', args.label):
        raise ValueError('invalid evidence label')
    directory = Path(os.environ['PLENORA_CONFIRMATION_DIR'])
    directory.mkdir(parents=True, exist_ok=True)
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    command = command or ['cargo', 'test', '--no-fail-fast', '--workspace', '--all-targets', '--all-features', '--locked']
    command = capture_command(command)
    log = directory/(args.label+'.log')
    with log.open('w', encoding='utf-8') as stream:
        result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, text=True, check=False)
    output = log.read_text(encoding='utf-8')
    print(output)
    passed = passed_tests(output)
    record = dict(identity(), exit_code=result.returncode, passed=passed, command=command)
    name = 'rust-tests.json' if args.label == 'workspace-tests' else args.label+'.json'
    (directory/name).write_text(json.dumps(record, indent=2)+'\n', encoding='utf-8')
    return result.returncode or int(not passed)


if __name__ == '__main__':
    sys.exit(main())
