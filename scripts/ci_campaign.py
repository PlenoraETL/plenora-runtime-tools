"""Aggregate original outcomes after an exhaustive CI campaign, never softened conclusions."""
import json
import os
from pathlib import Path
import sys


def failures(steps, expected):
    if not expected or len(expected) != len(set(expected)):
        raise ValueError('invalid campaign inventory')
    return [name for name in expected if steps.get(name, {}).get('outcome') != 'success']


def main():
    expected = sys.argv[1:]
    steps = json.loads(os.environ['CAMPAIGN_STEPS'])
    failed = failures(steps, expected)
    rows = ['| Step | Original outcome |', '|---|---|']
    rows += [f"| {name} | {steps.get(name, {}).get('outcome', 'missing')} |" for name in expected]
    report = '\n'.join(rows) + '\n'
    print(report)
    if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
        with Path(summary).open('a', encoding='utf-8') as output:
            output.write(report)
    return int(bool(failed))


if __name__ == '__main__':
    sys.exit(main())
