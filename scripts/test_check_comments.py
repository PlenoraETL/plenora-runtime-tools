"""Controprove del gate commenti, adattate dal test di plenora-io-tools."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_comments as gate


class CommentTests(unittest.TestCase):
    def violations(self, source, name="sample.rs"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / name
            path.write_text(source, encoding="utf-8")
            return gate.check_file(path, root)

    def test_debt_markers(self):
        for marker in ("TODO", "FIXME", "HACK", "XXX"):
            with self.subTest(marker=marker):
                found = self.violations(f"// {marker}: lavoro aperto\n")
                self.assertEqual([v.rule for v in found], ["debito anonimo"])

    def test_whole_words(self):
        for word in ("METODO_CHIUSO", "metodo", "PREFIXME_NO", "XXXL"):
            self.assertEqual(self.violations(f"// {word}\n"), [])

    def test_history(self):
        for phrase in (
            "tranche 2", "writer pre-fix", "prima stesura",
            "versione precedente", "Da allora", "Qui c'era",
            "prima era", "roadmap", "nello stesso commit", "ADR 0005",
        ):
            with self.subTest(phrase=phrase):
                self.assertTrue(self.violations(f"// {phrase}\n"))

    def test_current_rationale_and_references(self):
        for phrase in (
            "Il limite impedisce allocazioni senza controllo.",
            "La compatibilità richiede di conservare il campo.",
            "Limite e chiusura: docs/qualification.md.",
            "Decisione: docs/adr/0005-apalis-and-nats-adapter-baseline.md.",
        ):
            self.assertEqual(self.violations(f"// {phrase}\n"), [])

    def test_strings_are_not_comments(self):
        sources = {
            "sample.rs": 'let a = "// TODO"; let b = r###"/* FIXME */"###;\n',
            "sample.py": 'value = "# TODO"\n',
            "sample.toml": 'value = "# FIXME"\n',
            "sample.yml": 'value: "# XXX"\n',
            "sample.ps1": '$value = "# HACK"\n',
            "sample.sh": 'value="# TODO"\n',
        }
        for name, source in sources.items():
            with self.subTest(name=name):
                self.assertEqual(self.violations(source, name), [])

    def test_extractors_and_line_numbers(self):
        for name, source, line in (
            ("sample.rs", '\n/* esterno /* TODO */ fine */', 2),
            ("sample.py", '\n# FIXME\n', 2),
            ("sample.py", '"""XXX"""\n', 1),
            ("sample.toml", 'value = 1 # HACK\n', 1),
        ):
            with self.subTest(name=name, source=source):
                found = self.violations(source, name)
                self.assertEqual(len(found), 1)
                self.assertEqual(found[0].line, line)

    def test_scope_includes_untracked_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            included = (
                "Cargo.toml", "crates/a/src/lib.rs", "adapters/a/src/lib.rs",
                "examples/a/src/main.rs", "tests/a.rs", "scripts/a.py",
                ".github/workflows/ci.yml", "fuzz/fuzz_targets/a.rs",
            )
            excluded = (
                "target/a.rs", "contracts/upstream/a.rs", "docs/a.py",
                "scripts/__pycache__/a.py", ".git/a.rs", "vendor/a.rs",
            )
            for name in included + excluded:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("", encoding="utf-8")
            self.assertEqual(
                {p.relative_to(root).as_posix() for p in gate.source_files(root)},
                set(included),
            )

    def test_exit_status(self):
        violation = gate.Violation(Path("sample.rs"), 1, "debito anonimo", "TODO")
        for violations, expected in (([], 0), ([violation], 1)):
            with patch.object(gate, "check_repository", return_value=(1, violations)):
                with patch("builtins.print"):
                    self.assertEqual(gate.main(), expected)

    def test_repository(self):
        checked, violations = gate.check_repository()
        self.assertGreater(checked, 200)
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
