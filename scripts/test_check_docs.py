"""Controprove isolate del gate documentale."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_docs as gate


class DocsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def test_scope_without_docs_and_with_nested_guides(self):
        self.write("README.md", "# Guida\n")
        self.assertEqual(gate.scan(self.root), (1, []))
        self.write("docs/adr/decisione.md", "# Decisione\n")
        self.write("crates/a/README.md", "# Crate\n")
        self.write("contracts/upstream/README.md", "[copia](assente.md)")
        self.write("target/README.md", "[generato](assente.md)")
        self.assertEqual(gate.scan(self.root), (3, []))

    def test_local_links_and_anchors(self):
        self.write("docs/guida.md", "# Qualità\n## Prova\n## Prova\n")
        self.write(
            "README.md",
            "[guida](docs/guida.md#qualità)\n"
            "[duplicato](docs/guida.md#prova-1)\n"
            "[remoto](https://example.com/assente)\n"
            "[email](mailto:info@example.com)\n",
        )
        self.assertEqual(gate.scan(self.root)[1], [])
        self.write("README.md", "[x](assente.md)\n[x](docs/guida.md#assente)\n")
        self.assertEqual(len(gate.scan(self.root)[1]), 2)

    def test_encoded_paths_and_same_page_anchor(self):
        self.write("docs/con spazio.md", "# Titolo\n[x](#titolo)\n")
        self.write("README.md", "[guida](docs/con%20spazio.md#titolo)\n")
        self.assertEqual(gate.scan(self.root)[1], [])

    def test_python_commands(self):
        self.write("scripts/check.py", "")
        self.write("README.md", "python scripts/check.py\npython3 scripts\\check.py\n")
        self.assertEqual(gate.scan(self.root)[1], [])
        self.write("README.md", "python scripts/assente.py\n")
        self.assertEqual(len(gate.scan(self.root)[1]), 1)

    def test_python_syntax_and_fences(self):
        for text, expected in (
            ("```python\nawait run()\n```\n", 0),
            ("```python\nif:\n```\n", 1),
            ("```sh\ncomando\n", 1),
        ):
            with self.subTest(text=text):
                self.write("README.md", text)
                self.assertEqual(len(gate.scan(self.root)[1]), expected)

    def test_unreadable_document(self):
        self.write("README.md", "").write_bytes(b"\xff")
        violations = gate.scan(self.root)[1]
        self.assertEqual([v.reason for v in violations], ["documento illeggibile"])

    def test_exit_status(self):
        violation = gate.Violation(self.root / "README.md", "link locale inesistente")
        for violations, expected in (([], 0), ([violation], 1)):
            with patch.object(gate, "scan", return_value=(1, violations)):
                with patch("builtins.print"):
                    self.assertEqual(gate.main(), expected)

    def test_repository(self):
        checked, violations = gate.scan()
        self.assertGreater(checked, 40)
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
