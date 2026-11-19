#!/usr/bin/env python3
"""Piano PR dai workflow e ricevute locali; Python 3.14 e PyYAML del repo.

La lettura funziona anche su un interprete non qualificato, per poter riportare
la mancanza della versione richiesta. Nessun download di action o toolchain.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time
import uuid

try:
    import yaml
except ImportError:
    print("CI locale: manca PyYAML. Installare: python -m pip install -r requirements-ci-locale.txt", file=sys.stderr)
    raise SystemExit(2) from None

try:
    from .ci_locale_expr import Unsupported, evaluate, partial, render, truth
except ImportError:
    from ci_locale_expr import Unsupported, evaluate, partial, render, truth

ROOT = Path(__file__).resolve().parents[1]
# Sono adattamenti di ambiente, non una seconda lista dei gate. Le action
# sconosciute restano visibili. Non installare nulla nell'host implicitamente.
ACTION_MAP = {
    "actions/checkout": "checkout",
    "actions/setup-python": "python",
    "actions-rust-lang/setup-rust-toolchain": "rust",
    "Swatinem/rust-cache": "cache",
    "dtolnay/rust-toolchain": "rust",
    "actions/cache": "cache",
    "actions/upload-artifact": "upload",
    "actions/download-artifact": "download",
}
ACTION_REVISIONS = {
    "actions/checkout": {"3d3c42e5aac5ba805825da76410c181273ba90b1"},
    "dtolnay/rust-toolchain": {"6bed0761d98439e5a578e2877258200ad565ba87"},
    "Swatinem/rust-cache": {"6323deb102c322ba6fcbdcafc7e3dddab59af2b6"},
    "actions/upload-artifact": {"043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"},
}
# Additional checkouts are verified in place; no clone, fetch or checkout.
CHECKOUT_POLICY = {"repository": "OWNER/REPO", "ref": "exact SHA or locally resolved ref",
                   "path": "existing clean checkout inside worktree", "writes_git": False}
ACTION_INPUTS = {
    "checkout": {"ref", "fetch-depth", "repository", "path", "submodules", "lfs"},
    "python": {"python-version", "python-version-file"},
    "rust": {"toolchain", "components"},
    "cache": {"workspaces", "path", "key", "restore-keys"},
    "upload": {"name", "path", "if-no-files-found", "retention-days", "compression-level"},
    "download": {"name", "path", "pattern", "merge-multiple"},
}
SHELLS = {
    "bash": (["bash", "--noprofile", "--norc", "-e", "-o", "pipefail"], ".sh"),
    "sh": (["sh", "-e"], ".sh"),
    "pwsh": (["pwsh", "-NoLogo", "-NoProfile", "-NonInteractive", "-File"], ".ps1"),
    "powershell": (["powershell", "-NoLogo", "-NoProfile", "-NonInteractive", "-File"], ".ps1"),
    "python": ([sys.executable], ".py"),
}
PUBLISH = re.compile(r"\b(?:cargo\s+publish\b|npm\s+publish\b|twine\s+upload\b|gh\s+(?:release|api)\b|git\s+push\b)")
LIVE = re.compile(r"\b(?:docker|podman|vagrant|virsh|VBoxManage|psql|mysql|mongosh)\b", re.I)

# Tabella di adattamenti, non elenco dei gate: estendere qui per la famiglia.
PRECONDITIONS = (
    (r"\b(?:sudo|apt-get|brew|choco)\b", "provisioning", "provisioning di sistema riservato alla CI"),
    (r"\bgdal-config\b|--features[ =]+gdal-backend|--require-filegdb", "tool", "gdal-config"),
    (r"\bdocker\b", "tool", "docker"),
    (r"\bpwsh\b", "tool", "pwsh"),
    (r"\bcargo\b", "tool", "cargo"),
    (r"\bgit\b", "tool", "git"),
)


def policy_env(offline):
    return {"CARGO_BUILD_JOBS": "4", "CARGO_INCREMENTAL": "0", "GIT_OPTIONAL_LOCKS": "0", "RUSTUP_AUTO_INSTALL": "0",
            "CARGO_NET_OFFLINE": "true" if offline else "false",
            "PIP_NO_INDEX": "1" if offline else "0", "PIP_DISABLE_PIP_VERSION_CHECK": "1"}


def preconditions(job, step, root, command, offline):
    """Controlli prima del gate. Nessuna reinterpretazione del suo exit code."""
    direct = step.get("run") or ""
    if offline:
        # Una wheelhouse deve essere esplicita e locale; la sola cache pip non basta.
        for line in command.splitlines():
            if re.search(r"\bpip(?:3)?\s+install\b", line):
                links = re.search(r"--find-links(?:=|\s+)([\w./-]+)", line)
                local = links and inside(root, links[1]).is_dir()
                if not ("--no-index" in line and local and "http" not in line):
                    return "offline: pip install senza wheelhouse locale esplicita"
        if re.search(r"\b(?:curl|wget)\b|urlretrieve\(|urlopen\(|requests\.(?:get|post)\(|\bgit\s+(?:fetch|clone|pull)\b", command):
            return "offline: download richiesto dal comando/script"
        if re.search(r"\bcargo\s+(?:\+\S+\s+)?(?:build|test|check|clippy|run|fetch|metadata|install|update|bench)\b", command):
            return "offline: risoluzione Cargo; completezza della cache non certificata"
    for pattern, kind, value in PRECONDITIONS:
        if not re.search(pattern, direct, re.S):
            continue
        if kind == "provisioning":
            return value
        if kind == "tool" and not shutil.which(value):
            return f"strumento di sistema assente: {value}"
    if step.get("mapping") == "checkout" and not step["with"].get("path") and step["with"].get("fetch-depth") == "0":
        if git(root, "rev-parse", "--is-shallow-repository") == "true":
            return "storia git completa richiesta; clone shallow"
    if step.get("mapping") == "python":
        wanted = step["toolchain"].get("python", "")
        if re.fullmatch(r"\d+\.\d+(?:\.\d+)?", wanted) and tuple(map(int, wanted.split('.'))) != sys.version_info[:len(wanted.split('.'))]:
            return f"Python {wanted} richiesto; host {platform.python_version()}"
    if step.get("mapping") == "rust":
        if not shutil.which("rustup"):
            return "strumento di sistema assente: rustup"
        wanted = step["with"].get("toolchain", "")
        try:
            installed = subprocess.check_output(["rustup", "toolchain", "list"], text=True, stderr=subprocess.PIPE)
            if not any(line.split()[0] == wanted or line.startswith(wanted + "-") for line in installed.splitlines()):
                return f"toolchain Rust assente: {wanted}"
            components = [c.strip().removesuffix("-preview") for c in step["with"].get("components", "").split(",") if c.strip()]
            if components:
                present = subprocess.check_output(["rustup", "component", "list", "--installed", "--toolchain", wanted],
                                                  text=True, stderr=subprocess.PIPE)
                for component in components:
                    if not any(line == component or line.startswith(component + "-") for line in present.splitlines()):
                        return f"componente Rust assente: {component} ({wanted})"
        except (OSError, subprocess.SubprocessError):
            return "toolchain Rust non verificabile"
    return ""


def inside(root, relative):
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or ".git" in path.parts:
        raise Unsupported(f"percorso fuori dal worktree: {relative}")
    return path


def read_yaml(root, relative):
    # BaseLoader evita che YAML 1.1 trasformi la chiave 'on' in True.
    class UniqueLoader(yaml.BaseLoader):
        def construct_mapping(self, node, deep=False):
            result = {}
            for key_node, value_node in node.value:
                key = self.construct_object(key_node, deep=deep)
                if key in result:
                    raise Unsupported("membro YAML duplicato")
                result[key] = self.construct_object(value_node, deep=deep)
            return result
    data = yaml.load(inside(root, relative).read_text(encoding="utf-8"), Loader=UniqueLoader)
    if not isinstance(data, dict):
        raise Unsupported(f"documento YAML non mapping: {relative}")
    return data


def matrices(matrix):
    if not matrix:
        return [{}]
    if not isinstance(matrix, dict):
        raise Unsupported("matrice dinamica non risolvibile senza output Actions")
    axes = {k: v for k, v in matrix.items() if k not in ("include", "exclude")}
    if any(not isinstance(v, list) for v in axes.values()):
        raise Unsupported("asse matrice non statico")
    originals = [dict(zip(axes, values)) for values in itertools.product(*axes.values())] if axes else []
    originals = [row for row in originals if not any(
        all(row.get(k) == v for k, v in exc.items()) for exc in matrix.get("exclude", []))]
    rows = [dict(row) for row in originals]
    for inc in matrix.get("include", []):
        matched = False
        for original, row in zip(originals, rows):
            if all(k not in original or original[k] == v for k, v in inc.items()):
                row.update(inc)
                matched = True
        if not matched:
            rows.append(dict(inc))
    return rows


def runner_os(label):
    labels = label if isinstance(label, list) else [label]
    for value in labels:
        for prefix, name in (("windows", "Windows"), ("ubuntu", "Linux")):
            if str(value).lower().startswith(prefix):
                return name
    return "sconosciuta"


def events(value):
    return [value] if isinstance(value, str) else list(value or {})


def base_context(root, sha):
    return {"github.event_name": "pull_request", "github.sha": sha,
            "github.event.pull_request.head.sha": sha, "github.workspace": str(root),
            "always()": True, "cancelled()": False, "success()": True, "failure()": False}


def derive(root=ROOT, sha="HEAD", profile="pr"):
    root = root.resolve()
    plan = {"action_revisions": {k: sorted(v) for k, v in ACTION_REVISIONS.items()}, "checkout_policy": CHECKOUT_POLICY, "schema": 2, "sha": sha, "profile": profile, "jobs": [], "excluded_workflows": [], "sources": {}}

    for name in ("rust-toolchain.toml", ".python-version", "contracts/source.json"):
        if (root / name).is_file():
            plan["sources"][name] = hashlib.sha256((root / name).read_bytes()).hexdigest()

    def document(relative):
        data = read_yaml(root, relative)
        plan["sources"][relative] = hashlib.sha256(inside(root, relative).read_bytes()).hexdigest()
        return data

    def steps(items, job, defaults, env, context, origin, chain=()):
        result = []
        for index, raw in enumerate(items):
            if not isinstance(raw, dict):
                raw = {"name": "passo mancante o non mapping"}
            pointer = f"{origin}/steps/{index}"
            step_env = partial({**env, **raw.get("env", {})}, context)
            ctx = {**context, **{f"env.{k}": v for k, v in step_env.items()}}
            resolved = partial(raw, ctx)
            runtime = partial(raw, {k: v for k, v in ctx.items()
                                    if not k.startswith("env.") and k not in
                                    ("success()", "failure()", "always()", "cancelled()")})
            use = resolved.get("uses", "")
            entry = {"source": pointer, "raw": raw, "runtime": runtime, "name": resolved.get("name", use or str(index)),
                     "run": resolved.get("run"), "uses": use, "with": resolved.get("with", {}),
                     "shell": resolved.get("shell", defaults.get("shell", "pwsh" if job["os"] == "Windows" else "bash")),
                     "working_directory": resolved.get("working-directory", defaults.get("working-directory", ".")),
                     "env": step_env, "if": runtime.get("if", "success()"),
                     "continue_on_error": resolved.get("continue-on-error", "false"),
                     "toolchain": dict(job["toolchain"]), "reason": ""}
            if use.startswith("./"):
                try:
                    if use in chain:
                        raise Unsupported("ciclo nelle action locali")
                    directory = inside(root, use)
                    action_path = next((p for p in (directory / "action.yml", directory / "action.yaml") if p.is_file()), None)
                    if action_path is None:
                        raise Unsupported(f"action locale assente: {use}")
                    action = document(action_path.relative_to(root).as_posix())
                    if action.get("runs", {}).get("using") != "composite":
                        raise Unsupported("action locale non composite (runtime non mappato)")
                    inputs = {k: v.get("default", "") for k, v in action.get("inputs", {}).items()}
                    inputs.update(entry["with"])
                    child_ctx = {**ctx, **{f"inputs.{k}": v for k, v in inputs.items()},
                                 "github.action_path": str(directory)}
                    entry["children"] = steps(action["runs"].get("steps", []), job, defaults,
                                              step_env, child_ctx, action_path.relative_to(root).as_posix() + "/runs", (*chain, use))
                    for child in flatten(entry["children"]):
                        child.setdefault("parent_conditions", []).append(entry["if"])
                    # Wrapper conservato; i figli hanno tutti una ricevuta propria.
                    entry["reason"] = "contenitore composite; esiti nei figli"
                except (Unsupported, OSError) as exc:
                    entry["reason"] = str(exc)
            elif use:
                action, _, revision = use.partition("@")
                mapping = ACTION_MAP.get(action) if revision in ACTION_REVISIONS.get(action, set()) else None
                entry["mapping"] = mapping
                if mapping and set(entry["with"]) - ACTION_INPUTS[mapping]:
                    entry["reason"] = f"opzioni action non mappate: {sorted(set(entry['with']) - ACTION_INPUTS[mapping])}"
                if mapping == "python":
                    version = entry["with"].get("python-version", "")
                    if "python-version-file" in entry["with"]:
                        try:
                            version = inside(root, entry["with"]["python-version-file"]).read_text(encoding="utf-8").strip()
                        except (OSError, Unsupported) as exc:
                            entry["reason"] = str(exc)
                    job["toolchain"]["python"] = version
                elif mapping == "rust":
                    job["toolchain"]["rust"] = entry["with"]
                elif mapping == "cache":
                    entry["reason"] = "cache Actions non riprodotta; nessun gate eseguito"
                elif mapping in ("upload", "download"):
                    entry["reason"] = "trasferimento artefatti Actions non riprodotto localmente"
                elif mapping is None:
                    entry["reason"] = f"action non mappata: {use}"
                entry["toolchain"] = dict(job["toolchain"])
            elif not isinstance(entry["run"], str) or not entry["run"].strip():
                entry["reason"] = "passo senza run o uses"
            result.append(entry)
        return result

    def workflow(relative, prefix="", inherited=None, chain=()):
        data = document(relative)
        inherited = inherited or {}
        for key, raw in data.get("jobs", {}).items():
            error = ""
            try:
                expanded = matrices(raw.get("strategy", {}).get("matrix"))
                if not expanded:
                    expanded, error = [{}], "matrice senza combinazioni attive"
            except Unsupported as exc:
                expanded, error = [{}], str(exc)
            for number, matrix in enumerate(expanded):
                ctx = {**base_context(root, sha), **inherited.get("context", {}),
                       **{f"matrix.{k}": v for k, v in matrix.items()}}
                label = partial(raw.get("runs-on", ""), ctx)
                ctx["runner.os"] = runner_os(label)
                env = partial(data.get("env", {}), ctx)
                ctx.update({f"env.{k}": v for k, v in env.items()})
                env = partial({**env, **raw.get("env", {})}, ctx)
                ctx.update({f"env.{k}": v for k, v in env.items()})
                identifier = f"{prefix}{relative}:{key}[{number}]"
                job = {"id": identifier, "key": key, "workflow": relative, "source": relative + "/jobs/" + key,
                       "raw": raw, "matrix": matrix, "runs_on": label, "os": runner_os(label),
                       "env": env, "context": ctx, "services": partial(raw.get("services", {}), ctx),
                       "container": partial(raw.get("container"), ctx), "needs": raw.get("needs", []),
                       "if": raw.get("if", "success()"), "caller_conditions": inherited.get("conditions", []),
                       "continue_on_error": raw.get("continue-on-error", "false"),
                       "blocking": raw.get("continue-on-error", "false") != "true",
                       "filters": data.get("on", {}), "toolchain": {}, "reason": error,
                       "release": inherited.get("release", False) or "release" in events(data.get("on")) or bool(re.search(r"release|rilascio|distribuzione", relative, re.I))}
                job["profile"] = "release" if job["release"] else "pr" if "pull_request" in events(data.get("on")) else "qualifica"
                job["selected_profile"] = profile
                defaults = {**data.get("defaults", {}).get("run", {}), **raw.get("defaults", {}).get("run", {})}
                job["steps"] = steps(raw.get("steps", []), job, defaults, env, ctx, job["source"])
                plan["jobs"].append(job)
                use = raw.get("uses", "")
                if use.startswith("./"):
                    try:
                        if use in chain:
                            raise Unsupported("ciclo nei workflow riusabili")
                        child = document(use[2:])
                        call = child.get("on", {}).get("workflow_call") or {}
                        inputs = {k: v.get("default", "") for k, v in call.get("inputs", {}).items()}
                        inputs.update(partial(raw.get("with", {}), ctx))
                        job["reason"] = "chiamata locale; job espansi con prefisso del chiamante"
                        workflow(use[2:], identifier + "/", {"context": {f"inputs.{k}": v for k, v in inputs.items()},
                                 "conditions": job["caller_conditions"] + [job["if"]], "release": job["release"]}, (*chain, use))
                    except (OSError, Unsupported) as exc:
                        job["reason"] = str(exc)
                elif use:
                    job["reason"] = f"workflow remoto non mappabile offline: {use}"
    for path in sorted((root / ".github/workflows").iterdir()):
        if path.suffix not in (".yml", ".yaml"):
            continue
        relative = path.relative_to(root).as_posix()
        data = document(relative)
        workflow(relative)
        if "pull_request" not in events(data.get("on")):
            plan["excluded_workflows"].append({"path": relative, "reason": "nessun trigger pull_request", "events": data.get("on")})
    return plan


def flatten(items):
    for step in items:
        yield step
        yield from flatten(step.get("children", []))


def host_os():
    return platform.system() if platform.system() in ("Linux", "Windows") else "sconosciuta"


def classify(job, step, root, allow_services=False, offline=False):
    """Disponibilita' statica; non afferma che prerequisiti o test siano verdi."""
    if job["release"] or PUBLISH.search(step.get("run") or ""):
        return "NON ESEGUITO", "release/pubblicazione vietata"
    if job.get("profile", "pr") != job.get("selected_profile", "pr"):
        return "NON ESEGUITO", "job fuori profilo"
    if job["reason"] and not job["raw"].get("uses", "").startswith("./"):
        return "NON ESEGUITO", job["reason"]
    if host_os() not in ("Linux", "Windows") or job["os"] not in ("Linux", "Windows"):
        return "NON ESEGUITO", "piattaforma non supportata"
    if job["os"] != host_os():
        return "NON ESEGUITO", f"piattaforma richiesta {job['runs_on']}; host {host_os()}"
    if job["container"] or job["services"]:
        return "NON ESEGUITO", ("servizi/container live non abilitati" if not allow_services else
                                "provisioning dei servizi/container dichiarati non mappato")
    command = step.get("run") or ""
    # Comprende gli script locali direttamente richiamati, senza eseguirli.
    for name in re.findall(r"(?:\./)?scripts/[\w./-]+\.(?:py|sh|ps1)", command):
        path = inside(root, name)
        if path.is_file():
            command += "\n" + path.read_text(encoding="utf-8")
    if PUBLISH.search(command):
        return "NON ESEGUITO", "script con pubblicazione/remoto vietato"
    if LIVE.search(command):
        return "NON ESEGUITO", "comando/script richiede servizi live o VM non riprodotti"
    if step["reason"]:
        return "NON ESEGUITO", step["reason"]
    missing = preconditions(job, step, root, command, offline)
    if missing:
        return "NON ESEGUITO", missing
    if step.get("mapping") == "checkout" and not any(
            "${{" in str(value) for value in step["with"].values()):
        try:
            source = checkout_source(step["with"], root, offline)
            if source is not None:
                additional_checkout(root, step["with"], source, emit=False)
        except (Unsupported, OSError, subprocess.SubprocessError) as exc:
            return "NON ESEGUITO", str(exc)
    if step.get("mapping"):
        return "ESEGUIBILE", "verifica locale del setup, senza installazioni"
    shell = step["shell"]
    if shell not in SHELLS:
        return "NON ESEGUITO", f"shell non mappata: {shell}"
    if not shutil.which(SHELLS[shell][0][0]):
        return "NON ESEGUITO", f"shell richiesta non disponibile: {shell}"
    return "ESEGUIBILE", "prerequisiti e condizioni verificati durante l'esecuzione"


def checkout_source(args, root, offline=False):
    """Validate an existing checkout without accessing a remote or writing Git."""
    if any(str(args.get(k, "false")).lower() not in ("", "false") for k in ("submodules", "lfs")):
        raise Unsupported("checkout submodules/lfs non mappato")
    if not args.get("repository") and not args.get("path"):
        return None
    destination = inside(root, args.get("path", "."))
    if destination == root.resolve() or not args.get("ref"):
        raise Unsupported("checkout aggiuntivo richiede path distinto e ref esplicito")
    if not (destination / ".git").exists():
        raise Unsupported("checkout aggiuntivo assente; preparazione esterna richiesta")
    return str(destination)


def additional_checkout(root, args, source, emit=True):
    """Verify repository, ref and clean HEAD; never mutate a checkout."""
    destination = inside(root, args["path"])
    if str(destination) != source:
        raise Unsupported("sorgente checkout diversa dal path dichiarato")
    repository = args.get("repository")
    origin = git(destination, "remote", "get-url", "origin")
    if repository and origin not in (f"https://github.com/{repository}.git",
                                     f"https://github.com/{repository}", f"git@github.com:{repository}.git"):
        raise Unsupported("repository checkout diverso dal dichiarato")
    ref = args["ref"]
    if ref.startswith("-") or any(c.isspace() for c in ref):
        raise Unsupported("ref checkout non valido")
    expected = git(destination, "rev-parse", "--verify", ref + "^{commit}")
    obtained = git(destination, "rev-parse", "HEAD")
    if expected != obtained or git(destination, "status", "--porcelain"):
        raise Unsupported("checkout sporco o SHA diverso dal ref richiesto")
    if re.fullmatch(r"[0-9a-fA-F]{40}", ref) and expected.lower() != ref.lower():
        raise Unsupported("SHA richiesto diverso dal risolto")
    evidence = {"repository": repository, "ref": ref, "path": str(destination),
                "expected_sha": expected, "obtained_sha": obtained}
    if emit:
        print(json.dumps(evidence))
    return evidence


def action_command(step, root, sha, offline=False):
    mapping, args = step["mapping"], step["with"]
    if mapping == "checkout":
        source = checkout_source(args, root, offline)
        if source is not None:
            return [sys.executable, "-c", "import sys,json; from pathlib import Path; "
                    "sys.path.insert(0,sys.argv[1]); from ci_locale import additional_checkout, strict_json; "
                    "additional_checkout(Path(sys.argv[2]),strict_json(sys.argv[3]),sys.argv[4])",
                    str(Path(__file__).resolve().parent), str(root), json.dumps(args), source]
        if args.get("ref", sha) != sha:
            raise Unsupported("checkout di revisione diversa da HEAD")
        return [sys.executable, "-c", "import subprocess,sys; "
                "head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(); print(head); "
                "shallow=subprocess.check_output(['git','rev-parse','--is-shallow-repository'],text=True).strip(); "
                "raise SystemExit(0 if head==sys.argv[1] and (sys.argv[2]!='0' or shallow=='false') else 1)",
                sha, args.get("fetch-depth", "1")]
    if mapping == "python":
        version = step["toolchain"]["python"]
        if not re.fullmatch(r"\d+\.\d+(?:\.\d+)?", version):
            raise Unsupported(f"versione Python non esatta: {version}")
        return [sys.executable, "-c", "import sys; print(sys.version); "
                "raise SystemExit(0 if sys.version_info[:len(sys.argv[1].split('.'))] == "
                "tuple(map(int, sys.argv[1].split('.'))) else 1)", version]
    if mapping == "rust":
        version = args.get("toolchain", "")
        if not version or any(c in version for c in ", \n"):
            raise Unsupported("toolchain Rust non singola o non dichiarata")
        components = [x.strip().removesuffix("-preview") for x in args.get("components", "").split(",") if x.strip()]
        return [sys.executable, "-c",
                "import subprocess,sys; v=sys.argv[1]; "
                "subprocess.run(['rustup','run',v,'rustc','--version'],check=True); "
                "s=subprocess.check_output(['rustup','component','list','--installed','--toolchain',v],text=True); "
                "print(s); raise SystemExit(0 if all(any(line.startswith(c+'-') or line==c "
                "for line in s.splitlines()) for c in sys.argv[2:]) else 1)", version, *components]
    raise Unsupported(f"mapping senza esecutore: {mapping}")


def strict_json(source):
    """Reject duplicate members and non-finite numbers in structured arguments."""
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise Unsupported("membro JSON duplicato")
            result[key] = value
        return result
    def constant(_):
        raise Unsupported("numero JSON non finito")
    return json.loads(source, object_pairs_hook=unique, parse_constant=constant)


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, text=True, stderr=subprocess.PIPE,
                                   env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).strip()


def condition(source, context):
    """Actions aggiunge success() quando manca una funzione di stato."""
    expression = re.sub(r"'(?:[^']|'')*'", "", str(source))
    if not re.search(r"\b(?:success|failure|always|cancelled)\s*\(", expression):
        if not context["success()"]:
            return False
    return truth(evaluate(source, context))


def execute(plan, root, selected=(), allow_services=False, timeout=1800, offline=False):
    root = root.resolve()
    known = {j["key"] for j in plan["jobs"]} | {j["id"] for j in plan["jobs"]}
    if set(selected) - known:
        raise Unsupported(f"job sconosciuti: {sorted(set(selected) - known)}")
    sha = git(root, "rev-parse", "HEAD")
    directory = root / "target/ci-locale" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8])
    directory.mkdir(parents=True)
    receipt = {"schema": 2, "network_mode": "offline" if offline else "online",
               "services_opt_in": allow_services, "sha": sha, "dirty": bool(git(root, "status", "--porcelain")),
               "started_at": datetime.now(timezone.utc).isoformat(), "host": platform.platform(),
               "python": sys.version, "selected_jobs": list(selected), "profile": plan["profile"], "sources": plan["sources"],
               "overrides": policy_env(offline), "jobs": [], "steps": []}
    path = directory / "ricevuta.json"

    def save():
        receipt["counts"] = dict(Counter(s["status"] for s in receipt["steps"]))
        path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")

    for job in plan["jobs"]:
        state = {"id": job["id"], "matrix": job["matrix"], "needs": job["needs"], "reason": job["reason"]}
        receipt["jobs"].append(state)
        temp = directory / "tmp"
        temp.mkdir(exist_ok=True)
        context = {**job["context"], **base_context(root, sha), "runner.temp": str(temp)}
        try:
            job_enabled = all(condition(c, context) for c in job["caller_conditions"] + [job["if"]])
            job_condition_reason = "condizione del job falsa"
        except Unsupported as exc:
            job_enabled, job_condition_reason = False, str(exc)
        exported = {}
        failed, condition_failed, incomplete = False, False, False
        records = []
        for step in flatten(job["steps"]):
            status, reason = classify(job, step, root, allow_services, offline)
            context = {k: v for k, v in context.items() if not k.startswith("env.")}
            runtime = step.get("runtime", step["raw"])
            runtime_env = {**step["env"], **exported, **runtime.get("env", {})}
            context.update({f"env.{k}": v for k, v in runtime_env.items()})
            try:
                runtime_env = render(runtime_env, context)
                context.update({f"env.{k}": v for k, v in runtime_env.items()})
            except Unsupported as exc:
                status, reason = "NON ESEGUITO", str(exc)
            record = {"job": job["id"], **step, "status": status, "reason": reason,
                      "services": job["services"], "container": job["container"], "duration_seconds": 0,
                      "exit_code": None, "command": step["run"] or step["uses"],
                      "environment": dict(step["env"]), "log": None, "condition_false": False}
            if job["os"] == host_os() and step["if"] != "success()":
                context.update({"success()": not condition_failed and not incomplete, "failure()": condition_failed})
                try:
                    if not condition(step["if"], context):
                        status = "NON ESEGUITO"
                        record.update(status=status, reason=f"condizione falsa: {step['if']}", condition_false=True)
                except Unsupported:
                    pass  # L'esecutore riporta il contesto mancante se il passo e' mappato.
            records.append(record)
            receipt["steps"].append(record)
            if selected and job["key"] not in selected and job["id"] not in selected:
                record.update(status="NON ESEGUITO", reason="job non selezionato")
            elif status == "ESEGUIBILE":
                try:
                    if not job_enabled:
                        raise Unsupported(job_condition_reason)
                    # Le dipendenze Actions non si inventano dai file residui.
                    if job["needs"]:
                        raise Unsupported(f"needs non riprodotto localmente: {job['needs']}")
                    context.update({"success()": not condition_failed and not incomplete, "failure()": condition_failed})
                    conditions = step.get("parent_conditions", []) + [step["if"]]
                    if not all(condition(c, context) for c in conditions):
                        record["condition_false"] = not incomplete and not condition_failed
                        raise Unsupported("condizione falsa o prerequisito precedente non PASS: " + str(conditions))
                    runtime = step.get("runtime", step["raw"])
                    runtime_env = {**step["env"], **exported, **runtime.get("env", {})}
                    context.update({f"env.{k}": v for k, v in runtime_env.items()})
                    runtime_env = render(runtime_env, context)
                    context.update({f"env.{k}": v for k, v in runtime_env.items()})
                    values = render({"run": runtime.get("run"), "env": runtime_env,
                                     "with": runtime.get("with", {}),
                                     "working_directory": runtime.get("working-directory", step["working_directory"])}, context)
                    working = inside(root, values["working_directory"])
                    if not working.is_dir():
                        raise Unsupported(f"working-directory assente: {working}")
                    env = {**os.environ, **{k: str(v) for k, v in values["env"].items()}, **receipt["overrides"]}
                    temp = directory / "tmp"
                    temp.mkdir(exist_ok=True)
                    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")
                    env.update(GITHUB_WORKSPACE=str(root), RUNNER_TEMP=str(temp), RUNNER_OS=host_os())
                    for variable in ("GITHUB_ENV", "GITHUB_PATH", "GITHUB_OUTPUT"):
                        file = directory / f"{len(receipt['steps']):04d}-{variable}.txt"
                        file.touch()
                        env[variable] = str(file)
                    rust = step["toolchain"].get("rust", {}).get("toolchain")
                    if rust and "RUSTUP_TOOLCHAIN" not in values["env"]:
                        env["RUSTUP_TOOLCHAIN"] = rust
                    if step.get("mapping"):
                        command = action_command({**step, **values}, root, sha, offline)
                    else:
                        argv, suffix = SHELLS[step["shell"]]
                        script = directory / f"{len(receipt['steps']):04d}{suffix}"
                        body = values["run"]
                        if step["shell"] in ("pwsh", "powershell"):
                            # Windows PowerShell 5 non rende fatali i fallimenti
                            # nativi intermedi: non fingere parita' con pwsh 7.3+.
                            body = ("$ErrorActionPreference = 'Stop'\n"
                                    "if ($PSVersionTable.PSVersion -lt [version]'7.3') { throw 'Richiesto PowerShell 7.3 per errori nativi fatali' }\n"
                                    "$PSNativeCommandUseErrorActionPreference = $true\n" + body +
                                    "\nif (Test-Path variable:LASTEXITCODE) { exit $LASTEXITCODE }\n")
                        script.write_text(body, encoding="utf-8", newline="\n")
                        # Usa la shell verificata in PATH anche su Windows,
                        # dove CreateProcess puo' preferire il launcher WSL.
                        command = [shutil.which(argv[0]) or argv[0], *argv[1:], str(script)]
                    record.update(command=command, resolved_run=values["run"], working_directory=str(working),
                                  environment={k: v for k, v in env.items() if k in values["env"] or k in receipt["overrides"]
                                               or k in ("GITHUB_WORKSPACE", "GITHUB_ENV", "GITHUB_PATH", "GITHUB_OUTPUT",
                                                        "RUNNER_TEMP", "RUNNER_OS", "RUSTUP_TOOLCHAIN", "PATH", "PYTHONPATH")})
                    log = directory / f"{len(receipt['steps']):04d}.log"
                    start = time.monotonic()
                    with log.open("w", encoding="utf-8") as output:
                        try:
                            code = subprocess.run(command, cwd=working, env=env, stdout=output,
                                                  stderr=subprocess.STDOUT, timeout=timeout).returncode
                        except OSError as exc:
                            raise Unsupported(f"processo non avviabile: {exc}") from exc
                        except subprocess.TimeoutExpired as exc:
                            output.write(str(exc))
                            code = 125
                    record.update(status="PASS" if code == 0 else "FAIL", exit_code=code,
                                  duration_seconds=round(time.monotonic() - start, 3), log=str(log),
                                  reason="processo terminato" if code == 0 else "processo fallito")
                    # Uno script che dichiara esplicitamente di non avere provato
                    # non riceve PASS solo perche' termina con zero.
                    if code == 0 and re.search(r"\b(?:skipping|skipped|NON ESEGUITO)\b", log.read_text(encoding="utf-8", errors="replace"), re.I):
                        record.update(status="NON ESEGUITO", reason="il log dichiara prove saltate; consultare il log")
                    for line in Path(env["GITHUB_ENV"]).read_text(encoding="utf-8-sig").splitlines():
                        if "=" not in line or "<<" in line:
                            raise Unsupported("GITHUB_ENV multilinea/non mappato")
                        key, value = line.split("=", 1)
                        exported[key] = value
                        context[f"env.{key}"] = value
                    if Path(env["GITHUB_PATH"]).read_text(encoding="utf-8-sig").strip():
                        raise Unsupported("GITHUB_PATH modificato; propagazione non mappata")
                    for line in Path(env["GITHUB_OUTPUT"]).read_text(encoding="utf-8-sig").splitlines():
                        if "=" not in line or "<<" in line:
                            raise Unsupported("GITHUB_OUTPUT multilinea/non mappato")
                        key, value = line.split("=", 1)
                        context[f"steps.{step['raw'].get('id', '')}.outputs.{key}"] = value
                except (Unsupported, OSError) as exc:
                    if record["status"] != "FAIL":
                        record.update(status="NON ESEGUITO", reason=str(exc))
            failed |= record["status"] == "FAIL"
            condition_failed |= record["status"] == "FAIL" and str(step["continue_on_error"]).lower() != "true"
            # La cache e i contenitori composite non sono prerequisiti dei gate.
            incomplete |= record["status"] in ("NON ESEGUITO", "SALTATO") and not (
                step.get("mapping") == "cache" or "children" in step or record["condition_false"])
            print(f"{record['status']}: {job['id']} / {step['name']}: {record['reason']}", flush=True)
            save()
        state["status"] = "FAIL" if failed else "PASS" if records and all(r["status"] == "PASS" for r in records) else "NON ESEGUITO"
    receipt.update(finished_at=datetime.now(timezone.utc).isoformat(),
                   final_sha=git(root, "rev-parse", "HEAD"), final_dirty=bool(git(root, "status", "--porcelain")))
    receipt["exit_code"] = (1 if any(s["status"] == "FAIL" for s in receipt["steps"]) else
                            2 if not receipt["jobs"] or any(j["status"] != "PASS" for j in receipt["jobs"]) else 0)
    save()
    return path, receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--piano", action="store_true")
    mode.add_argument("--esegui", action="store_true")
    parser.add_argument("--profilo", choices=["pr", "qualifica", "release"], default="pr")
    parser.add_argument("--job", action="append", default=[])
    parser.add_argument("--offline", action="store_true", help="non eseguire passi che richiedono rete")
    parser.add_argument("--servizi-live", action="store_true", help="abilita comandi live; nessun provisioning implicito")
    parser.add_argument("--timeout", type=int, default=1800, help="secondi per passo")
    args = parser.parse_args(argv)
    try:
        sha = git(ROOT, "rev-parse", "HEAD")
        if args.esegui and args.profilo != "pr":
            raise Unsupported("qualifica e release sono disponibili solo come piano")
        plan = derive(ROOT, sha, args.profilo)
        plan.update(schema=2, network_mode="offline" if args.offline else "online")
        known = {value for job in plan["jobs"] for value in (job["key"], job["id"])}
        if set(args.job) - known:
            raise Unsupported(f"job sconosciuti: {sorted(set(args.job) - known)}")
        if args.piano:
            for job in plan["jobs"]:
                for step in flatten(job["steps"]):
                    status, reason = classify(job, step, ROOT, args.servizi_live, args.offline)
                    if args.job and job["key"] not in args.job and job["id"] not in args.job:
                        status, reason = "NON ESEGUITO", "job non selezionato"
                    step["availability"] = "ESEGUIBILE" if status == "ESEGUIBILE" else "NON ESEGUIBILE"
                    step["availability_reason"] = reason
            print(json.dumps(plan, ensure_ascii=False, indent=2))
            return 0
        path, receipt = execute(plan, ROOT, args.job, args.servizi_live, args.timeout, args.offline)
        print(f"Ricevuta: {path}\nEsiti: {receipt['counts']}")
        # Un piano incompleto non diventa verde neppure come exit code.
        if receipt["counts"].get("FAIL"):
            return 1
        if not receipt["jobs"] or any(j["status"] != "PASS" for j in receipt["jobs"]):
            return 2
        return 0
    except (Unsupported, OSError, yaml.YAMLError, subprocess.SubprocessError) as exc:
        print(f"CI locale: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
