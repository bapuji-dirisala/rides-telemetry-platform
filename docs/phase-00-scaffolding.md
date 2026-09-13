# Phase 0 — Repo scaffolding

## Goal

Set up the repo with the same tooling discipline as the wider portfolio so
every subsequent phase can focus on the streaming problem rather than
re-litigating Python packaging, linting, or CI conventions.

## What this phase delivers

- `pyproject.toml` with:
  - `hatchling` build backend
  - Python 3.11-3.12 supported
  - MIT license
  - Optional extras: `producer` (Kafka client + Faker), `spark`
    (PySpark 3.5.3 + delta-spark 3.2.1), `aws` (boto3), `dev`
    (pytest, ruff, mypy, pre-commit)
  - Ruff config with pycodestyle, pyflakes, isort, bugbear, comprehensions,
    pyupgrade, pep8-naming, and simplify rules enabled
- `.pre-commit-config.yaml` with ruff (fix + format) and mypy running on
  every commit
- `.gitignore` — Python, venv, IDE, data, secrets, notebooks
- MIT `LICENSE`
- `README.md` — twelve-phase roadmap, tech stack, getting started, and
  the "two operating modes" (free-tier local vs cloud paid) framing
- `.github/workflows/ci.yml` — lint + typecheck + test on push and PR,
  with `concurrency` cancelling superseded runs
- `src/rides_telemetry/` package skeleton with a `__version__` string
- `tests/test_smoke.py` — a single test that imports the package and
  asserts the version is set; enough to prove CI wiring works before
  there's any real code

## Design decisions

- **Same conventions as `member-lakehouse`.** The portfolio has a
  consistent identity: same Python version, same ruff config, same
  pre-commit hooks, same CI shape. A reader who has seen one repo can
  read the next one without re-orienting.

- **No streaming or Spark deps in the default install.** The `dev`
  extra pulls in just enough to run linting and the smoke test. Actual
  streaming and Spark dependencies live in `producer`, `spark`, and
  `aws` extras and only get installed when a phase needs them. This
  keeps CI fast and cheap in the early phases.

- **No Docker / compose yet.** Deliberately deferred to phase 2 when
  the first Kafka broker shows up. Adding infra plumbing before there's
  a use case for it is a common scaffolding trap.

- **Package layout under `src/`.** Prevents accidental imports from the
  repo root during tests and matches the `member-lakehouse` layout.

## How to run

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
pytest
```

Expected: pytest reports `1 passed`.

## Gotchas

- Global `~/.gitconfig` may point at a work email. This repo's local
  `.git/config` overrides `user.email` to `dirisala.bapuji@gmail.com`
  — verify with `git config --show-origin user.email` after cloning
  on a new machine.

## References

- Parent repo layout: [member-lakehouse Phase 0](https://github.com/bapuji-dirisala/member-lakehouse)
- Hatchling build backend: <https://hatch.pypa.io/latest/config/build/>
- Ruff configuration: <https://docs.astral.sh/ruff/configuration/>
