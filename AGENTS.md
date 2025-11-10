# Repository Guidelines

## Project Structure & Module Organization
- `column_checkers.py` hosts reusable column validators; keep them idempotent and accept the target column as an explicit argument so they work for both artists and tracks.
- `utils.py` groups shared quality-report helpers and the upcoming orchestrator that will execute checker arrays—consolidate cross-project utilities here.
- `dataset/` stores raw inputs (untracked); note the dataset snapshot in your PR. The `01_*.ipynb` and `02_*.ipynb` notebooks are professor demos and should stay unchanged.

## Build, Test, and Development Commands
- Create a virtual environment: `python -m venv .venv && source .venv/bin/activate`.
- Install essentials: `python -m pip install --upgrade pip pandas numpy scipy`.
- Run targeted experiments in an interactive shell (`python -i`) or add a guarded `main` block in `utils.py` for scripted runs as the orchestrator stabilises.

## Coding Style & Naming Conventions
- Adhere to PEP 8 (4-space indent, snake_case identifiers, lowercase module names); keep function names action-oriented (`check_longitude`, `check_valid_string`).
- Document side effects in concise docstrings and list expected column types; avoid silent DataFrame mutations beyond the documented scope.
- Order imports as standard library, third-party, local; prefer vectorised Pandas operations and deterministic behaviour across repeated runs.

## Testing Guidelines
- Use `pytest` in a `tests/` folder; name modules `test_<subject>.py` and build minimal DataFrame fixtures highlighting both valid and failing paths.
- Assert downstream effects such as cleaned values, warnings, and row counts before wiring a checker into shared arrays.
- Track coverage (aim ≥80%) and flag intentional exclusions inside the affected test file.

## Commit & Pull Request Guidelines
- Match the repository’s concise, imperative commit subjects (`Add utils`, `first checks added`) and group related checker changes together.
- Describe affected columns, datasets, and validation outcomes in the PR body; attach sample commands or notebook cells for reviewers.
- Provide before/after metrics when rules drop or correct records and link to professor feedback or issue references when available.

## Agent Workflow Tips
- Use `functools.partial` to preconfigure reusable validators (e.g., `partial(check_ids, column="id_track")`) before passing them to the orchestrator.
- Reset or copy DataFrames when chaining multiple agents to avoid compounding mutations, and align on a consistent warning/logging strategy across notebooks.
