# UNIPI Data Mining Project

## Overview
This repository contains our work for the Data Mining course project focused on cleaning and analysing a music metadata collection. Early exploration, discussions with the professor, and manual audits revealed numerous malformed values (IDs, dates, coordinates, booleans, free text), so the current milestone concentrates on building reliable column-level validators before moving to modelling.

## Repository Structure
- `column_checkers.py`: Library of reusable validation helpers (gender, dates, IDs, geography, booleans). Each function is designed to mutate a `pandas.DataFrame` in place after enforcing domain rules.
- `utils.py`: Shared utilities for summarising data quality issues and the future home of the orchestrator class that will execute arrays of checkers.
- `dataset/`: Placeholder for the raw dataset provided by the professor (not versioned—drop your local copy here).
- `01_data_understangind.ipynb` & `02_data_preparation.ipynb`: Example notebooks supplied by the professor; they do not target this dataset and are kept for reference only.
- `data_mining_project.pdf`: Official assignment brief.

## Setup
1. Create a virtual environment and activate it:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
2. Install the working dependencies:
   ```bash
   python -m pip install --upgrade pip pandas numpy scipy
   ```
3. Place the latest dataset dump inside `dataset/`. Large files should remain untracked.

## Data Quality Workflow
1. Load the target DataFrame (artists or tracks) in a notebook or script.
2. Compose the validation pipeline by partially configuring the generic checkers:
   ```python
   from functools import partial
   from column_checkers import check_ids, check_date, check_gender_df

   artist_checks = [
       partial(check_ids, column="id_author"),
       partial(check_date, column="birth_date"),
       partial(check_gender_df, column="gender"),
   ]
   ```
3. The forthcoming `DataQuality` orchestrator in `utils.py` will accept an ordered array of such callables and apply them sequentially. Until it is finalised, iterate through the list manually and inspect the resulting frames or the report dictionary produced by helper functions in `utils.py`.

## Contribution Notes
- Keep new checkers deterministic, vectorised, and column-agnostic.
- Document any assumptions (e.g., valid value sets, date ranges) and align them with the professor before enforcing strict drops.
- When opening pull requests, describe the affected columns, share row counts before/after cleaning, and note the dataset version you used.

## Next Steps
- Finish wiring the `DataQuality` class to execute checker arrays and produce a consolidated report.
- Add automated tests (preferably `pytest`) covering typical and edge-case records for each checker.
- Extend validation coverage to the remaining features and integrate the cleaned dataset into downstream modelling notebooks.
