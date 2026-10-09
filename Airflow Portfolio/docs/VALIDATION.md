# Validation record

Verified on October 9, 2026:

- Six pipeline unit tests passed: inclusive and contiguous date windows; 23-hour spring and 25-hour fall daylight-saving days; stable record counts on replay; unique order-line identities; dry runs with no database creation; invalid ranges and entities; completed-shift paid time and unknown employee checks; logical-boundary date selection.
- The complete offline demo ran twice. Table counts stayed at 1 customer, 1 loyalty account, 1 category, 1 product, 3 payments, 3 order lines, and 3 completed shifts.
- SQL queries returned CAD 11.00 and 7.5 paid hours for each synthetic day.
- All Python files passed syntax parsing.
- Generated portfolio text was checked against potentially sensitive literals in the inspected source files without exposing their values. No matching literals or common private-key/token patterns were found. The implementation was written afresh; source files and environment files were not copied.

## Runtime limitation

Airflow is not installed in the preparation environment. The actual DagBag import test was skipped, and the DAGs were not executed in an Airflow scheduler. The optional test and constrained installation instructions are included so that DAG compatibility can be checked in the documented environment. Pipeline tests do not establish scheduler or live API compatibility.

## Reviewer commands

```bash
python3 -m unittest discover -s tests -v
python3 scripts/run_demo.py
python3 scripts/run_demo.py
```

Generated databases and bytecode are excluded from the delivered project. The source demo regenerates its own fictional data.
