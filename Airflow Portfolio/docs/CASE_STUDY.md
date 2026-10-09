# Portfolio case study: Aurora Café Analytics

## Project overview

Designed a fictional retail ETL project to demonstrate Airflow orchestration patterns developed through practical work: daily entity synchronization, operator-triggered backfills, single-day replays, orders-only reloads, and rolling employee-shift synchronization.

The business narrative is fictional. The implementation uses synthetic point-of-sale data and a local warehouse. It does not claim that this café is a real client or that the demo achieved measured production results.

## Problem and approach

A café reporting team needs consistent sales and paid-hours data. Scheduled runs load completed business dates, while manual workflows recover missing history without replaying everything. A rolling shift window revisits recent dates to illustrate how late clock-outs can be incorporated by upsert.

The project separates orchestration from extraction and transformation. DAG files declare schedules and dependencies; reusable callables handle task context; the pipeline module manages UTC boundaries, paginated records, order-line expansion, completed-shift validation, and stable writes.

## Skills demonstrated

| Area | Evidence |
| --- | --- |
| Orchestration | Four DAGs, retries, timeouts, dependencies, and branch joins |
| Backfill operations | Inclusive date parameters, entity selection, and bounded chunk windows |
| Time handling | America/Edmonton business dates and daylight-saving-aware UTC conversion |
| Data engineering | Paginated extraction, nested order-line transforms, stable IDs and upserts |
| Workforce reporting | Open-shift exclusion, demo employee validation, paid-minute calculation |
| Configuration hygiene | Placeholder-only configuration and generated-file exclusions |
| Verification | Repeat-run, date-window, validation, and dry-run tests |

## Design improvements in the reconstruction

The demo uses logical run boundaries rather than wall-clock time for scheduled work. It uses exclusive midnight end boundaries rather than 23:59 so records in the last minute are not omitted. Errors propagate to the scheduler rather than being returned as successful task outcomes. Order-line identity includes the parent order to avoid collisions across orders.

## Outcome and limitations

The offline demonstration produces repeatable sales and staffing records from fictional inputs and can be reviewed without external access. API integrations, real notification delivery, distributed execution, and production volume testing are outside the implemented scope. No throughput, cost savings, or reliability percentages are invented.

## Suggested portfolio description

“Built a fictional Airflow analytics pipeline inspired by practical point-of-sale integration work. Implemented daily scheduling, selective date-range backfills, order-line transformation, completed-shift processing, and idempotent warehouse writes. Created a credential-free offline demo with synthetic data and automated checks for repeat runs, date boundaries, and daylight-saving behavior.”
