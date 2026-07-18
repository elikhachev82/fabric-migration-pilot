# Project 3: Microsoft Fabric Migration Pilot

## What was actually built
1. **`notebooks/lakehouse_ingest.py`** — a PySpark notebook (the native
   Fabric authoring experience) that ingests the inventory domain into a
   Fabric Lakehouse Delta table, replacing one existing ADF pipeline. It
   deliberately mirrors the same transformation logic validated in Project 1
   (`fct_inventory`), so the underlying business rules are proven correct —
   only the execution engine and destination changed. **This notebook has
   been run end-to-end in a live Fabric workspace** (see "Real run results"
   below), not just written and reasoned through.
2. **Data-quality gate** built into the notebook itself (quarantine bad
   rows to a separate Delta table instead of silently loading them, or
   silently failing the whole run). Verified against a sample dataset with
   deliberately injected bad rows (missing SKU, missing warehouse, negative
   quantity) — all 20 were correctly caught and quarantined, none silently
   loaded.
3. **A Direct Lake Power BI report** (`Inventory Overview`) built directly
   on top of the Lakehouse's `fct_inventory` Delta table — no Import or
   DirectQuery mode was offered or selected; Fabric's semantic model wizard
   only exposes Direct Lake for a Lakehouse-backed model. Confirms the
   "no separate refresh cycle" architecture claim below is real, not
   theoretical.
4. **A benchmark comparison** (`benchmark/benchmark_template.csv`) — the
   structure you'd fill in with real numbers after running both pipelines
   side by side for the same production-scale load. Still a template (see
   "Honest scope").

## Architecture: before vs. after
```
BEFORE (current state)
  Source systems → Azure Data Factory (copy + mapping data flow) → Azure SQL / ADLS → Power BI (Import)

PILOT (Fabric)
  Source systems → Fabric Lakehouse (Files landing zone)
                  → PySpark notebook (schema enforcement + DQ gate + transform)
                  → Managed Delta table
                  → Power BI (Direct Lake mode -- no separate import/refresh step)
```

The most interesting architectural change to be ready to discuss: **Direct
Lake mode**. Power BI can query Fabric Lakehouse Delta tables directly
without an import/refresh cycle, which is a genuinely different mental model
from the "extract → model → import → scheduled refresh" pattern the current
Power BI/ADF setup relies on.

## Real run results
Run against a personal Fabric environment (Azure Fabric F2 capacity — the
built-in 60-day Fabric trial couldn't be activated because the Entra tenant
was too new, so an F2 capacity was purchased directly against Azure credit
instead; paused between sessions to control cost) using a synthetic but
realistic 500-row inventory sample (`sample_data/inventory/inventory_sample.csv`)
with 20 rows deliberately seeded as bad data:

- 500 rows read from `Files/raw/inventory/inventory_sample.csv`
- 20 rows quarantined (missing SKU, missing warehouse, negative on-hand
  quantity) — 0 silently loaded, 0 silently dropped
- 480 rows loaded into `nikkiso_lakehouse.fct_inventory`
- Notebook wall-clock time: ~15 seconds (small sample size — not
  comparable to the production runtime figures below)
- Direct Lake semantic model + Power BI report built on `fct_inventory`
  directly; no import step, no refresh schedule configured or possible —
  confirmed 48 of 480 rows (10%) below reorder point live from the report

## Honest scope
- The transformation logic mirrors the dbt logic already validated with
  real test output in Project 1, and has now also been run for real in a
  live Fabric notebook (see above) — not just written and reasoned through.
- What's *not* yet real: a true side-by-side benchmark at production scale.
  The 500-row personal-trial run proves the mechanics (schema enforcement,
  DQ gate, Delta write, Direct Lake read) work correctly, but says nothing
  about runtime or cost at the ~10K-row production volume the ADF baseline
  reflects. `benchmark_template.csv` remains a template with realistic
  placeholder numbers for that reason — frame it as "here's how I'd
  structure the comparison" and be upfront that the small-scale personal
  run and the production-scale benchmark are two different things, unless
  you run an actual production-volume comparison before the interview.

