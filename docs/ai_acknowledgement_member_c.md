# AI Acknowledgment — Leyte: Orchestration, Diagrams & Analytics

Per the course's Academic Integrity and Responsible Use of AI policy ("Students must be able to explain and defend all submitted code, including code produced with AI-assisted tools"), this document records where and how AI assistance (Claude, Anthropic) was used for the Orchestration, Diagrams & Analytics workstream, what it produced, and what was independently verified by the student rather than taken on faith.

*Last updated 2026-10-14 to match the repository after the instructor-feedback changes (the curated gate, the write-audit-publish order, the density-relative hotspot threshold, and the comparison-frame rule).*

## Scope of AI Assistance

DeepSeek and VS Code AI was used as a coding assistant across the project's design, orchestration and analytics phases: drafting the Airflow DAG's task graph and trigger rules, drafting the architecture, lineage and ERD diagrams as Mermaid source, drafting the data contract document, and — for this workstream specifically — writing the EDA and clustering notebooks, the comparison-frame module, and the analytics prose.

## What Was AI-Generated

Orchestration and diagrams (this workstream):

- `dags/crash_pipeline.py` — the Airflow DAG: dynamic task mapping over the three sources, the fan-in to `harmonize`, the write-audit-publish order, the retry policy, and the self-failing `quality_report`
- `tests/test_dag_imports.py` — structural tests for the DAG's tasks, dependencies, trigger rules, retries and schedule
- `docs/diagrams/architecture.mmd`, `docs/diagrams/lineage.mmd`, `docs/diagrams/erd.mmd` — the Mermaid source for the three diagrams

Analytics (summarized with AI, then reviewed and written by the student):

- `notebooks/comparison_frames.py` — the shared comparison-frame definition (city geographies, injury crashes, shared window, rate per 100,000 residents per year)
- `notebooks/clustering.ipynb` — the DBSCAN hotspot notebook, including the density-relative `min_samples_for()` threshold and the 80% subsample stability check
- the analytics prose in Sections 12 and the Appendix

Documentation drafted with AI, then reviewed (drafted because I can't remember some parts after a while):

- `docs/data_contract.md` — the producer/consumer contract for `fact_crash`

## What Was Independently Verified, Not Just Accepted

This is the part worth being explicit about for the technical defense: AI output was treated as a first draft to be tested, not a finished product.

- The DAG's task graph, dependencies, trigger rules, retries and schedule were confirmed against `tests/test_dag_imports.py`, which asserts the `harmonize → validate_curated → publish → load` order, the per-source dynamic task mapping, the `all_success` trigger rule on the curated gate, and the `ALL_DONE` trigger rule on `quality_report`.
- The write-audit-publish behaviour was confirmed from the latest quality report, not from reading the code alone: each source's `curated_reconciliation_check` line records the batch validated in that run and the exact staged = curated + excluded count — Chicago 1,096,581 = 1,096,395 + 186; UK 513,801 = 513,732 + 69; NYC 20,537 = 20,537 + 0 — and `curated_partition_check` confirms 211 partition files across three sources, matching what harmonize reported.
- The architecture and lineage diagrams were read line by line against the code they describe — `dags/crash_pipeline.py` and `src/transform/derivations.py` — not against an idealized version of the pipeline. The ERD was checked against `sql/schema.sql` for every table, key and foreign key.
- The lineage diagram draws NYC `severity` as a derived field, not a direct mapping, matching `nyc_severity()` in `src/transform/derivations.py`, which builds severity from the injury and death counts because NYC publishes no categorical severity at all.
- The analytics numbers were reproduced from the curated Parquet in `notebooks/clustering.ipynb`: in the Chicago vs London, 2025 frame (2025-01-01 to 2025-12-31, city geographies, injury crashes with valid coordinates), London 26.3% of injury crashes fall in hotspots against 8.8% in Chicago; in the Chicago vs NYC, 2026 frame (2026-03-13 to 2026-06-11), Chicago 16.5% against NYC 6.2%.
- Hotspot stability was checked empirically, not assumed: DBSCAN was re-run on 80% subsamples with three seeds per city, giving London 27.6–28.6%, Chicago 9.1–9.3% in the London frame, Chicago 16.9–17.7% in the NYC frame, and NYC 8.2–9.5%. The narrow bands are what make the cross-city shares interpretable.
- The density-relative threshold was implemented as `min_samples_for(city, n_points)` — a 150 m neighbourhood, at least 10× the city's own average injury-crash density, with a floor of 5 — and the run prints the per-city `min_samples` values it used, so the reader can see what produced each cluster set.
- The comparison-frame rule in `comparison_frames.py` (same place, same event type, same period, rate per 100,000 residents per year) is what drops raw totals and Great Britain-vs-one-city from the analytics. The notebook's Finding cell flags that the two frames give Chicago different shares (8.8% in the London frame, 16.5% in the NYC frame) and that shares are not comparable across frames.

## What This Means for the Technical Defense

Any team member should be able to explain four decisions:

- **Harmonize runs once, not per source.** It combines every source's validated batch and rewrites the whole curated layer; running it per source would have three tasks rewriting the same folder.
- **The curated gate sits after harmonization.** Staging checks see one source at a time, so they cannot catch a mistake made when the sources are combined.
- **Hotspot comparison uses shares, not counts.** The two cities have different volumes and populations, so a count would rank London higher just because it has more crashes.
- **DBSCAN, not k-means.** K-means assumes spherical clusters; road networks produce elongated hotspots and most crashes are not in hotspots.

## What Remains Open / Human-Owned

- The five curated checks and their thresholds were a judgment call made by the student after reading the instructor's feedback, not purely AI-decided — the reconciliation tolerance (0.1%, minimum 25 rows) and the DST-exclusion handling were reviewed and confirmed against what the data actually showed.
- The choice of `eps = 150 m` and `DENSITY_RATIO = 10` for hotspot clustering is a stated simplification to defend if asked, not a hidden assumption. The 80% subsample check is what supports presenting the resulting shares.
- The comparison frames themselves — which cities to compare, over which shared window, at what geography — were decisions made by the student, not the AI. `comparison_frames.py` implements the rule; the rule was chosen.
- The data contract is a producer/consumer agreement the student wrote and the team reviewed. Where the contract and the code disagree, the code is wrong and must be fixed — that is a commitment the student owns, not the AI.
