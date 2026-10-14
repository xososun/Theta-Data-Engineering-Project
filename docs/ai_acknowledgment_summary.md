# AI Acknowledgment Summary

Prepared for inclusion in the Final Technical Report (Section 14), per the
course's Academic Integrity and Responsible Use of AI policy. This section
applies project-wide; Member A's (Santos) workstream also has a detailed
standalone record (`docs/ai_acknowledgment_member_a.md`) that this summary
draws from and is consistent with.

**Status: this is a template. The Member A section is complete and matches
the repository as of 2026-10-14 (apart from the one item marked for Santos
in the detailed record). The Member B (Hermoso) and Member C (Leyte)
sections must be filled in by those members before this document is
submitted** — do not submit with placeholder text still present.

---

## Statement of AI Tool Use

This project made use of Claude (Anthropic) as a coding and design
assistant across multiple workstreams: source data profiling, canonical
schema and source-adapter design, PostgreSQL schema design, data
ingestion and refresh detection, [transformation/validation — Member B to
confirm], and [orchestration/diagrams/analytics — Member C to confirm].

Consistent with the course policy, AI assistance does not remove
individual responsibility for understanding, verifying, and being able to
defend all submitted work. Each member's section below states what was
AI-assisted, what was independently verified and how, and what remains a
human decision the AI did not make.

## Principles Followed Across All Workstreams

- **AI output was treated as a draft, not a deliverable.** Code, config,
  and documentation produced with AI assistance were run, tested, and
  checked against real data or a real environment before being considered
  finished, not accepted on the basis that it looked correct.
- **Failures and corrections are documented, not hidden.** Where an
  AI-proposed approach was wrong or incomplete, the project's
  documentation records what went wrong and how it was actually fixed,
  rather than presenting only the final, working state as if it were the
  first attempt.
- **Source-of-truth artifacts (the canonical schema, adapter configs, DDL)
  were treated as decisions team members made and reviewed**, not
  accepted from the AI without evaluation — profiling findings that
  informed them were produced by running real code against real data.
- No fabricated data, results, or test outcomes appear in this project.
  Where something has not been verified, the project's documentation
  (`README.md`, `TASKS.md`) says so explicitly.

---

## Member A (Santos) — Ingestion & Environment

**Full detail:** `docs/ai_acknowledgment_member_a.md`

AI-assisted: source profiling script, the three source extractors
(Chicago, NYC, UK), shared ingestion utilities, the Docker/Airflow
environment, the associated tests, and the first drafts of the adapters,
canonical schema, DDL and data dictionary, which the team then reviewed and
decided. Refresh detection (SHA-256 comparison and `source_freshness`) was
added in the instructor-feedback round; the detailed record states how AI
was involved in it.

Independently verified: every extractor was run against real, full-scale
or live data (not just reviewed) — 1,096,581 real Chicago rows, 513,801
real UK rows across 5 annual files, and a live call to the real NYC
Socrata API (1,793 rows for a 7-day window). Error-handling paths were
tested against deliberately bad input (missing/malformed files, missing
years, ambiguous filenames). Batch IDs were confirmed collision-safe by
actually re-running extractors. Chicago and the UK now reuse the previous
batch when a file's SHA-256 is unchanged, and NYC writes a new batch each
run; this behaviour is covered by tests in `tests/test_freshness.py`.

Notable issues found and fixed: one AI-proposed fix (a Docker entrypoint
workaround) was found, through testing, not to have addressed the real
cause of a bug; the actual root cause was identified and fixed afterward.
Separately, reading the DAG against the NYC extractor showed that a
partial NYC pull would not have failed the `extract` task; the task now
raises when the manifest says the pull was not complete. See the linked
document for the full account.

## Member B (Hermoso) — Transformation, Validation & Database

*[To be completed by Member B.]*

AI-assisted: *[list what was AI-generated or AI-assisted — e.g.
transformation code implementing the adapter YAML mappings, data-quality
check implementations, Parquet partitioning code, etc.]*

Independently verified: *[state specifically what was tested against real
data/a real database, and how — e.g. "ran the transformation pipeline
against the real raw-layer batches Member A produced and confirmed N rows
loaded into fact_crash with 0 constraint violations" rather than "it
compiled."]*

Notable issues found and fixed during verification: *[if any AI-proposed
approach turned out to be wrong, incomplete, or needed correction after
testing, describe it honestly here, the same way Member A's section does
— this is expected, not something to omit.]*

## Member C (Leyte) — Orchestration, Diagrams & Analytics

*[To be completed by Member C.]*

AI-assisted: *[list what was AI-generated or AI-assisted — e.g. the
Airflow DAG, architecture/lineage/ERD diagrams, the data contract
document, EDA/clustering notebooks, etc.]*

Independently verified: *[state specifically what was tested — e.g. "ran
the DAG in the real Airflow scheduler and confirmed all tasks completed
with the expected dependencies/retries" or, if diagrams, "diagrams were
checked against the actual implemented schema/pipeline, not an idealized
version."]*

Notable issues found and fixed during verification: *[same as above —
document real corrections, not just final state.]*

---

## Submission Checklist for This Document

- [ ] Member A has resolved the two bracketed items in
      `docs/ai_acknowledgment_member_a.md` (freshness code; unchanged-file
      re-run)
- [ ] Member B has replaced the bracketed placeholders above with real,
      specific detail
- [ ] Member C has replaced the bracketed placeholders above with real,
      specific detail
- [ ] No bracketed placeholder text remains anywhere in this document
- [ ] Both files are committed under `docs/`, and `README.md` has an AI
      acknowledgement section that matches them
- [ ] Each member has read the other two sections, consistent with the
      course's individual-accountability requirement for the technical
      defense
