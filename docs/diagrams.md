# Diagrams

Architecture, data-flow/lineage, and ERD diagrams for the cross-city road
crash data pipeline. Each diagram is authored as Mermaid source under
`docs/diagrams/` so it diffs cleanly in version control and can be
regenerated or exported to PNG/SVG for the final paper.

Rendered versions appear inline below via VS Code's Markdown Preview
Mermaid Support extension, and on GitHub when this file is viewed there.

---

## 1. Architecture

**Source:** [`diagrams/architecture.mmd`](diagrams/architecture.mmd)

Covers: sources → ingestion → raw → staging → validation → harmonization
→ curated gate → published Parquet → load → PostgreSQL → consumption, with Airflow as the
orchestration layer, Docker Compose as the runtime, YAML as config, and
Git as version control. Each box names the module that implements it.

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 36, "padding": 8}}}%%
flowchart TB

    %% ═════════ ROW 1 · INGEST ═════════
    subgraph R1["① INGEST · Sources → Extract → RAW  (extract logs source_freshness → PostgreSQL)"]
        direction LR

        S1["<b>Chicago Crashes</b><br/>bulk CSV<br/>(Socrata export)"]
        S2["<b>NYC Collisions</b><br/>Socrata REST API<br/>→ JSON"]
        S3["<b>UK STATS19</b><br/>5 annual CSVs<br/>2021–2025"]

        A1["<b>chicago_extractor</b><br/>• newest export in data/incoming/<br/>• byte-for-byte copy<br/>• UTF-8 + header check<br/>• SHA-256 = last batch → reuse"]
        A2["<b>nyc_extractor</b><br/>• retries + backoff<br/>• compound pagination<br/>• 90-day window to newest date<br/>• 0 rows = failure"]
        A3["<b>uk_extractor</b><br/>• 5 annual files<br/>• missing year fails run<br/>• UTF-8 + header check<br/>• SHA-256 = last batch → reuse"]

        RAW["<b>RAW</b> · never overwritten<br/>data/raw/source_id/batch_id/<br/>original files + manifest.json<br/>(retrieved_at_utc, sha256)"]

        S1 --> A1
        S2 --> A2
        S3 --> A3
        A1 --> RAW
        A2 --> RAW
        A3 --> RAW
    end

    %% ═════════ ROW 2 · TRANSFORM + GATES ═════════
    subgraph R2["② STAGE · VALIDATE · HARMONIZE"]
        direction LR

        STG["<b>STAGING</b> · per source<br/>adapter field_mapping<br/>(from · literal · map · derive_fn)<br/>→ canonical columns<br/>null sentinels -1, (0,0) → null"]

        G1{{"<b>GATE 1 · per batch</b><br/>8 checks: schema · nullability<br/>uniqueness · accepted values · range<br/>date logic · referential · row count<br/>results → dq_run_log<br/>any fail stops run"}}

        HARM["<b>HARMONIZE</b> · all sources<br/>strict dates · local → UTC<br/>canonical types · crash_date_key<br/>DST-ambiguous excluded<br/>writes data/curated/_candidate/"]

        G2{{"<b>GATE 2 · candidate</b><br/>5 checks: schema &amp; types<br/>unique keys across sources<br/>per-source row reconciliation<br/>timestamps &amp; date keys · partitions<br/>results → dq_run_log<br/>any fail: nothing published"}}

        STG --> G1
        G1 ==>|"all sources pass"| HARM
        HARM --> G2
    end

    %% ═════════ ROW 3 · PUBLISH + SERVE ═════════
    subgraph R3["③ PUBLISH · LOAD · SERVE"]
        direction LR

        CUR[["<b>CURATED</b> · 25 fields<br/>candidate swapped in<br/>partitioned Parquet<br/>data/curated/fact_crash/<br/>source_id= / year= / month="]]

        LOAD["<b>LOAD</b><br/>UPSERT on<br/>(source_id, source_record_id)<br/>dim_date first,<br/>then fact_crash"]

        DB[("<b>PostgreSQL</b><br/>fact_crash · dim_source<br/>dim_date · dq_run_log")]

        RPT["<b>quality_report.py</b><br/>data/quality_reports/*.md"]
        K1["SQL queries<br/>(analyst)"]
        K2["EDA notebook<br/>temporal · severity"]
        K3["Clustering notebook<br/>DBSCAN hotspots"]

        CUR --> LOAD --> DB
        DB --> RPT
        DB --> K1
        DB --> K2
        CUR --> K2
        CUR --> K3
    end

    %% ═════════ ROW 4 · RUNTIME ═════════
    subgraph R4["④ ORCHESTRATION &amp; RUNTIME"]
        direction LR
        OR1["<b>Apache Airflow</b> · DAG crash_pipeline<br/>weekly · 2 retries<br/>per source (mapped ×3): extract → stage → validate<br/>then once: harmonize → validate_curated<br/>→ publish → load → quality_report (ALL_DONE)"]
        OR2["<b>Docker Compose</b><br/>postgres · airflow-webserver<br/>airflow-scheduler · pipeline"]
        OR3["<b>YAML configs</b><br/>adapters · canonical schema"]
        OR4["<b>Git / GitHub</b><br/>version control"]
    end

    %% Row-to-row flow (subgraph-level links keep each row horizontal)
    R1 ==>|"RAW batches"| R2
    R2 ==>|"Gate 2 passes → publish"| R3
    R3 -.- R4

    %% ═════════ STYLES ═════════
    classDef source  fill:#e3f2fd,stroke:#1565c0,color:#0d47a1
    classDef extract fill:#fff8e1,stroke:#f57f17,color:#5d4037
    classDef raw     fill:#f3e5f5,stroke:#6a1b9a,color:#4a148c
    classDef stage   fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20
    classDef gate    fill:#ffebee,stroke:#c62828,stroke-width:3px,color:#b71c1c
    classDef curated fill:#e0f7fa,stroke:#00838f,color:#006064
    classDef store   fill:#ede7f6,stroke:#4527a0,color:#311b92
    classDef consume fill:#fce4ec,stroke:#ad1457,color:#880e4f
    classDef runtime fill:#eceff1,stroke:#455a64,stroke-dasharray:4 2,color:#263238

    class S1,S2,S3 source
    class A1,A2,A3 extract
    class RAW raw
    class STG stage
    class G1,G2 gate
    class HARM,CUR curated
    class LOAD,DB,RPT store
    class K1,K2,K3 consume
    class OR1,OR2,OR3,OR4 runtime

    style R1 fill:#fafafa,stroke:#9e9e9e
    style R2 fill:#fafafa,stroke:#9e9e9e
    style R3 fill:#fafafa,stroke:#9e9e9e
    style R4 fill:#fafafa,stroke:#9e9e9e,stroke-dasharray:4 2
```

**Reading this diagram:** Solid arrows are data flow; dashed arrows are
orchestration/runtime relationships (Airflow *schedules* the stages, it
is not itself a data stage).

- **File checks and freshness live in ingestion.** The Chicago and UK
  extractors confirm each file is readable UTF-8 with a header row, and
  compare its SHA-256 with the last raw batch: an unchanged file is not
  copied again and the previous batch is reused. NYC treats an empty API
  result as a failure. Each source writes a `source_freshness` row to
  `dq_run_log`, so every run records whether new data arrived.
- **Staging maps, validation gates.** `staging.py` applies each
  adapter's `field_mapping` and nulls sentinel values, leaving values as
  source-faithful text. `checks.py` then runs 8 checks per batch and
  writes every result to `dq_run_log`. Any `fail` stops the run before
  harmonization, so no partial data reaches curated or Postgres.
- **Harmonization runs once, across all sources, and is audited before
  anyone sees it.** `curated.py` reads the batches validated in this
  run, converts local time to UTC, casts types and writes a *candidate*.
  `curated_checks.py` then runs 5 checks on the combined result (schema
  and types, unique keys across sources, per-source row reconciliation,
  timestamps and date keys, partitions). Only if all pass is the
  candidate published to `data/curated/fact_crash/`; otherwise nothing
  is published or loaded (write-audit-publish). Date formats and types
  are set in code, not read from the YAML configs.
- **Postgres is loaded from the curated Parquet**, by an UPSERT on the
  natural key, so reruns update rows rather than duplicate them. The
  Parquet layer holds only the latest batch per source; Postgres keeps
  every batch ever loaded.
- **The quality report** summarizes the run's `dq_run_log` rows and runs
  even when an earlier stage failed.

---

## 2. Data Flow / Lineage

**Source:** [`diagrams/lineage.mmd`](diagrams/lineage.mmd)

Traces each canonical `fact_crash` field back to its source column(s)
across `chicago_us`, `nyc_us`, and `uk_stats19` (including UK `city`,
derived from the police force: "London" for Greater London), plus the
`batch_id` → raw batch folder → `manifest.json` traceability chain.
Structural nulls (fields a source cannot populate) are shown as dashed
paths, matching the gap matrix in `docs/data_dictionary.md`.

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 20, "rankSpacing": 30, "padding": 8}}}%%
flowchart TB

    %% ═════════ HOW TO READ ═════════
    subgraph R0[" "]
        direction TB
        LEG["<b>Column lineage → fact_crash (canonical curated table)</b><br/>Each card: source fields (left) → canonical fields (right)<br/>✗ = source cannot populate this field · red dashed box = source has no field in this card<br/>Colors: <b>Chicago</b> blue · <b>NYC</b> amber · <b>UK STATS19</b> green"]
    end

    %% ═════════ ROW 1 · IDENTITY + TRACEABILITY ═════════
    subgraph R1[" "]
        direction TB

        subgraph G1["Identity"]
            direction LR
            I1["<b>Chicago</b><br/>CRASH_RECORD_ID"]
            I2["<b>NYC</b><br/>collision_id"]
            I3["<b>UK</b><br/>collision_index"]
            IC["<b>canonical</b><br/>crash_id = {source_id}:{source_record_id}<br/>source_record_id"]
            I1 --> IC
            I2 --> IC
            I3 --> IC
        end

        subgraph G2["Row-level traceability"]
            direction TB
            L1["<b>batch_id</b><br/>every fact_crash row carries<br/>batch_id + source_record_id"]
            L2["<b>data/raw/{source_id}/{batch_id}/</b><br/>original files<br/>record found by source_record_id"]
            L3["<b>manifest.json</b><br/>retrieved_at_utc · file list<br/>sha256 per file"]
            L1 --> L2 --> L3
        end
    end

    %% ═════════ ROW 2 · TIME + LOCATION ═════════
    subgraph R2[" "]
        direction TB

        subgraph G3["Time"]
            direction LR
            T1["<b>Chicago</b><br/>crash time: CRASH_DATE<br/>police notified: DATE_POLICE_NOTIFIED"]
            T2["<b>NYC</b><br/>crash time: crash_date + crash_time<br/>police notified: ✗ no field"]
            T3["<b>UK</b><br/>crash time: date + time (DD/MM/YYYY)<br/>police notified: ✗ no field"]
            TC["<b>canonical</b><br/>crash_timestamp_utc<br/>(local → UTC in curated.py)<br/>police_notified_timestamp_utc"]
            T1 --> TC
            T2 --> TC
            T3 --> TC
        end

        subgraph G4["Location"]
            direction LR
            O1["<b>Chicago</b><br/>coordinates: LATITUDE / LONGITUDE<br/>city / country: not mapped"]
            O2["<b>NYC</b><br/>coordinates: latitude / longitude<br/>city / country: not mapped"]
            O3["<b>UK</b><br/>coordinates: latitude / longitude<br/>city: police_force (1 Metropolitan,<br/>48 City of London) → uk_city() = London<br/>✗ null outside Greater London"]
            OC["<b>canonical</b><br/>latitude / longitude<br/>+ has_valid_coordinates<br/>city / country"]
            O1 --> OC
            O2 --> OC
            O3 --> OC
        end
    end

    %% ═════════ ROW 3 · SEVERITY + COUNTS ═════════
    subgraph R3[" "]
        direction TB

        subgraph G5["Severity"]
            direction LR
            V1["<b>Chicago</b><br/>severity + raw:<br/>MOST_SEVERE_INJURY"]
            V2["<b>NYC</b><br/>severity: nyc_severity() from<br/>injured + killed counts<br/>✗ cannot populate 'serious'<br/>raw: persons_injured + persons_killed<br/>(aggregate, ~88% null)"]
            V3["<b>UK</b><br/>severity + raw: collision_severity<br/>(1=fatal, 2=serious, 3=minor)"]
            VC["<b>canonical</b><br/>severity<br/>source_severity_raw"]
            V1 --> VC
            V2 --> VC
            V3 --> VC
        end

        subgraph G6["Counts"]
            direction LR
            N1["<b>Chicago</b><br/>injured: INJURIES_TOTAL<br/>killed: INJURIES_FATAL<br/>vehicles: NUM_UNITS"]
            N2["<b>NYC</b><br/>injured: number_of_persons_injured<br/>killed: Σ pedestrians + cyclist<br/>+ motorist killed<br/>vehicles: non-null count of<br/>vehicle_type_code1..5"]
            N3["<b>UK</b><br/>injured: number_of_casualties<br/>killed: ✗ no num_killed field<br/>vehicles: number_of_vehicles"]
            NC["<b>canonical</b><br/>num_injured_total<br/>num_killed<br/>num_vehicles_involved"]
            N1 --> NC
            N2 --> NC
            N3 --> NC
        end
    end

    %% ═════════ ROW 4 · DESCRIPTORS + CONDITIONS ═════════
    subgraph R4[" "]
        direction TB

        subgraph G7["Crash description"]
            direction LR
            D1["<b>Chicago</b><br/>crash_type: FIRST_CRASH_TYPE<br/>primary_cause: PRIM_CONTRIBUTORY_CAUSE<br/>hit_and_run: HIT_AND_RUN_I"]
            D2["<b>NYC</b><br/>crash_type: ✗ no field<br/>primary_cause: contributing_factor_vehicle_1<br/>hit_and_run: ✗ no field → defaults false"]
            D3["<b>UK</b><br/>crash_type: ✗ no field<br/>primary_cause: ✗ no field<br/>hit_and_run: ✗ no field → defaults false"]
            DC["<b>canonical</b><br/>crash_type<br/>primary_cause<br/>hit_and_run"]
            D1 --> DC
            D2 --> DC
            D3 --> DC
        end

        subgraph G8["Conditions"]
            direction LR
            W1["<b>Chicago</b><br/>weather: WEATHER_CONDITION<br/>lighting: LIGHTING_CONDITION<br/>speed limit: POSTED_SPEED_LIMIT"]
            W2["<b>NYC</b><br/>✗ no weather field<br/>✗ no lighting field<br/>✗ no speed_limit field"]
            W3["<b>UK</b><br/>weather: weather_conditions (-1 → null)<br/>lighting: light_conditions (-1 → null)<br/>speed limit: speed_limit (-1 → null)"]
            WC["<b>canonical</b><br/>weather_condition<br/>lighting_condition<br/>posted_speed_limit_mph"]
            W1 --> WC
            W2 -.->|"cannot populate"| WC
            W3 --> WC
        end
    end

    %% Keep rows stacked in a grid (invisible links between rows)
    R0 ~~~ R1
    R1 ~~~ R2
    R2 ~~~ R3
    R3 ~~~ R4

    %% ═════════ STYLES ═════════
    classDef chi   fill:#e3f2fd,stroke:#1565c0,color:#0d47a1
    classDef nyc   fill:#fff8e1,stroke:#f57f17,color:#5d4037
    classDef uk    fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20
    classDef gap   fill:#ffebee,stroke:#c62828,stroke-dasharray:3 2,color:#b71c1c
    classDef canon fill:#e0f7fa,stroke:#00838f,stroke-width:2px,color:#006064
    classDef lin   fill:#f3e5f5,stroke:#6a1b9a,color:#4a148c
    classDef legend fill:#fafafa,stroke:#9e9e9e,color:#263238

    class I1,T1,O1,V1,N1,D1,W1 chi
    class I2,T2,O2,V2,N2,D2 nyc
    class I3,T3,O3,V3,N3,D3,W3 uk
    class W2 gap
    class IC,TC,OC,VC,NC,DC,WC canon
    class L1,L2,L3 lin
    class LEG legend

    %% Row wrappers invisible; group cards lightly outlined
    style R0 fill:none,stroke:none
    style R1 fill:none,stroke:none
    style R2 fill:none,stroke:none
    style R3 fill:none,stroke:none
    style R4 fill:none,stroke:none
    style G1 fill:#fafafa,stroke:#9e9e9e
    style G2 fill:#fafafa,stroke:#9e9e9e
    style G3 fill:#fafafa,stroke:#9e9e9e
    style G4 fill:#fafafa,stroke:#9e9e9e
    style G5 fill:#fafafa,stroke:#9e9e9e
    style G6 fill:#fafafa,stroke:#9e9e9e
    style G7 fill:#fafafa,stroke:#9e9e9e
    style G8 fill:#fafafa,stroke:#9e9e9e
```

**Reading this diagram:** Solid arrows are real field mappings from an
adapter YAML; arrows labelled with a function name (`nyc_severity()`)
are derived in `src/transform/derivations.py`. Dashed red arrows are the
structural gaps from `docs/data_dictionary.md` — fields a source does not
collect, not mapping omissions. NYC severity is a partial gap: it is
derived from injury and death counts, so it can be `fatal`, `minor` or
`none` but never `serious`. Blue = Chicago, amber = NYC, green = UK,
cyan = canonical `fact_crash`.

The purple band is the row-level lineage chain. Every `fact_crash` row
carries the `batch_id` of the run that last wrote it, which names its
raw folder `data/raw/{source_id}/{batch_id}/`; `source_record_id`
locates the record in that batch's files, and the batch's
`manifest.json` records when it was retrieved and each file's SHA-256.
(`source_row_raw_ref` exists in the schema for a future per-row pointer
but is not populated yet.)

---

## 3. Entity-Relationship Diagram (ERD)

**Source:** [`diagrams/erd.mmd`](diagrams/erd.mmd)

Derived from `sql/schema.sql`. Covers `dim_source`, `dim_date`,
`fact_crash`, and `dq_run_log`, with foreign keys, the
`uq_fact_crash_source_natural_key` UNIQUE constraint, and CHECK
constraints shown as annotations.

```mermaid
erDiagram
    dim_source ||--o{ fact_crash : "source_id"
    dim_source ||--o{ dq_run_log : "source_id"
    dim_date   ||--o{ fact_crash : "crash_date_key"

    dim_source {
        TEXT     source_id           PK "chicago_us | nyc_us | uk_stats19"
        TEXT     provider            "NOT NULL"
        TEXT     dataset_url
        TEXT     city                "NULL for uk_stats19 (national)"
        CHAR2    country             "NOT NULL, ISO 3166-1 alpha-2"
        TEXT     source_local_timezone "NOT NULL, IANA tz"
        TEXT     license
        TIMESTAMPTZ created_at       "NOT NULL DEFAULT now()"
    }

    dim_date {
        DATE     date_key            PK
        SMALLINT year                "NOT NULL"
        SMALLINT month               "NOT NULL"
        SMALLINT day                 "NOT NULL"
        SMALLINT day_of_week         "NOT NULL, 0=Mon..6=Sun"
        BOOLEAN  is_weekend          "NOT NULL"
    }

    fact_crash {
        TEXT     crash_id            PK "source_id:source_record_id"
        TEXT     source_id           FK "NOT NULL"
        TEXT     source_record_id    "NOT NULL, natural key part"
        TIMESTAMPTZ crash_timestamp_utc "NOT NULL"
        DATE     crash_date_key      FK "NOT NULL"
        TIMESTAMPTZ police_notified_timestamp_utc "NULL for nyc_us, uk_stats19"
        TEXT     city                "NULL for uk_stats19"
        CHAR2    country             "NOT NULL"
        DOUBLE   latitude            "NULL if missing/sentinel"
        DOUBLE   longitude           "NULL if missing/sentinel"
        BOOLEAN  has_valid_coordinates "NOT NULL DEFAULT FALSE"
        TEXT     severity            "NOT NULL, CHECK in (fatal|serious|minor|none|unknown)"
        TEXT     source_severity_raw "audit trail"
        INTEGER  num_injured_total   "CHECK >= 0"
        INTEGER  num_killed          "CHECK >= 0; NULL for uk_stats19"
        INTEGER  num_vehicles_involved "CHECK >= 0"
        TEXT     crash_type          "NULL for nyc_us, uk_stats19"
        TEXT     primary_cause       "NULL for uk_stats19"
        TEXT     weather_condition   "NULL for nyc_us"
        TEXT     lighting_condition  "NULL for nyc_us"
        INTEGER  posted_speed_limit_mph "NULL for nyc_us"
        BOOLEAN  hit_and_run         "NOT NULL DEFAULT FALSE; reliable only for chicago_us"
        TIMESTAMPTZ ingested_at_utc  "NOT NULL DEFAULT now()"
        TEXT     batch_id            "NOT NULL"
        TEXT     source_row_raw_ref  "lineage pointer to raw layer"
    }

    dq_run_log {
        BIGSERIAL id                 PK
        TEXT     batch_id            "NOT NULL"
        TEXT     source_id           FK "nullable"
        TEXT     check_name          "NOT NULL"
        TEXT     status              "NOT NULL, CHECK in (pass|fail|warn)"
        BIGINT   rows_checked        "NOT NULL DEFAULT 0"
        BIGINT   rows_failed         "NOT NULL DEFAULT 0"
        TEXT     details             "free-text summary"
        TIMESTAMPTZ run_timestamp   "NOT NULL DEFAULT now()"
    }
```