# File-Format Benchmark: CSV vs JSON vs Parquet

**Sample:** 8,924 rows from the curated `fact_crash` layer (source=chicago_us, year=2024, month=3).

Each format was written from the same in-memory DataFrame, then read back.

## Size

| Format | Size | Relative to Parquet |
|---|---|---|
| CSV | 4.98 MB | 2.03x |
| JSON | 9.30 MB | 3.79x |
| PARQUET | 2.45 MB | 1.00x |

## Write / Read Time

| Format | Write (s) | Read (s) |
|---|---|---|
| CSV | 0.399 | 0.142 |
| JSON | 0.465 | 0.287 |
| PARQUET | 0.067 | 0.012 |

## Interpretation

- **Parquet** is smallest (columnar layout + compression), fastest to write, and fastest to read back on this sample. Its columnar format lets readers fetch only the columns an analysis needs, and it stores data in a compact binary layout instead of text, so both disk and parse costs drop.
- **CSV** is mid-size and reasonably fast to parse because pandas' C parser is highly optimized, but it carries no type information, uses more bytes per value, and cannot skip columns that an analysis doesn't need.
- **JSON** is the largest here (keys repeat on every record) and mid-speed. It is the format our raw API responses arrive in, not the format we store curated data in.

**Conclusion:** Parquet is the correct storage format for the curated and partitioned layers because (a) it is smallest, (b) it writes fastest, (c) it preserves schema and types, and (d) it supports partition pruning and column projection, which CSV and JSON cannot. CSV remains only as the interchange format of the original sources; JSON remains only as the raw API format.