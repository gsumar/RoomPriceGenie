# 🏨 RoomPriceGenie

> Coding Challenge for a Data Engineer.

---

## 🚀 How to Run the Pipeline

All commands must be run from the project root. `PYTHONPATH=src` is required so Python can resolve the internal packages.

### Default run
Uses hotel `1035`, dates `2026-05-01 → 2026-05-31`, output to `reports/`:

```bash
cd /Users/gorka/Documents/workspace/RoomPriceGenie
PYTHONPATH=src python src/HotelReservationKPIPipeline.py
```

### Custom hotel & date range
Positional arguments: `hotel_id`, `from_date`, `to_date`

```bash
cd /Users/gorka/Documents/workspace/RoomPriceGenie
PYTHONPATH=src python src/HotelReservationKPIPipeline.py 1035 2026-05-01 2026-05-31
```

### Custom output directory
Use `--output-dir` to write the CSV to a different folder (default: `reports/`):

```bash
cd /Users/gorka/Documents/workspace/RoomPriceGenie
PYTHONPATH=src python src/HotelReservationKPIPipeline.py 1035 2026-05-01 2026-05-31 --output-dir reports
```

---

## 🧠 Architecture & Design Decisions

### Overview

The entire pipeline is orchestrated by a single entry point: `src/HotelReservationKPIPipeline.py`. When executed, it ingests data from all configured sources, passes it through each layer of the **Medallion architecture** (Bronze → Silver → Gold), and finally produces a consumer-ready CSV report.

This design was chosen because it provides a clean separation of concerns at every stage, makes each layer independently testable, and mirrors how production-grade data platforms (e.g. Databricks, Delta Lake) are structured.

```
External Systems
      │
      ▼
 [Bronze Layer]  ← raw ingestion, no transformation
      │
      ▼
 [Silver Layer]  ← schema validation, deduplication, flattening
      │
      ▼
  [Gold Layer]   ← business KPIs, aggregation
      │
      ▼
  [Reporting]    ← filtered, zero-filled CSV export
```

---

### 🔌 Ingestion — Designed for Scalability

The ingestion layer (`src/datalake/bronze/connectors`) was built under the assumption that **this pipeline will eventually connect to multiple systems** — REST APIs, relational databases, or cloud storage paths like S3. Each connector is isolated and independently replaceable.

Two abstract base classes define the ingestion contract:

- **`OdysseyConnector`** — for API-style sources (e.g. Odyssey reservation system)
- **`DbConnector`** — for database or file-based sources (e.g. CSV from a DB export)

Both expose a single `load(path) → DataFrame` method and are callable as objects. Adding a new source (e.g. an S3-backed Parquet file, a REST API, or a new database table) requires only implementing the abstract `load()` method — no changes to the pipeline orchestrator needed.

> **Design decision:** keeping connectors thin and stateless means they can be swapped, mocked in tests, or run in parallel without side effects.

---

### 🥉 Bronze Layer — Raw, Untouched, Auditable

The Bronze layer stores data **exactly as it arrives** from the source system — no filtering, no transformations, no discarding of bad records. For reservations, the full JSON payload (including malformed or contract-violating entries) is loaded into a flat DataFrame via `ReservationConnector`. For inventory, the raw CSV rows are passed through untouched via `HotelRoomInventoryConnector`.

**Why this matters:**
- ✅ **Auditability** — if a downstream KPI is wrong, you can always trace it back to the exact payload received
- ✅ **Reprocessability** — if the Silver schema or validation rules change, you can re-run from Bronze without re-ingesting from the source
- ✅ **No data loss** — records that are invalid today may be valid after a schema update

---

### 🥈 Silver Layer — Validated, Structured, Trustworthy

The Silver layer is the quality gate. Every record is validated against a **versioned JSON schema** stored in `schema/bronze/` and enforced by the `SchemaRegistry`. Only records that pass all checks are allowed through.

For reservations (`ReservationsCurater`), validation goes well beyond field types. An entire reservation is discarded if **any** of the following fail:

| Check | Description |
|-------|-------------|
| **Schema validation** | All required fields present, correct types, `status` matches the allowed enum (`confirmed`, `cancelled`, `checked_in`, `checked_out`) |
| **Period validity** | `departure_date` must be strictly after `arrival_date` |
| **Stay dates fit window** | Each stay date must fall within the reservation's arrival–departure period |
| **Unique nights** | No overlapping nights within the same reservation's stay dates |
| **Deduplication** | If a `reservation_id` appears multiple times, the most recent `updated_at` version wins |

Valid reservations are then **flattened** — the nested `stay_dates` array is expanded into one row per night — and **normalised into two tables**: a parent `reservations` table and a child `stay_dates` table. This relational structure is the foundation that Gold builds on.

For inventory (`HotelRoomInventoryCleaner`), only essential type coercion is applied to ensure merge compatibility downstream — no schema validation, no data filtering, and no deduplication. This reflects our assumption that inventory data is trusted at the source.

**Benefits of this approach:**
- ✅ **Fail at the boundary** — invalid data never reaches the aggregation layer
- ✅ **Schema versioning** — the `SchemaRegistry` supports multiple versions (`v1`, `v2`, ...) per source, enabling schema evolution without breaking existing pipelines
- ✅ **Separation of concerns** — validation logic lives in Silver, not scattered across the pipeline
- ✅ **Normalised output** — the parent/child split avoids denormalisation issues in downstream joins

#### Silver Layer Data Model

The validated data is normalized into two related tables:

**📋 reservations** (parent)
```
hotel_id         string [FK → inventory]
reservation_id   string [PK]
status           string (confirmed | cancelled | checked_in | checked_out)
arrival_date     date
departure_date   date
created_at       datetime
updated_at       datetime
```

**📅 stay_dates** (child)
```
hotel_id                    string [FK → inventory]
reservation_id              string [FK → reservations]
start_date                  date
end_date                    date
room_type_id                string [FK → inventory]
room_type_name              string
room_revenue_gross_amount   float
room_revenue_net_amount     float
fnb_gross_amount            float
fnb_net_amount              float
```

**🏨 inventory** (reference)
```
hotel_id      string [PK]
room_type_id  string [PK]
quantity      int
```

---

### 🥇 Gold Layer — Business-Ready KPIs

By the time data reaches Gold, all invalid entries have already been removed in Silver. The Gold layer (`PerformanceKPIs`) focuses purely on **business logic**: joining reservations, stay dates, and inventory to compute nightly hotel KPIs.

Key business rules applied here:
- **Occupancy** is computed using only **non-cancelled** reservations — a cancelled booking does not occupy a room
- **Revenue** (`TOTAL_NET_REVENUE`) includes **all** reservation statuses, since revenue may still be owed or recognised for cancelled bookings
- **ADR** (Average Daily Rate) is revenue per occupied room, rounded to the nearest integer
- Each stay date is **expanded night by night** using `pd.date_range`, so multi-night stays contribute correctly to each `NIGHT_OF_STAY`

The result is a clean, aggregated DataFrame keyed by `hotel_id + NIGHT_OF_STAY`, ready for any downstream consumer.

#### Gold Layer Data Model

A single denormalized KPI table — one row per hotel per night (internal schema uses lowercase):

**💰 performance_kpis** (fact table - internal schema)
```
hotel_id                 string [PK]
night_of_stay            date   [PK]
occupancy_percentage     float  (0-100, non-cancelled only)
total_net_revenue        float  (room + F&B net, all statuses)
adr                      int    (revenue per occupied room)
```

**Sample output rows (hotel 1035):**
```
hotel_id | night_of_stay | occupancy_percentage | total_net_revenue | adr
---------|---------------|----------------------|-------------------|-----
1035     | 2026-05-31    | 50.0                 | 1063.67           | 152
1035     | 2026-05-30    | 21.43                | 505.84            | 169
1035     | 2026-05-29    | 28.57                | 696.50            | 174
```

**Key properties:**
- ✅ One row per hotel per night (clean grain, no ambiguity)
- ✅ Complete date spine (all dates present, zero-filled by Reporting)
- ✅ Asymmetric business logic (cancelled excluded from occupancy, but included in revenue)
- ✅ BI-ready (directly consumable for dashboards and exports)

---

### 📊 Reporting Layer — Emulating a BI Consumer

The `KPIReportExporter` is designed to emulate what a BI tool (e.g. Tableau, Looker) expects when generating dashboards: **a complete date spine with no gaps**. For every day in the requested `from_date → to_date` range, a row is guaranteed to exist — even if there were zero bookings that night. Missing dates are zero-filled (`OCCUPANCY_PERCENTAGE = 0`, `TOTAL_NET_REVENUE = 0`, `ADR = 0`).

The report is filtered by `hotel_id` and sorted by `NIGHT_OF_STAY` descending (most recent first), matching the convention of most revenue management dashboards.

---

## 📋 Assumptions

This solution is built on three key assumptions:

### 1. Intermediate Data Is Processed In-Memory (Not Persisted)

**Assumption:** For this exercise, intermediate Bronze/Silver/Gold datasets are not stored. Data is processed in pandas `DataFrame`s and kept in memory only while the pipeline is running.

**Why:** The challenge is focused on transformation logic and KPI correctness, so in-memory processing keeps the implementation simple and fast to iterate.

**Trade-off:** This approach is not ideal for large-scale production workloads, lineage, or long-term reprocessing. In production, intermediate layers could be persisted in Athena tables over S3, or in another cloud database/warehouse engine.

---

### 2. Hotel Room Inventory is Trusted

**Assumption:** The `hotel_room_inventory.csv` export from the database is always correct and requires no validation or cleanup.

**Why:** Hotel room inventory is a relatively static reference dataset typically managed by a dedicated hotel management system. Database exports from such systems are generally considered trustworthy sources. The `HotelRoomInventoryCleaner` performs only essential type coercion for merge compatibility — no schema validation, no data filtering, and no deduplication.

**Trade-off:** If bad data ever enters the inventory source, it will propagate directly to KPI calculations. In production, periodic data quality checks or reconciliation with the source system would mitigate this risk.

---

### 3. Entire Reservation Validation — All-or-Nothing

**Assumption:** If **any** single validation check fails for a reservation, the entire reservation and all its stay dates are discarded.

**Why:** A reservation is a business entity with atomic meaning:
- If a stay date overlaps with another stay date (violating uniqueness), it suggests the entire reservation payload is corrupted or duplicated
- If arrival/departure dates are invalid, the whole booking's timeline is unreliable
- If a stay date falls outside the reservation window, all stay dates in that reservation become suspect

Rather than trying to "salvage" individual corrupted pieces of a reservation, this all-or-nothing approach ensures the Gold layer contains only reservations we fully trust. This design prevents subtle bugs where a partially-cleaned reservation could lead to incorrect KPI calculations.

**Trade-off:** We may discard more data than strictly necessary, but in return we guarantee data integrity in the Gold layer. This is the right choice for revenue analytics, where accuracy is more important than coverage.

