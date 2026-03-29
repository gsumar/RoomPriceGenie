# RoomPriceGenie

Coding Challenge for a Data Engineer.

## Pipeline Architecture

The implementation follows a medallion-style flow managed by `src/HotelReservationKPIPipeline.py`:

- **Bronze** (`src/datalake/bronze/connectors`): raw loaders for reservations, stay dates, and hotel room inventory.
- **Silver** (`src/datalake/silver`): entity-level cleaners with a shared `clean` contract.
- **Gold** (`src/datalake/gold/performance_kpis.py`): nightly KPI aggregation per hotel with columns:
  - `NIGHT_OF_STAY`
  - `OCCUPANCY_PERCENTAGE`
  - `TOTAL_NET_REVENUE`
  - `ADR`
- **Reporting** (`src/reporting/kpi_report_exporter.py`): filters by hotel/date range and exports CSV sorted by `NIGHT_OF_STAY` descending, including zero-filled rows for missing dates.

## Run Pipeline

Default run (equivalent to hotel `1035`, `2026-05-01` to `2026-05-31`, output to `reports`):

```bash
cd /Users/gorka/Documents/workspace/RoomPriceGenie
PYTHONPATH=src python src/HotelReservationKPIPipeline.py
```

Run for any hotel/date range (required arguments are positional: `hotel_id`, `from_date`, `to_date`):

```bash
cd /Users/gorka/Documents/workspace/RoomPriceGenie
PYTHONPATH=src python src/HotelReservationKPIPipeline.py 1035 2026-05-01 2026-05-31
```

Optional output directory (default is `reports`):

```bash
cd /Users/gorka/Documents/workspace/RoomPriceGenie
PYTHONPATH=src python src/HotelReservationKPIPipeline.py 1035 2026-05-01 2026-05-31 --output-dir reports
```

## Output Naming Convention

The generated CSV follows:

- `kpi_<hotel_id>_<yyyy>_<mm>_<dd>to_<yyyy>_<mm>_<dd>_.csv`

Example:

- `kpi_1035_2026_05_01to_2026_05_31_.csv`
