# QA Validation Implementation Summary

## Overview
A pure Python QA validation suite has been implemented to independently verify the medallion pipeline's KPI calculations for hotel 1035 in May 2026.

## Files Created

### 1. `qa/qa_pure_python.py`
**Pure Python QA Implementation** (no pandas, no external dependencies beyond Python standard library)

**What it does:**
- Loads `reservations_data.json` and `hotel_room_inventory.csv` from `external-systems/`
- Extracts data for hotel 1035 only
- Filters by active reservation statuses: `confirmed`, `checked_in`, `in_house`, `booked`
- Validates dates: drops records with invalid dates (matching medallion behavior)
- Expands date ranges (inclusive start_date to end_date) into individual night records
- Distributes revenue evenly across nights
- Filters by room types in the inventory
- Calculates KPIs per night:
  - `OCCUPANCY_PERCENTAGE`: (occupied_rooms / total_rooms) * 100
  - `TOTAL_NET_REVENUE`: room_revenue + fnb_revenue
  - `ADR`: room_revenue / occupied_rooms
- Generates CSV sorted by `NIGHT_OF_STAY` descending
- Outputs to: `qa/kpi_1035_qa_pure_python.csv`

**Data Filtering Logic (matching medallion exactly):**
1. Join stay_dates with reservations (using reservation_id)
2. Filter by active status
3. Filter by valid room types in inventory
4. Drop records with invalid/unparseable dates
5. Check end_date >= start_date

**Run:**
```bash
python qa/qa_pure_python.py
```

### 2. `qa/qa_compare.py`
**Comparison and Validation Script**

**What it does:**
- Loads medallion report: `reports/kpi_1035_2026-05-01_to_2026-05-31.csv`
- Loads QA report: `qa/kpi_1035_qa_pure_python.csv`
- Compares row count (must be 31 rows for May 2026)
- Compares each row's values with configurable tolerance (default 0.01)
- Reports:
  - ✅ PASS if all rows match
  - ❌ FAIL with detailed diff output if mismatches found

**Run:**
```bash
python qa/qa_compare.py
```

### 3. `qa/.gitkeep`
Git repository marker to preserve the qa/ folder structure.

## Key Differences Aligned

During implementation, we discovered and aligned these behaviors:

1. **Invalid Date Handling**: Both implementations now drop records with unparseable dates (e.g., '2026-55-10' becomes NaT in pandas, ValueError in Python)
2. **Date Range Validation**: Both drop records where end_date < start_date
3. **Status Filtering**: Both filter to active statuses AFTER joining stay_dates with reservations
4. **Inventory Filtering**: Both perform INNER JOIN on (hotel_id, room_type_id) with inventory
5. **Date Expansion**: Both treat date ranges as INCLUSIVE (start_date to end_date)
6. **Revenue Distribution**: Both divide stay revenue evenly across nights
7. **Zero-Fill**: Both ensure all dates in May 1-31, 2026 appear in output (zeros for missing data)
8. **Sort Order**: Both sort by `NIGHT_OF_STAY` descending

## Data Pipeline Comparison

| Stage | Medallion | QA Pure Python |
|-------|-----------|----------------|
| **Bronze** | Load JSON/CSV files | Load JSON/CSV files |
| **Silver** | Validate schema, coerce types, drop NaT | Validate on-the-fly, skip invalid |
| **Gold** | Join, filter, expand, aggregate | Join, filter, expand, aggregate |
| **Reporting** | Filter by hotel/dates, fill zeros | (same as QA) |
| **Output** | CSV with consistent precision | CSV matching medallion format |

## Running the Validation

### Step 1: Generate QA independent output
```bash
cd /Users/gorka/Documents/workspace/RoomPriceGenie
python qa/qa_pure_python.py
```

Expected output:
```
================================================================================
QA VALIDATION - Pure Python Implementation
================================================================================

Loading reservations from: .../external-systems/odyssey/reservations_data.json
Loading inventory from: .../external-systems/db/hotel_room_inventory.csv

Hotel 1035 Inventory Summary:
  Room types: ['LD', 'GS', 'LS', 'SG', 'SU']
  Total rooms: 14
  Active reservations: 5052
  Total stay_dates records: 11951

Calculating KPIs for May 2026...
Generated 31 KPI rows

✅ QA CSV written to: .../qa/kpi_1035_qa_pure_python.csv

Sample output (first 3 rows - descending):
...
```

### Step 2: Compare outputs
```bash
python qa/qa_compare.py
```

Expected output (if validation passes):
```
================================================================================
QA COMPARISON REPORT
================================================================================

Medallion report:  .../reports/kpi_1035_2026-05-01_to_2026-05-31.csv
QA report:         .../qa/kpi_1035_qa_pure_python.csv

Row count:
  Medallion: 31 rows
  QA:        31 rows
  ✅ MATCH

Row-by-row comparison (tolerance=0.01):
  ✅ ALL ROWS MATCH

================================================================================
✅ VALIDATION PASSED - Both outputs match!
================================================================================
```

## Files Generated

- `qa/kpi_1035_qa_pure_python.csv` - Independent QA output (regenerated each run)
- `qa/qa_pure_python.py` - QA script
- `qa/qa_compare.py` - Comparison script
- `qa/.gitkeep` - Folder marker

## Next Steps

1. Run `python qa/qa_pure_python.py` to generate the QA output
2. Run `python qa/qa_compare.py` to validate
3. If mismatches appear, the script will show which rows differ and by how much
4. Both implementations can be compared line-by-line to identify data quality issues

## Design Rationale

- **Pure Python**: No dependencies means maximum portability and easy auditing
- **No Pandas**: Validates that the medallion logic is sound by reimplementing independently
- **Matching Logic**: QA implementation mirrors medallion exactly so mismatches point to actual issues
- **Clear Filtering**: Each step (join, filter, validate, expand) is explicit and testable
- **Tolerance**: Floating-point precision differences are expected, so tolerance is configurable

