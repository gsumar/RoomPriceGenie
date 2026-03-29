"""
QA Validation Script - Pure Python Implementation
Independently computes KPIs for hotel 1035 May 2026 without pandas
to validate the medallion pipeline output.

Validation rules mirror the Silver layer exactly:
  - All required reservation and stay_date fields must be present and valid
  - status must be a known enum value
  - departure_date must be greater than arrival_date
  - Each stay_date must fall within the reservation period
  - No duplicate nights across stay_date ranges
  - If any check fails the ENTIRE reservation is discarded
  - Deduplication: keep the latest updated_at version of each reservation
  - Stay_dates are only collected from the deduplicated winning record
"""

import json
import csv
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path


VALID_STATUSES = {'confirmed', 'cancelled', 'checked_in', 'checked_out'}


def _parse_date(value):
    """Parse a YYYY-MM-DD string. Returns datetime or None."""
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value.strip(), '%Y-%m-%d')
    except ValueError:
        return None


def _parse_datetime(value):
    """Parse an ISO-8601 datetime string. Returns datetime or None."""
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def _parse_float_string(value, required=True):
    """
    Parse a string that represents a float.
    Returns float, or 0.0 for optional missing, or None on failure.
    """
    if value is None or (isinstance(value, str) and value.strip() == ''):
        return None if required else 0.0
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def _validate_reservation(res):
    """
    Validate a raw reservation dict against the full API contract.
    Returns a normalised dict or None if the reservation must be discarded.
    Any invalid stay_date causes the whole reservation to be discarded.
    """
    # --- Required string fields ---
    hotel_id = res.get('hotel_id')
    reservation_id = res.get('reservation_id')
    status = res.get('status')

    if not hotel_id or not isinstance(hotel_id, str) or not hotel_id.strip():
        return None
    if not reservation_id or not isinstance(reservation_id, str) or not reservation_id.strip():
        return None
    if not status or not isinstance(status, str) or status.strip() not in VALID_STATUSES:
        return None

    # --- Required date/datetime fields ---
    arrival = _parse_date(res.get('arrival_date'))
    departure = _parse_date(res.get('departure_date'))
    created_at = _parse_datetime(res.get('created_at'))
    updated_at = _parse_datetime(res.get('updated_at'))

    if arrival is None or departure is None or created_at is None or updated_at is None:
        return None

    # Business rule: departure must be after arrival
    if not (departure > arrival):
        return None

    # --- Required stay_dates list ---
    stay_dates_raw = res.get('stay_dates')
    if not isinstance(stay_dates_raw, list) or len(stay_dates_raw) == 0:
        return None

    # --- Validate each stay_date — one failure discards the whole reservation ---
    valid_stay_dates = []
    for sd in stay_dates_raw:
        start = _parse_date(sd.get('start_date'))
        end = _parse_date(sd.get('end_date'))

        room_type_id = sd.get('room_type_id')
        room_type_name = sd.get('room_type_name')
        room_rev_gross = _parse_float_string(sd.get('room_revenue_gross_amount'), required=True)
        room_rev_net = _parse_float_string(sd.get('room_revenue_net_amount'), required=True)
        fnb_gross = _parse_float_string(sd.get('fnb_gross_amount'), required=False)
        fnb_net = _parse_float_string(sd.get('fnb_net_amount'), required=False)

        if start is None or end is None:
            return None
        if not room_type_id or not isinstance(room_type_id, str) or not room_type_id.strip():
            return None
        if not room_type_name or not isinstance(room_type_name, str) or not room_type_name.strip():
            return None
        if room_rev_gross is None or room_rev_net is None:
            return None

        # Business rule: stay_date must fall within reservation period
        if start < arrival or end >= departure:
            return None

        valid_stay_dates.append({
            'start_date': start,
            'end_date': end,
            'room_type_id': room_type_id.strip(),
            'room_type_name': room_type_name.strip(),
            'room_revenue_gross_amount': room_rev_gross,
            'room_revenue_net_amount': room_rev_net,
            'fnb_gross_amount': fnb_gross,
            'fnb_net_amount': fnb_net,
        })

    # Business rule: no duplicate nights across all stay_date ranges
    seen_nights = set()
    for sd in valid_stay_dates:
        night = sd['start_date']
        while night <= sd['end_date']:
            key = night.strftime('%Y-%m-%d')
            if key in seen_nights:
                return None
            seen_nights.add(key)
            night += timedelta(days=1)

    return {
        'hotel_id': hotel_id.strip(),
        'reservation_id': reservation_id.strip(),
        'status': status.strip(),
        'arrival_date': arrival,
        'departure_date': departure,
        'created_at': created_at,
        'updated_at': updated_at,
        'stay_dates': valid_stay_dates,
    }


def load_reservations_and_stays(json_path):
    """
    Load and validate reservations from JSON.

    Pass 1: validate every record and deduplicate by latest updated_at.
            Invalid records are discarded entirely.
    Pass 2: collect stay_dates for hotel 1035 only from the winning
            (deduplicated) record — never from stale earlier versions.
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Pass 1 — validate and deduplicate
    reservations = {}  # (hotel_id, reservation_id) -> normalised record
    for res in data.get('data', []):
        normalized = _validate_reservation(res)
        if normalized is None:
            continue

        key = (normalized['hotel_id'], normalized['reservation_id'])
        existing = reservations.get(key)
        if existing is None or normalized['updated_at'] >= existing['updated_at']:
            reservations[key] = normalized

    # Pass 2 — flatten stay_dates for hotel 1035 from winners only
    stay_dates = []
    for (hotel_id, res_id), normalized in reservations.items():
        if hotel_id != '1035':
            continue
        for sd in normalized['stay_dates']:
            stay_dates.append({
                'hotel_id': hotel_id,
                'reservation_id': res_id,
                'start_date': sd['start_date'].strftime('%Y-%m-%d'),
                'end_date': sd['end_date'].strftime('%Y-%m-%d'),
                'room_type_id': sd['room_type_id'],
                'room_revenue_net_amount': sd['room_revenue_net_amount'],
                'fnb_net_amount': sd['fnb_net_amount'],
            })

    return reservations, stay_dates


def load_inventory(csv_path):
    """Load inventory for hotel 1035 from CSV."""
    inventory = {}
    total_rooms = 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            hotel_id = row['hotel_id']
            if hotel_id == '1035':
                room_type_id = row['room_type_id']
                quantity = int(row['quantity'])
                inventory[room_type_id] = quantity
                total_rooms += quantity

    return inventory, total_rooms


def calculate_kpis(reservations, stay_dates, inventory, total_rooms, start_date, end_date):
    """Calculate KPIs for hotel 1035 for the given date range."""

    # Filter stay_dates to those with a valid room type in inventory
    valid_stays = []
    for stay in stay_dates:
        res_key = (stay['hotel_id'], stay['reservation_id'])
        if res_key not in reservations:
            continue
        if stay['room_type_id'] not in inventory:
            continue
        valid_stays.append(stay)

    nightly_data = defaultdict(list)   # night -> [(room_rev, fnb_rev)]
    nightly_occupancy = defaultdict(int)  # night -> occupied_rooms (non-cancelled only)

    for stay in valid_stays:
        start = datetime.strptime(stay['start_date'], '%Y-%m-%d')
        end = datetime.strptime(stay['end_date'], '%Y-%m-%d')
        res_key = (stay['hotel_id'], stay['reservation_id'])
        status = reservations[res_key]['status']

        nights = max((end - start).days + 1, 1)
        room_rev_per_night = stay['room_revenue_net_amount'] / nights
        fnb_rev_per_night = stay['fnb_net_amount'] / nights

        current = start
        while current <= end:
            night_str = current.strftime('%Y-%m-%d')
            nightly_data[night_str].append((room_rev_per_night, fnb_rev_per_night))
            if status != 'cancelled':
                nightly_occupancy[night_str] += 1
            current += timedelta(days=1)

    # Generate one row per day in the requested range (zero-filled for missing nights)
    kpis = []
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    current = start_dt

    while current <= end_dt:
        night_str = current.strftime('%Y-%m-%d')
        occupied = nightly_occupancy.get(night_str, 0)
        total_revenue = round(
            sum(r[0] + r[1] for r in nightly_data.get(night_str, [])), 2
        )
        occupancy_pct = round((occupied / total_rooms) * 100.0, 2) if total_rooms > 0 else 0.0
        adr = int(round(total_revenue / occupied)) if occupied > 0 else 0

        kpis.append({
            'NIGHT_OF_STAY': night_str,
            'OCCUPANCY_PERCENTAGE': occupancy_pct,
            'TOTAL_NET_REVENUE': total_revenue,
            'ADR': adr,
        })
        current += timedelta(days=1)

    kpis.sort(key=lambda x: x['NIGHT_OF_STAY'], reverse=True)
    return kpis


def write_csv(kpis, output_path):
    """Write KPIs to CSV."""
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f, fieldnames=['NIGHT_OF_STAY', 'OCCUPANCY_PERCENTAGE', 'TOTAL_NET_REVENUE', 'ADR']
        )
        writer.writeheader()
        for row in kpis:
            writer.writerow({
                'NIGHT_OF_STAY': row['NIGHT_OF_STAY'],
                'OCCUPANCY_PERCENTAGE': str(row['OCCUPANCY_PERCENTAGE']),
                'TOTAL_NET_REVENUE': str(row['TOTAL_NET_REVENUE']),
                'ADR': str(row['ADR']),
            })


def main():
    root = Path(__file__).parents[1]
    json_path = root / 'external-systems' / 'odyssey' / 'reservations_data.json'
    csv_path = root / 'external-systems' / 'db' / 'hotel_room_inventory.csv'
    output_path = root / 'qa' / 'kpi_1035_qa_pure_python.csv'

    print('=' * 80)
    print('QA VALIDATION - Pure Python Implementation')
    print('=' * 80)
    print()

    print(f'Loading reservations from: {json_path}')
    reservations, stay_dates = load_reservations_and_stays(json_path)

    print(f'Loading inventory from: {csv_path}')
    inventory, total_rooms = load_inventory(csv_path)

    hotel_1035_reservations = [v for k, v in reservations.items() if k[0] == '1035']

    print()
    print('Hotel 1035 Inventory Summary:')
    print(f'  Room types: {list(inventory.keys())}')
    print(f'  Total rooms: {total_rooms}')
    print(f'  Valid unique reservations (all hotels): {len(reservations)}')
    print(f'  Valid unique reservations (hotel 1035): {len(hotel_1035_reservations)}')
    print(f'  Stay_date rows (hotel 1035): {len(stay_dates)}')
    print()

    print('Calculating KPIs for May 2026...')
    kpis = calculate_kpis(reservations, stay_dates, inventory, total_rooms, '2026-05-01', '2026-05-31')
    print(f'Generated {len(kpis)} KPI rows')
    print()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_csv(kpis, output_path)
    print(f'✅ QA CSV written to: {output_path}')
    print()

    print('Sample output (first 3 rows - descending):')
    print('-' * 80)
    for row in kpis[:3]:
        print(f"{row['NIGHT_OF_STAY']}           {row['OCCUPANCY_PERCENTAGE']:>8}%         {row['TOTAL_NET_REVENUE']:>10}             {row['ADR']}")
    print()
    print('Sample output (last 3 rows - ascending):')
    print('-' * 80)
    for row in kpis[-3:]:
        print(f"{row['NIGHT_OF_STAY']}           {row['OCCUPANCY_PERCENTAGE']:>8}%         {row['TOTAL_NET_REVENUE']:>10}             {row['ADR']}")
    print()


if __name__ == '__main__':
    main()
