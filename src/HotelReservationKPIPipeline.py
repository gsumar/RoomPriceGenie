import argparse
import logging
from pathlib import Path

import pandas as pd

# Bronze: raw data ingestion connectors
from datalake.bronze.connectors import HotelRoomInventoryConnector, ReservationConnector

# Silver: data quality/cleaning components
from datalake.silver import (
    HotelRoomInventoryCleaner,
    ReservationsCleaner,
)

# Gold: KPI aggregation logic
from datalake.gold import PerformanceKPIs

# Reporting: CSV export for consumers
from reporting import KPIReportExporter


log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)


class HotelReservationKPIPipeline:
    """Orchestrates Bronze -> Silver -> Gold -> Reporting for hotel KPI output."""

    def __init__(self) -> None:
        # Bronze (raw loaders)
        self.reservation_connector = ReservationConnector()
        self.inventory_connector = HotelRoomInventoryConnector()

        # Silver (cleaners)
        self.reservations_cleaner = ReservationsCleaner()
        self.inventory_cleaner = HotelRoomInventoryCleaner()

        # Gold (business KPI builder)
        self.performance_kpis = PerformanceKPIs()

        # Reporting (CSV exporter)
        self.report_exporter = KPIReportExporter()

    def run(
            self,
            reservations_json_path: str | Path,
            inventory_csv_path: str | Path,
            report_output_dir: str | Path,
            report_hotel_id: str,
            report_start_date: str,
            report_end_date: str,
    ) -> Path:
        log.info('Starting pipeline execution')

        # Bronze: load raw datasets
        bronze_reservations = self.reservation_connector(reservations_json_path)
        bronze_inventory = self.inventory_connector(inventory_csv_path)

        # Silver: validate and clean
        silver_reservation_stays = self.reservations_cleaner(bronze_reservations)
        silver_inventory = self.inventory_cleaner(bronze_inventory)

        # Gold: split, compute KPI dataframe
        gold_performance_kpis = self.performance_kpis.build(
            reservation_stays_df=silver_reservation_stays,
            inventory_df=silver_inventory,
        )

        # Reporting: filter and export CSV
        report_path = self.report_exporter.export_csv(
            gold_df=gold_performance_kpis,
            output_dir=report_output_dir,
            hotel_id=report_hotel_id,
            start_date=report_start_date,
            end_date=report_end_date,
        )

        log.info('Pipeline completed with %s gold rows', len(gold_performance_kpis))
        return report_path


def _validate_date_range(from_date: str, to_date: str) -> None:
    """Validate CLI date inputs in YYYY-MM-DD format and logical ordering."""
    from_dt = pd.to_datetime(from_date, format='%Y-%m-%d', errors='coerce')
    to_dt = pd.to_datetime(to_date, format='%Y-%m-%d', errors='coerce')
    if pd.isna(from_dt) or pd.isna(to_dt):
        raise ValueError('from_date and to_date must use format YYYY-MM-DD')
    if from_dt > to_dt:
        raise ValueError('from_date must be less than or equal to to_date')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the hotel reservation KPI pipeline.')
    parser.add_argument('hotel_id', nargs='?', default='1035')
    parser.add_argument('from_date', nargs='?', default='2026-05-01')
    parser.add_argument('to_date', nargs='?', default='2026-05-31')
    parser.add_argument('--output-dir', default='reports')
    args = parser.parse_args()

    try:
        _validate_date_range(args.from_date, args.to_date)
    except ValueError as exc:
        parser.error(str(exc))

    root = Path(__file__).parents[1]
    pipeline = HotelReservationKPIPipeline()

    report_path = pipeline.run(
        reservations_json_path=root / 'external-systems' / 'odyssey' / 'reservations_data.json',
        inventory_csv_path=root / 'external-systems' / 'db' / 'hotel_room_inventory.csv',
        report_output_dir=root / args.output_dir,
        report_hotel_id=str(args.hotel_id),
        report_start_date=args.from_date,
        report_end_date=args.to_date,
    )
    log.info('Report generated: %s', report_path)
