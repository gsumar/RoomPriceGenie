from pathlib import Path
import logging
import pandas as pd

from .report_exporter import ReportExporter


log = logging.getLogger(__name__)


class KPIReportExporter(ReportExporter):
    """Export report-ready CSV files from Gold KPI rows."""

    OUTPUT_COLUMNS = [
        'NIGHT_OF_STAY',
        'OCCUPANCY_PERCENTAGE',
        'TOTAL_NET_REVENUE',
        'ADR',
    ]

    def export_csv(
        self,
        gold_df: pd.DataFrame,
        output_dir: str | Path,
        hotel_id: str,
        start_date: str,
        end_date: str,
    ) -> Path:
        hotel_id = str(hotel_id)
        date_start = pd.Timestamp(start_date).normalize()
        date_end = pd.Timestamp(end_date).normalize()

        date_spine = pd.DataFrame(
            {'NIGHT_OF_STAY': pd.date_range(start=date_start, end=date_end, freq='D')}
        )

        filtered = gold_df.copy()
        if not filtered.empty:
            filtered['hotel_id'] = filtered['hotel_id'].astype('string')
            filtered['NIGHT_OF_STAY'] = pd.to_datetime(filtered['NIGHT_OF_STAY'], errors='coerce')
            filtered = filtered[
                (filtered['hotel_id'] == hotel_id)
                & (filtered['NIGHT_OF_STAY'] >= date_start)
                & (filtered['NIGHT_OF_STAY'] <= date_end)
            ]

        report_df = date_spine.merge(filtered, on='NIGHT_OF_STAY', how='left')
        report_df['OCCUPANCY_PERCENTAGE'] = report_df['OCCUPANCY_PERCENTAGE'].fillna(0.0)
        report_df['TOTAL_NET_REVENUE'] = report_df['TOTAL_NET_REVENUE'].fillna(0.0)
        report_df['ADR'] = report_df['ADR'].fillna(0.0)

        report_df = report_df[self.OUTPUT_COLUMNS].sort_values('NIGHT_OF_STAY', ascending=False)
        report_df['NIGHT_OF_STAY'] = report_df['NIGHT_OF_STAY'].dt.strftime('%Y-%m-%d')

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        start_str = date_start.strftime('%Y_%m_%d')
        end_str = date_end.strftime('%Y_%m_%d')
        filename = f'kpi_{hotel_id}_{start_str}_to_{end_str}.csv'
        target = output_path / filename
        report_df.to_csv(target, index=False)

        log.info('Exported KPI report to %s', target)
        return target
