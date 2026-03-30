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
            {'night_of_stay': pd.date_range(start=date_start, end=date_end, freq='D')}
        )

        filtered = gold_df.copy()
        if not filtered.empty:
            filtered['hotel_id'] = filtered['hotel_id'].astype('string')
            filtered['night_of_stay'] = pd.to_datetime(filtered['night_of_stay'], errors='coerce')
            filtered = filtered[
                (filtered['hotel_id'] == hotel_id)
                & (filtered['night_of_stay'] >= date_start)
                & (filtered['night_of_stay'] <= date_end)
            ]

        report_df = date_spine.merge(filtered, on='night_of_stay', how='left')
        report_df['occupancy_percentage'] = report_df['occupancy_percentage'].fillna(0.0)
        report_df['total_net_revenue'] = report_df['total_net_revenue'].fillna(0.0)
        report_df['adr'] = report_df['adr'].fillna(0.0)

        report_df = report_df[['night_of_stay', 'occupancy_percentage', 'total_net_revenue', 'adr']].sort_values('night_of_stay', ascending=False)
        report_df['night_of_stay'] = report_df['night_of_stay'].dt.strftime('%Y-%m-%d')
        
        # Rename to UPPERCASE for CSV export
        report_df = report_df.rename(columns={
            'night_of_stay': 'NIGHT_OF_STAY',
            'occupancy_percentage': 'OCCUPANCY_PERCENTAGE',
            'total_net_revenue': 'TOTAL_NET_REVENUE',
            'adr': 'ADR',
        })

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        start_str = date_start.strftime('%Y_%m_%d')
        end_str = date_end.strftime('%Y_%m_%d')
        filename = f'kpi_{hotel_id}_{start_str}_to_{end_str}.csv'
        target = output_path / filename
        report_df.to_csv(target, index=False)

        log.info('Exported KPI report to %s', target)
        return target
