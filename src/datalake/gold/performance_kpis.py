import logging
import pandas as pd

from .base import GoldBuilder


log = logging.getLogger(__name__)


class PerformanceKPIs(GoldBuilder):
    """Build nightly KPI metrics per hotel."""

    def build(
        self,
        reservations_df: pd.DataFrame,
        stay_dates_df: pd.DataFrame,
        inventory_df: pd.DataFrame,
    ) -> pd.DataFrame:
        _empty = pd.DataFrame(
            columns=['hotel_id', 'night_of_stay', 'occupancy_percentage', 'total_net_revenue', 'adr']
        )

        if stay_dates_df.empty or reservations_df.empty:
            return _empty

        merged = stay_dates_df.merge(
            reservations_df[['hotel_id', 'reservation_id', 'status']],
            on=['hotel_id', 'reservation_id'],
            how='inner',
        )
        merged['status'] = merged['status'].astype('string').str.lower()

        merged = merged.merge(
            inventory_df[['hotel_id', 'room_type_id']],
            on=['hotel_id', 'room_type_id'],
            how='inner',
        )
        if merged.empty:
            return _empty

        expanded = self._expand_to_nights(merged)
        if expanded.empty:
            return _empty

        # occupancy_percentage: Count only non-cancelled reservations
        occupancy_expanded = expanded[expanded['status'] != 'cancelled']
        occupancy = (
            occupancy_expanded.groupby(['hotel_id', 'night_of_stay'], as_index=False)
            .size()
            .rename(columns={'size': 'occupied_rooms'})
        )

        # total_net_revenue: Include all reservations regardless of status
        revenue = (
            expanded.groupby(['hotel_id', 'night_of_stay'], as_index=False)
            .agg(
                room_net_revenue=('room_net_revenue_per_night', 'sum'),
                fnb_net_revenue=('fnb_net_revenue_per_night', 'sum'),
            )
        )
        revenue['total_net_revenue'] = revenue['room_net_revenue'] + revenue['fnb_net_revenue']

        total_rooms = (
            inventory_df.groupby('hotel_id', as_index=False)
            .agg(total_rooms=('quantity', 'sum'))
            .astype({'hotel_id': 'string'})
        )

        kpis = occupancy.merge(revenue, on=['hotel_id', 'night_of_stay'], how='outer')
        kpis = kpis.merge(total_rooms, on='hotel_id', how='left')

        kpis['occupied_rooms'] = kpis['occupied_rooms'].fillna(0)
        kpis['room_net_revenue'] = kpis['room_net_revenue'].fillna(0.0)
        kpis['total_net_revenue'] = kpis['total_net_revenue'].fillna(0.0)
        kpis['total_rooms'] = kpis['total_rooms'].fillna(0)

        kpis['occupancy_percentage'] = 0.0
        has_inventory = kpis['total_rooms'] > 0
        kpis.loc[has_inventory, 'occupancy_percentage'] = (
            (kpis.loc[has_inventory, 'occupied_rooms'] / kpis.loc[has_inventory, 'total_rooms'])
            * 100.0
        )
        kpis['occupancy_percentage'] = kpis['occupancy_percentage'].round(2)

        kpis['total_net_revenue'] = kpis['total_net_revenue'].round(2)

        kpis['adr'] = 0
        has_occupied = kpis['occupied_rooms'] > 0
        kpis.loc[has_occupied, 'adr'] = (
            (kpis.loc[has_occupied, 'total_net_revenue'] / kpis.loc[has_occupied, 'occupied_rooms'])
            .round(0)
            .astype('int64')
        )

        kpis = kpis[
            [
                'hotel_id',
                'night_of_stay',
                'occupancy_percentage',
                'total_net_revenue',
                'adr',
            ]
        ]

        log.info('Built gold KPI rows: %s', len(kpis))
        return kpis

    @staticmethod
    def _expand_to_nights(stay_dates_df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for record in stay_dates_df.to_dict(orient='records'):
            start = pd.Timestamp(record['start_date']).normalize()
            end = pd.Timestamp(record['end_date']).normalize()
            nights = pd.date_range(start=start, end=end, freq='D')
            number_of_nights = max(len(nights), 1)

            room_per_night = float(record['room_revenue_net_amount']) / number_of_nights
            fnb_per_night = float(record['fnb_net_amount']) / number_of_nights

            for night in nights:
                rows.append(
                    {
                        'hotel_id': record['hotel_id'],
                        'reservation_id': record['reservation_id'],
                        'status': record['status'],
                        'night_of_stay': night,
                        'room_net_revenue_per_night': room_per_night,
                        'fnb_net_revenue_per_night': fnb_per_night,
                    }
                )

        return pd.DataFrame(rows)
