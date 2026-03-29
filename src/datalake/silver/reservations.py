import logging

import pandas as pd

from ..schema_registry import SchemaRegistry
from .base import SilverCleaner


log = logging.getLogger(__name__)
registry = SchemaRegistry()


class ReservationsCurater(SilverCleaner):
    """Validate Odyssey reservations as whole records and flatten valid stay rows."""

    SOURCE = 'odyssey/reservations_data'
    VERSION = 'v1'

    RESERVATION_COLUMNS = [
        'hotel_id',
        'reservation_id',
        'status',
        'arrival_date',
        'departure_date',
        'created_at',
        'updated_at',
    ]

    STAY_DATE_COLUMNS = [
        'start_date',
        'end_date',
        'room_type_id',
        'room_type_name',
        'room_revenue_gross_amount',
        'room_revenue_net_amount',
        'fnb_gross_amount',
        'fnb_net_amount',
    ]

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        schema = registry.get_schema(self.SOURCE, self.VERSION)
        if not schema:
            raise ValueError(f'Schema not found: {self.SOURCE}/{self.VERSION}')

        valid_reservations = []
        for record in df.to_dict(orient='records'):
            normalized = registry.validate_record(schema.schema, record)
            if normalized is None:
                continue
            if not self._reservation_period_is_valid(normalized):
                continue
            if not self._stay_dates_fit_reservation_period(normalized):
                continue
            if not self._stay_dates_have_unique_nights(normalized['stay_dates']):
                continue
            valid_reservations.append(normalized)

        valid_reservations_df = pd.DataFrame(valid_reservations)
        if valid_reservations_df.empty:
            return pd.DataFrame(columns=self.RESERVATION_COLUMNS + self.STAY_DATE_COLUMNS)

        valid_reservations_df = valid_reservations_df.sort_values('updated_at').drop_duplicates(
            subset=['hotel_id', 'reservation_id'], keep='last'
        )

        flattened_rows = []
        for reservation in valid_reservations_df.to_dict(orient='records'):
            reservation_base = {
                'hotel_id': reservation['hotel_id'],
                'reservation_id': reservation['reservation_id'],
                'status': reservation['status'],
                'arrival_date': reservation['arrival_date'],
                'departure_date': reservation['departure_date'],
                'created_at': reservation['created_at'],
                'updated_at': reservation['updated_at'],
            }

            for stay_date in reservation['stay_dates']:
                flattened_rows.append(
                    {
                        **reservation_base,
                        'start_date': stay_date['start_date'],
                        'end_date': stay_date['end_date'],
                        'room_type_id': stay_date['room_type_id'],
                        'room_type_name': stay_date['room_type_name'],
                        'room_revenue_gross_amount': stay_date['room_revenue_gross_amount'],
                        'room_revenue_net_amount': stay_date['room_revenue_net_amount'],
                        'fnb_gross_amount': stay_date['fnb_gross_amount'],
                        'fnb_net_amount': stay_date['fnb_net_amount'],
                    }
                )

        cleaned = pd.DataFrame(flattened_rows)
        cleaned = cleaned[self.RESERVATION_COLUMNS + self.STAY_DATE_COLUMNS]

        log.info(
            'Validated %s reservations into %s flattened stay rows',
            len(valid_reservations_df),
            len(cleaned),
        )
        return cleaned

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Alias for clean() to make the validation intent explicit."""
        return self.clean(df)

    def normalize_tables(self, validated_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split validated Silver rows into parent reservations and child stay_dates tables."""
        if validated_df.empty:
            reservations_df = pd.DataFrame(columns=self.RESERVATION_COLUMNS)
            stay_dates_df = pd.DataFrame(columns=['hotel_id', 'reservation_id'] + self.STAY_DATE_COLUMNS)
            return reservations_df, stay_dates_df

        reservations_df = (
            validated_df[self.RESERVATION_COLUMNS]
            .sort_values('updated_at')
            .drop_duplicates(subset=['hotel_id', 'reservation_id'], keep='last')
            .copy()
        )

        stay_dates_df = validated_df[['hotel_id', 'reservation_id'] + self.STAY_DATE_COLUMNS].copy()
        return reservations_df, stay_dates_df

    @staticmethod
    def _reservation_period_is_valid(reservation: dict) -> bool:
        return reservation['departure_date'] > reservation['arrival_date']

    @staticmethod
    def _stay_dates_fit_reservation_period(reservation: dict) -> bool:
        arrival = reservation['arrival_date']
        departure = reservation['departure_date']

        for stay_date in reservation['stay_dates']:
            start = stay_date['start_date']
            end = stay_date['end_date']

            if end < start:
                return False
            if start < arrival:
                return False
            if end >= departure:
                return False

        return True

    @staticmethod
    def _stay_dates_have_unique_nights(stay_dates: list[dict]) -> bool:
        seen_nights = set()

        for stay_date in stay_dates:
            nights = pd.date_range(
                start=stay_date['start_date'],
                end=stay_date['end_date'],
                freq='D',
            )
            for night in nights:
                normalized_night = pd.Timestamp(night).normalize()
                if normalized_night in seen_nights:
                    return False
                seen_nights.add(normalized_night)

        return True

