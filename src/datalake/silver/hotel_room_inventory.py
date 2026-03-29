import logging

import pandas as pd

from ..schema_registry import SchemaRegistry
from .base import SilverCleaner


log = logging.getLogger(__name__)
registry = SchemaRegistry()


class HotelRoomInventoryCleaner(SilverCleaner):
    """Clean hotel inventory rows used for occupancy denominators."""

    SOURCE = 'db/hotel_room_inventory'
    VERSION = 'v1'

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        cleaned = registry.validate_and_transform_rows(self.SOURCE, self.VERSION, df)

        if cleaned.empty:
            return pd.DataFrame(columns=['hotel_id', 'room_type_id', 'quantity'])

        cleaned['hotel_id'] = cleaned['hotel_id'].astype('string')
        cleaned['room_type_id'] = cleaned['room_type_id'].astype('string')
        cleaned['quantity'] = cleaned['quantity'].astype('int64')

        cleaned = cleaned[cleaned['quantity'] >= 0]
        cleaned = cleaned.drop_duplicates(subset=['hotel_id', 'room_type_id'], keep='last')

        log.info('Cleaned inventory rows: %s', len(cleaned))
        return cleaned