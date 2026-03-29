from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import json
import logging
from pathlib import Path

import pandas as pd


log = logging.getLogger(__name__)


@dataclass
class SchemaVersion:
    version: str
    description: str
    schema: Dict[str, Dict[str, Any]]
    mapping: Dict[str, str] = field(default_factory=dict)

    def apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.mapping) if self.mapping else df


class SchemaRegistry:
    def __init__(self, schema_dir: str | None = None):
        if schema_dir is None:
            schema_dir = Path(__file__).parent.parent.parent / 'schema' / 'bronze'
        self.schema_dir = Path(schema_dir).resolve()
        self.schemas: Dict[str, Dict[str, SchemaVersion]] = {}
        self._load_schemas()

    def _load_schemas(self) -> None:
        if not self.schema_dir.exists():
            log.warning('Schema directory not found: %s', self.schema_dir)
            return
        self._load_schemas_recursive(self.schema_dir, '')

    def _load_schemas_recursive(self, directory: Path, prefix: str) -> None:
        for item in sorted(directory.iterdir()):
            if not item.is_dir():
                continue

            schema_files = sorted(item.glob('v*.json'))
            if schema_files:
                source_name = f'{prefix}/{item.name}' if prefix else item.name
                source_schemas: Dict[str, SchemaVersion] = {}

                for schema_file in schema_files:
                    with open(schema_file, 'r', encoding='utf-8') as f:
                        schema_data = json.load(f)

                    source_schemas[schema_file.stem] = SchemaVersion(
                        version=schema_data['version'],
                        description=schema_data['description'],
                        schema=schema_data['schema'],
                        mapping=schema_data.get('mapping', {}),
                    )

                self.schemas[source_name] = source_schemas
                continue

            new_prefix = f'{prefix}/{item.name}' if prefix else item.name
            self._load_schemas_recursive(item, new_prefix)

    def get_schema(self, source: str, version: str) -> Optional[SchemaVersion]:
        return self.schemas.get(source, {}).get(version)

    def validate_record(
        self,
        schema_definition: Dict[str, Dict[str, Any]],
        record: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        normalized: Dict[str, Any] = {}

        for field_name, rules in schema_definition.items():
            value = record.get(field_name, pd.NA)
            normalized_value, is_valid = self._validate_field(value=value, rules=rules)
            if not is_valid:
                return None
            normalized[field_name] = normalized_value

        return normalized

    def validate_and_transform_rows(
        self,
        source: str,
        version: str,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        schema = self.get_schema(source, version)
        if not schema:
            raise ValueError(f'Schema not found: {source}/{version}')

        valid_rows = []
        for record in df.to_dict(orient='records'):
            normalized = self.validate_record(schema.schema, record)
            if normalized is not None:
                valid_rows.append(normalized)

        cleaned = pd.DataFrame(valid_rows)
        if cleaned.empty:
            cleaned = pd.DataFrame(columns=list(schema.schema.keys()))

        return schema.apply_mapping(cleaned)

    def _validate_field(self, value: Any, rules: Dict[str, Any]) -> tuple[Any, bool]:
        field_type = rules.get('type')
        required = bool(rules.get('required', False))
        enum_values = rules.get('enum')
        item_schema = rules.get('item_schema')

        if field_type == 'list':
            return self._validate_list_field(
                value=value,
                required=required,
                item_schema=item_schema or {},
            )

        if self._is_missing(value):
            if not required:
                if field_type == 'float_string':
                    return 0.0, True
                return None, True
            return None, False

        if field_type == 'string':
            if not isinstance(value, str):
                return None, False
            normalized = value.strip()
            if required and normalized == '':
                return None, False
            if enum_values and normalized not in enum_values:
                return None, False
            return normalized, True

        if field_type == 'coerce_string':
            normalized = str(value).strip()
            if required and normalized == '':
                return None, False
            if enum_values and normalized not in enum_values:
                return None, False
            return normalized, True

        if field_type == 'date':
            if not isinstance(value, str) or value.strip() == '':
                return None, False
            parsed = pd.to_datetime(value, format='%Y-%m-%d', errors='coerce')
            if pd.isna(parsed):
                return None, False
            return pd.Timestamp(parsed).normalize(), True

        if field_type == 'datetime':
            if not isinstance(value, str) or value.strip() == '':
                return None, False
            parsed = pd.to_datetime(value, utc=True, errors='coerce')
            if pd.isna(parsed):
                return None, False
            return parsed, True

        if field_type == 'float_string':
            if not isinstance(value, str) or value.strip() == '':
                return (0.0, True) if not required else (None, False)
            parsed = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
            if pd.isna(parsed):
                return None, False
            return float(parsed), True

        if field_type == 'int':
            parsed = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
            if pd.isna(parsed):
                return None, False
            return int(parsed), True

        raise ValueError(f"Unsupported schema field type: '{field_type}'")

    def _validate_list_field(
        self,
        value: Any,
        required: bool,
        item_schema: Dict[str, Dict[str, Any]],
    ) -> tuple[Any, bool]:
        if self._is_missing(value):
            return ([], not required)

        if not isinstance(value, list):
            return None, False

        if required and len(value) == 0:
            return None, False

        normalized_items = []
        for item in value:
            if not isinstance(item, dict):
                return None, False
            normalized = self.validate_record(item_schema, item)
            if normalized is None:
                return None, False
            normalized_items.append(normalized)

        return normalized_items, True

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None or value is pd.NA:
            return True
        if isinstance(value, (list, dict)):
            return False
        try:
            result = pd.isna(value)
        except Exception:
            return False
        return bool(result) if isinstance(result, (bool, type(pd.NA), type(True))) else False
