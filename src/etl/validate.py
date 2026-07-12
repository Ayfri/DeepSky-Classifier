from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, ValidationError
from tqdm.auto import tqdm

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _missing_to_none(value: object) -> object:
	"""Map pandas' missing sentinels (NaN, NaT, pd.NA) onto ``None``.

	A left join leaves NaN, which is a *float*, in the columns of unmatched rows. A schema field
	typed ``int | None`` rejects that float, so every row lacking an optional enrichment (a Gaia
	counterpart, say) fails validation and is silently quarantined rather than kept with an empty
	column. Nullable means nullable, so hand pydantic the None it expects.
	"""
	if value is None or value is pd.NA or value is pd.NaT:
		return None
	if isinstance(value, float | np.floating) and bool(np.isnan(value)):
		return None
	return value


def validate_dataframe(
	df: pd.DataFrame,
	schema: type[BaseModel],
) -> tuple[pd.DataFrame, pd.DataFrame]:
	"""Returns (valid_df, quarantine_df) after row-level schema validation."""
	quarantine_records: list[dict[str, Any]] = []
	valid_records: list[dict[str, Any]] = []

	records = df.to_dict("records")
	for record in tqdm(
		records,
		total=len(records),
		desc=f"Validating {schema.__name__}",
		unit="row",
		dynamic_ncols=True,
	):
		try:
			validated = schema.model_validate(
				{key: _missing_to_none(value) for key, value in record.items()}
			)
			valid_records.append(validated.model_dump())
		except ValidationError as exc:
			quarantine_row = {str(key): value for key, value in record.items()}
			quarantine_row["_validation_errors"] = str(exc)
			quarantine_records.append(quarantine_row)

	logger.info(
		f"Validation complete: {len(valid_records)} valid, {len(quarantine_records)} quarantined"
	)

	return pd.DataFrame(valid_records), pd.DataFrame(quarantine_records)
