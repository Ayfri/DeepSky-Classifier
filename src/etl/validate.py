import pandas as pd
from pydantic import BaseModel, ValidationError
from tqdm.auto import tqdm

from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def validate_dataframe(
	df: pd.DataFrame,
	schema: type[BaseModel],
) -> tuple[pd.DataFrame, pd.DataFrame]:
	"""Returns (valid_df, quarantine_df) after row-level schema validation."""
	quarantine_records: list[dict] = []
	valid_records: list[dict] = []

	records = df.to_dict("records")
	for record in tqdm(
		records,
		total=len(records),
		desc=f"Validating {schema.__name__}",
		unit="row",
		dynamic_ncols=True,
	):
		try:
			validated = schema.model_validate(record)
			valid_records.append(validated.model_dump())
		except ValidationError as exc:
			record["_validation_errors"] = str(exc)
			quarantine_records.append(record)

	logger.info(
		f"Validation complete: {len(valid_records)} valid, "
		f"{len(quarantine_records)} quarantined"
	)

	return pd.DataFrame(valid_records), pd.DataFrame(quarantine_records)
