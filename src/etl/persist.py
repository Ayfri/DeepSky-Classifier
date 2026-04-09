from pathlib import Path

import pandas as pd

from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def persist_dataframe(
	df: pd.DataFrame,
	path: Path,
	fmt: str = "parquet",
) -> Path:
	path.parent.mkdir(parents=True, exist_ok=True)

	match fmt:
		case "parquet":
			target = path.with_suffix(".parquet")
			df.to_parquet(target, index=False)
		case "csv":
			target = path.with_suffix(".csv")
			df.to_csv(target, index=False)
		case _:
			raise ValueError(f"Unsupported output format: {fmt!r}")

	logger.info(f"Persisted {len(df)} rows to {target}")
	return target
