from pathlib import Path

from pydantic import BaseModel, Field


ALLOWED_CLASSES: set[str] = {"GALAXY", "QSO", "STAR"}

OUTPUT_FORMATS: set[str] = {"csv", "parquet"}


class PipelineConfig(BaseModel):
	data_release: int = Field(default=17, ge=1)
	limit_per_class: int = Field(default=2000, ge=1)
	output_dir: Path = Field(default=Path("data/raw/sdss"))
	output_format: str = Field(default="parquet")
	quarantine_dir: Path = Field(default=Path("data/quarantine"))
	target_labels: list[str] = Field(default_factory=lambda: sorted(ALLOWED_CLASSES))
