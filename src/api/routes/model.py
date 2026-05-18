from typing import Any

from fastapi import APIRouter, Depends

from src.api.deps import get_metadata
from src.api.schemas import ModelInfo

router = APIRouter(prefix="/model", tags=["model"])


@router.get("/info", response_model=ModelInfo)
def model_info(metadata: dict[str, Any] = Depends(get_metadata)) -> ModelInfo:
	"""Return artefact metadata from the run_metadata.json written at training time."""
	return ModelInfo(
		feature_columns=metadata.get("feature_columns", []),
		labels=metadata.get("labels", []),
		model_sha256=metadata.get("model_sha256", ""),
		n_estimators=metadata.get("n_estimators"),
		trained_at=metadata.get("timestamp"),
		dataset_sha256=metadata.get("dataset_sha256"),
		metrics=metadata.get("metrics"),
	)
