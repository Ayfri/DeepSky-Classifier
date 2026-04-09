from typing import Any

import pandas as pd
from sklearn.metrics import (
	accuracy_score,
	classification_report,
	confusion_matrix,
	f1_score,
)

from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def evaluate_model(
	y_true: pd.Series,
	y_pred: pd.Series,
	labels: list[str] | None = None,
) -> dict[str, Any]:
	accuracy = accuracy_score(y_true, y_pred)
	f1_macro = f1_score(y_true, y_pred, average="macro")
	f1_weighted = f1_score(y_true, y_pred, average="weighted")
	cm = confusion_matrix(y_true, y_pred, labels=labels)
	report = classification_report(y_true, y_pred, labels=labels, output_dict=True)

	metrics: dict[str, Any] = {
		"accuracy": float(accuracy),
		"classification_report": report,
		"confusion_matrix": cm.tolist(),
		"f1_macro": float(f1_macro),
		"f1_weighted": float(f1_weighted),
	}

	logger.info(
		f"Evaluation: accuracy={accuracy:.4f}, "
		f"F1_macro={f1_macro:.4f}, F1_weighted={f1_weighted:.4f}"
	)
	return metrics
