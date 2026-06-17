from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import numpy.typing as npt
import pandas as pd
import seaborn as sns
from sklearn.metrics import auc, roc_curve
from sklearn.preprocessing import label_binarize

from src.utils.logger import setup_logger

plt.switch_backend("Agg")
logger = setup_logger(__name__)


def plot_confusion_matrix(cm: list[list[int]], labels: list[str], output_path: Path) -> None:
	output_path.parent.mkdir(parents=True, exist_ok=True)
	fig, ax = plt.subplots(figsize=(6, 5))
	sns.heatmap(
		cm, annot=True, fmt="d", xticklabels=labels, yticklabels=labels, ax=ax, cmap="Blues"
	)
	ax.set_xlabel("Predicted")
	ax.set_ylabel("True")
	ax.set_title("Confusion Matrix")
	fig.savefig(output_path, dpi=150, bbox_inches="tight")
	plt.close(fig)
	logger.info(f"Confusion matrix ->{output_path}")


def plot_roc_curves(
	y_true: pd.Series,
	y_score: npt.NDArray[np.float64],
	labels: list[str],
	output_path: Path,
) -> None:
	y_bin = np.asarray(label_binarize(y_true, classes=labels))
	output_path.parent.mkdir(parents=True, exist_ok=True)
	fig, ax = plt.subplots(figsize=(7, 5))
	for i, label in enumerate(labels):
		fpr, tpr, _ = roc_curve(y_bin[:, i], y_score[:, i])
		roc_auc = auc(fpr, tpr)
		ax.plot(fpr, tpr, label=f"{label} (AUC={roc_auc:.3f})")
	ax.plot([0, 1], [0, 1], "k--", linewidth=0.8)
	ax.set_xlabel("False Positive Rate")
	ax.set_ylabel("True Positive Rate")
	ax.set_title("ROC Curves (OVR)")
	ax.legend()
	fig.savefig(output_path, dpi=150, bbox_inches="tight")
	plt.close(fig)
	logger.info(f"ROC curves ->{output_path}")


def plot_feature_importance(
	importances: npt.NDArray[np.float64],
	feature_names: list[str],
	output_path: Path,
) -> None:
	pairs = sorted(zip(importances.tolist(), feature_names, strict=True))
	names = [p[1] for p in pairs]
	imps = [p[0] for p in pairs]
	output_path.parent.mkdir(parents=True, exist_ok=True)
	fig, ax = plt.subplots(figsize=(7, 4))
	ax.barh(names, imps)
	ax.set_xlabel("Importance")
	ax.set_title("Feature Importance")
	fig.savefig(output_path, dpi=150, bbox_inches="tight")
	plt.close(fig)
	logger.info(f"Feature importance ->{output_path}")
