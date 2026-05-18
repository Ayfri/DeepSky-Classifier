import pandas as pd
import pytest

from src.ml.evaluate import evaluate_model


class TestEvaluateModel:
	def test_perfect_predictions(self):
		labels = ["GALAXY", "QSO", "STAR"]
		y = pd.Series(["STAR", "GALAXY", "QSO", "STAR", "GALAXY"])
		metrics = evaluate_model(y, y, labels=labels)
		assert metrics["accuracy"] == pytest.approx(1.0)
		assert metrics["f1_macro"] == pytest.approx(1.0)
		assert metrics["f1_weighted"] == pytest.approx(1.0)

	def test_output_keys_present(self):
		y_true = pd.Series(["STAR", "GALAXY", "QSO"])
		y_pred = pd.Series(["STAR", "GALAXY", "STAR"])
		metrics = evaluate_model(y_true, y_pred)
		assert {
			"accuracy",
			"f1_macro",
			"f1_weighted",
			"confusion_matrix",
			"classification_report",
		}.issubset(metrics.keys())

	def test_confusion_matrix_shape(self):
		labels = ["GALAXY", "QSO", "STAR"]
		y_true = pd.Series(labels * 3)
		y_pred = pd.Series(labels * 3)
		metrics = evaluate_model(y_true, y_pred, labels=labels)
		cm = metrics["confusion_matrix"]
		assert len(cm) == 3
		assert all(len(row) == 3 for row in cm)

	def test_partial_correct(self):
		y_true = pd.Series(["STAR", "GALAXY", "QSO", "STAR"])
		y_pred = pd.Series(["STAR", "GALAXY", "STAR", "STAR"])
		metrics = evaluate_model(y_true, y_pred)
		assert 0.0 < metrics["accuracy"] < 1.0

	def test_single_class(self):
		"""One-class edge case: metrics should not crash."""
		y = pd.Series(["STAR"] * 5)
		metrics = evaluate_model(y, y, labels=["STAR"])
		assert metrics["accuracy"] == pytest.approx(1.0)
