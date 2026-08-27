import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from src.core.models import Base, PredictionLog
from src.ml.drift import (
	check_probability_drift,
	compare_f1,
	reference_max_proba,
	served_max_proba,
)
from src.ml.train import train_classifier


def _write_metadata(path: Path, f1_macro: float) -> Path:
	path.write_text(json.dumps({"metrics": {"f1_macro": f1_macro}}), encoding="utf-8")
	return path


class TestCompareF1:
	def test_no_regression_on_stable_score(self, tmp_path: Path):
		previous = _write_metadata(tmp_path / "prev.json", 0.9833)
		current = _write_metadata(tmp_path / "curr.json", 0.9830)
		result = compare_f1(previous, current)
		assert result["regression"] is False
		assert result["drop"] == pytest.approx(0.0003)

	def test_regression_beyond_max_drop(self, tmp_path: Path):
		previous = _write_metadata(tmp_path / "prev.json", 0.9833)
		current = _write_metadata(tmp_path / "curr.json", 0.9600)
		result = compare_f1(previous, current)
		assert result["regression"] is True

	def test_improvement_is_not_a_regression(self, tmp_path: Path):
		previous = _write_metadata(tmp_path / "prev.json", 0.9600)
		current = _write_metadata(tmp_path / "curr.json", 0.9833)
		assert compare_f1(previous, current)["regression"] is False

	def test_drop_exactly_at_threshold_passes(self, tmp_path: Path):
		previous = _write_metadata(tmp_path / "prev.json", 0.98)
		current = _write_metadata(tmp_path / "curr.json", 0.97)
		assert compare_f1(previous, current, max_drop=0.01)["regression"] is False


class TestCheckProbabilityDrift:
	def test_same_distribution_is_not_drift(self):
		rng = np.random.default_rng(42)
		reference = rng.uniform(0.7, 1.0, 500)
		served = rng.uniform(0.7, 1.0, 500)
		assert check_probability_drift(served, reference)["drift_detected"] is False

	def test_shifted_distribution_is_drift(self):
		rng = np.random.default_rng(42)
		reference = rng.uniform(0.9, 1.0, 500)
		served = rng.uniform(0.3, 0.6, 500)
		result = check_probability_drift(served, reference)
		assert result["drift_detected"] is True
		assert result["statistic"] > result["threshold"]

	def test_result_carries_sample_sizes(self):
		result = check_probability_drift(np.full(60, 0.9), np.full(40, 0.9))
		assert result["n_served"] == 60
		assert result["n_reference"] == 40


class TestServedMaxProba:
	@pytest.fixture
	def engine(self, tmp_path: Path) -> Engine:
		engine = create_engine(f"sqlite:///{tmp_path / 'drift_test.db'}")
		Base.metadata.create_all(engine)
		return engine

	def _insert(self, engine: Engine, max_proba: float, sha: str) -> None:
		with Session(engine) as session:
			session.add(
				PredictionLog(
					features="{}", max_proba=max_proba, model_sha256=sha, predicted_class="STAR"
				)
			)
			session.commit()

	def test_reads_all_rows(self, engine: Engine):
		self._insert(engine, 0.97, "aaa")
		self._insert(engine, 0.55, "aaa")
		values = served_max_proba(engine)
		assert sorted(values.tolist()) == [0.55, 0.97]

	def test_filters_on_model_sha(self, engine: Engine):
		self._insert(engine, 0.97, "aaa")
		self._insert(engine, 0.55, "bbb")
		assert served_max_proba(engine, model_sha256="aaa").tolist() == [0.97]

	def test_empty_table_yields_empty_array(self, engine: Engine):
		assert len(served_max_proba(engine)) == 0


@pytest.fixture
def trained_dir(sample_curated_df: pd.DataFrame, tmp_path: Path) -> Path:
	data_file = tmp_path / "curated.parquet"
	sample_curated_df.to_parquet(data_file, index=False)
	train_classifier(data_path=data_file, output_dir=tmp_path, figures_dir=tmp_path)
	return tmp_path


class TestReferenceMaxProba:
	def test_returns_one_probability_per_test_row(self, trained_dir: Path):
		metadata = json.loads((trained_dir / "run_metadata.json").read_text(encoding="utf-8"))
		reference = reference_max_proba(trained_dir)
		assert len(reference) == metadata["test_rows"]
		assert bool(((reference > 0.0) & (reference <= 1.0)).all())

	def test_is_reproducible(self, trained_dir: Path):
		assert (
			reference_max_proba(trained_dir).tolist() == reference_max_proba(trained_dir).tolist()
		)

	def test_dataset_sha_mismatch_raises(self, trained_dir: Path, sample_curated_df: pd.DataFrame):
		metadata = json.loads((trained_dir / "run_metadata.json").read_text(encoding="utf-8"))
		sample_curated_df.iloc[:5].to_parquet(Path(metadata["dataset_path"]), index=False)
		with pytest.raises(ValueError, match="SHA mismatch"):
			reference_max_proba(trained_dir)


class TestMain:
	def _run(self, argv: list[str]) -> int:
		import sys
		from unittest.mock import patch

		from src.ml.drift import main

		with patch.object(sys, "argv", ["deepsky-drift", *argv]), pytest.raises(SystemExit) as exc:
			main()
		return int(exc.value.code or 0)

	def test_f1_exits_zero_without_regression(self, tmp_path: Path):
		previous = _write_metadata(tmp_path / "prev.json", 0.98)
		current = _write_metadata(tmp_path / "curr.json", 0.98)
		assert self._run(["f1", str(previous), str(current)]) == 0

	def test_f1_exits_one_on_regression(self, tmp_path: Path):
		previous = _write_metadata(tmp_path / "prev.json", 0.98)
		current = _write_metadata(tmp_path / "curr.json", 0.90)
		assert self._run(["f1", str(previous), str(current)]) == 1

	def test_proba_skips_below_min_served(self, trained_dir: Path, monkeypatch: pytest.MonkeyPatch):
		engine = create_engine(f"sqlite:///{trained_dir / 'main_test.db'}")
		Base.metadata.create_all(engine)
		import src.core.database as database

		monkeypatch.setattr(database, "get_engine", lambda: engine)
		assert self._run(["proba", "--model-dir", str(trained_dir)]) == 0

	def test_proba_exits_one_on_drifted_history(
		self, trained_dir: Path, monkeypatch: pytest.MonkeyPatch
	):
		engine = create_engine(f"sqlite:///{trained_dir / 'main_test.db'}")
		Base.metadata.create_all(engine)
		metadata = json.loads((trained_dir / "run_metadata.json").read_text(encoding="utf-8"))
		with Session(engine) as session:
			session.add_all(
				PredictionLog(
					features="{}",
					max_proba=0.34,
					model_sha256=metadata["model_sha256"],
					predicted_class="STAR",
				)
				for _ in range(60)
			)
			session.commit()
		import src.core.database as database

		monkeypatch.setattr(database, "get_engine", lambda: engine)
		assert self._run(["proba", "--model-dir", str(trained_dir)]) == 1
