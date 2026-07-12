"""Regression tests for the two-stage sample that replaced the scan-order `TOP n` truncation.

`TOP n` without `ORDER BY` returns a contiguous slice of the catalog, not a draw: the galaxies
ended up confined to z in [0.015, 0.025] and the macro F1 hit a meaningless 1.000. These tests
lock the fix: plates are drawn at random (first stage), rows are drawn uniformly from the plate
pool (second stage), and neither stage may fall back to reading the pool in scan order.
"""

import numpy as np
import pandas as pd
import pytest

from src.etl.catalogs import sdss as sdss_module
from src.etl.catalogs.sdss import SDSSExtractor

AVAILABLE_PLATES = list(range(266, 266 + 500))


class FakeResult:
	"""Stands in for the astropy Table returned by `SDSS.query_sql`."""

	def __init__(self, frame: pd.DataFrame) -> None:
		self._frame = frame

	def __bool__(self) -> bool:
		return not self._frame.empty

	def to_pandas(self) -> pd.DataFrame:
		return self._frame


def plate_listing() -> FakeResult:
	return FakeResult(pd.DataFrame({"plate": AVAILABLE_PLATES}))


def class_pool(rows: int) -> FakeResult:
	"""A pool ordered by redshift, so a scan-order read is visible in the result."""
	return FakeResult(
		pd.DataFrame(
			{
				"objid": list(range(rows)),
				"redshift": np.linspace(0.0, 5.0, rows),
			},
		),
	)


class TestSamplePlates:
	def test_draw_is_reproducible_for_a_given_random_state(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		monkeypatch.setattr(sdss_module.SDSS, "query_sql", lambda *_, **__: plate_listing())

		first = SDSSExtractor(random_state=42)._sample_plates(60)
		second = SDSSExtractor(random_state=42)._sample_plates(60)

		assert first == second
		assert len(first) == 60
		assert set(first).issubset(AVAILABLE_PLATES)
		assert len(set(first)) == 60, "plates must be drawn without replacement"

	def test_draw_is_not_the_head_of_the_catalog(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		"""The bug that was fixed: taking the first n rows instead of sampling them."""
		monkeypatch.setattr(sdss_module.SDSS, "query_sql", lambda *_, **__: plate_listing())

		plates = SDSSExtractor(random_state=42)._sample_plates(60)

		assert plates != AVAILABLE_PLATES[:60]
		assert max(plates) > AVAILABLE_PLATES[60], "the draw must reach beyond the first plates"

	def test_another_random_state_draws_other_plates(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		monkeypatch.setattr(sdss_module.SDSS, "query_sql", lambda *_, **__: plate_listing())

		assert SDSSExtractor(random_state=42)._sample_plates(60) != SDSSExtractor(
			random_state=7,
		)._sample_plates(60)

	def test_caps_the_draw_at_the_number_of_available_plates(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		monkeypatch.setattr(sdss_module.SDSS, "query_sql", lambda *_, **__: plate_listing())

		assert len(SDSSExtractor()._sample_plates(10_000)) == len(AVAILABLE_PLATES)

	def test_returns_empty_when_the_plate_listing_fails(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		def boom(*_: object, **__: object) -> FakeResult:
			raise ConnectionError("SkyServer is down")

		monkeypatch.setattr(sdss_module.SDSS, "query_sql", boom)

		assert SDSSExtractor()._sample_plates(60) == []


class TestFetchClass:
	def test_pool_is_subsampled_uniformly_not_by_scan_order(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		monkeypatch.setattr(sdss_module.SDSS, "query_sql", lambda *_, **__: class_pool(1_000))

		sample = SDSSExtractor(random_state=42)._fetch_class("GALAXY", limit=100, plates=[266])

		assert len(sample) == 100
		head = class_pool(1_000).to_pandas().head(100)
		assert sample["objid"].tolist() != head["objid"].tolist()
		# A scan-order read of a redshift-ordered pool would span 10 % of the range at most.
		assert sample["redshift"].max() - sample["redshift"].min() > 4.0

	def test_subsample_is_reproducible(self, monkeypatch: pytest.MonkeyPatch) -> None:
		monkeypatch.setattr(sdss_module.SDSS, "query_sql", lambda *_, **__: class_pool(1_000))

		first = SDSSExtractor(random_state=42)._fetch_class("STAR", limit=100, plates=[266])
		second = SDSSExtractor(random_state=42)._fetch_class("STAR", limit=100, plates=[266])

		assert first["objid"].tolist() == second["objid"].tolist()

	def test_keeps_every_row_when_the_pool_is_smaller_than_the_limit(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		monkeypatch.setattr(sdss_module.SDSS, "query_sql", lambda *_, **__: class_pool(50))

		sample = SDSSExtractor()._fetch_class("QSO", limit=2_000, plates=[266])

		assert len(sample) == 50

	def test_returns_empty_frame_when_the_query_fails(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		def boom(*_: object, **__: object) -> FakeResult:
			raise TimeoutError("query timed out")

		monkeypatch.setattr(sdss_module.SDSS, "query_sql", boom)

		assert SDSSExtractor()._fetch_class("STAR", limit=100, plates=[266]).empty
