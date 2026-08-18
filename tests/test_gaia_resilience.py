from pathlib import Path

import httpx2
import pandas as pd
import pytest

from src.etl.catalogs import gaia
from src.etl.catalogs.gaia import GaiaExtractor


class _FakeTable:
	def to_pandas(self) -> str:
		return "converted"


class _FakeResult:
	def to_table(self) -> _FakeTable:
		return _FakeTable()


class _FakeJob:
	def __init__(self, fail_times: int) -> None:
		self.fail_times = fail_times
		self.calls = 0

	def fetch_result(self) -> _FakeResult:
		self.calls += 1
		if self.calls <= self.fail_times:
			raise ConnectionError("Remote end closed connection without response")
		return _FakeResult()


class TestFetchResultWithRetry:
	def test_succeeds_on_first_attempt(self, monkeypatch: pytest.MonkeyPatch) -> None:
		monkeypatch.setattr(gaia.time, "sleep", lambda _: None)
		job = _FakeJob(fail_times=0)
		assert gaia._fetch_result_with_retry(job).to_pandas() == "converted"
		assert job.calls == 1

	def test_succeeds_after_transient_failures(self, monkeypatch: pytest.MonkeyPatch) -> None:
		monkeypatch.setattr(gaia.time, "sleep", lambda _: None)
		job = _FakeJob(fail_times=gaia.FETCH_RESULT_MAX_ATTEMPTS - 1)
		assert gaia._fetch_result_with_retry(job).to_pandas() == "converted"
		assert job.calls == gaia.FETCH_RESULT_MAX_ATTEMPTS

	def test_raises_after_exhausting_attempts(self, monkeypatch: pytest.MonkeyPatch) -> None:
		monkeypatch.setattr(gaia.time, "sleep", lambda _: None)
		job = _FakeJob(fail_times=gaia.FETCH_RESULT_MAX_ATTEMPTS)
		with pytest.raises(ConnectionError):
			gaia._fetch_result_with_retry(job)
		assert job.calls == gaia.FETCH_RESULT_MAX_ATTEMPTS


class TestCreateAuthenticatedSession:
	def test_returns_session_with_cookies(self, monkeypatch: pytest.MonkeyPatch) -> None:
		def fake_post(self: httpx2.Client, url: str, data: dict[str, str]) -> httpx2.Response:
			request = httpx2.Request("POST", url)
			return httpx2.Response(
				200, request=request, headers={"set-cookie": "JSESSIONID=abc123"}
			)

		monkeypatch.setattr(httpx2.Client, "post", fake_post)
		session = gaia._create_authenticated_session("proy01", "secret")
		assert session.cookies.get("JSESSIONID") == "abc123"

	def test_raises_when_no_cookies_returned(self, monkeypatch: pytest.MonkeyPatch) -> None:
		def fake_post(self: httpx2.Client, url: str, data: dict[str, str]) -> httpx2.Response:
			request = httpx2.Request("POST", url)
			return httpx2.Response(200, request=request)

		monkeypatch.setattr(httpx2.Client, "post", fake_post)
		with pytest.raises(RuntimeError, match="session cookie"):
			gaia._create_authenticated_session("proy01", "secret")


def _make_targets(n: int) -> pd.DataFrame:
	return pd.DataFrame(
		{
			"objid": list(range(1, n + 1)),
			"ra": [float(i) for i in range(n)],
			"dec": [float(i) for i in range(n)],
		}
	)


def _fake_batch_result(objids: list[int]) -> pd.DataFrame:
	return pd.DataFrame(
		{
			"objid": objids,
			"source_id": [1000 + o for o in objids],
			"parallax": [1.0] * len(objids),
			"pmdec": [0.1] * len(objids),
			"pmra": [0.2] * len(objids),
			"phot_g_mean_mag": [15.0] * len(objids),
			"match_sep_arcsec": [0.5] * len(objids),
		}
	)


class TestExtractForTargetsCheckpointing:
	def test_first_run_checkpoints_each_batch(
		self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
	) -> None:
		calls: list[str] = []

		def fake_run_query(
			self: GaiaExtractor,
			query: str,
			job_label: str = "",
			uploads: dict[str, object] | None = None,
		) -> pd.DataFrame:
			calls.append(job_label)
			objids = list(uploads["sdss_targets"]["objid"])  # type: ignore[index]
			return _fake_batch_result(objids)

		monkeypatch.setattr(GaiaExtractor, "_run_query", fake_run_query)
		extractor = GaiaExtractor()
		checkpoint_dir = tmp_path / "checkpoints"

		result = extractor._extract_for_targets(
			_make_targets(5), batch_size=2, max_sep_arcsec=1.5, checkpoint_dir=checkpoint_dir
		)

		assert len(calls) == 3
		assert len(result) == 5
		assert sorted(p.name for p in checkpoint_dir.glob("*.parquet")) == [
			"batch_001.parquet",
			"batch_002.parquet",
			"batch_003.parquet",
		]

	def test_resume_skips_already_checkpointed_batches(
		self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
	) -> None:
		calls: list[str] = []

		def fake_run_query(
			self: GaiaExtractor,
			query: str,
			job_label: str = "",
			uploads: dict[str, object] | None = None,
		) -> pd.DataFrame:
			calls.append(job_label)
			objids = list(uploads["sdss_targets"]["objid"])  # type: ignore[index]
			return _fake_batch_result(objids)

		monkeypatch.setattr(GaiaExtractor, "_run_query", fake_run_query)
		extractor = GaiaExtractor()
		checkpoint_dir = tmp_path / "checkpoints"
		checkpoint_dir.mkdir()
		_fake_batch_result([1, 2]).to_parquet(checkpoint_dir / "batch_001.parquet")

		result = extractor._extract_for_targets(
			_make_targets(5), batch_size=2, max_sep_arcsec=1.5, checkpoint_dir=checkpoint_dir
		)

		assert calls == ["[Gaia] TAP job 2/3", "[Gaia] TAP job 3/3"]
		assert len(result) == 5

	def test_failed_batch_is_not_checkpointed_and_does_not_abort_others(
		self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
	) -> None:
		def fake_run_query(
			self: GaiaExtractor,
			query: str,
			job_label: str = "",
			uploads: dict[str, object] | None = None,
		) -> pd.DataFrame:
			if "2/3" in job_label:
				raise RuntimeError("[Gaia] Job ended with phase: ERROR")
			objids = list(uploads["sdss_targets"]["objid"])  # type: ignore[index]
			return _fake_batch_result(objids)

		monkeypatch.setattr(GaiaExtractor, "_run_query", fake_run_query)
		extractor = GaiaExtractor()
		checkpoint_dir = tmp_path / "checkpoints"

		result = extractor._extract_for_targets(
			_make_targets(5), batch_size=2, max_sep_arcsec=1.5, checkpoint_dir=checkpoint_dir
		)

		assert (checkpoint_dir / "batch_001.parquet").exists()
		assert not (checkpoint_dir / "batch_002.parquet").exists()
		assert (checkpoint_dir / "batch_003.parquet").exists()
		assert len(result) == 3
