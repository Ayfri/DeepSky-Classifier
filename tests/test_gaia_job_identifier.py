import pandas as pd
import pytest
from astropy.table import Table

from src.etl.catalogs.gaia import (
	_arcsec_to_degrees,
	_build_target_query,
	_get_job_identifier,
	_iter_target_batches,
	_prepare_target_upload,
	_select_nearest_matches,
)


def test_get_job_identifier_prefers_job_id() -> None:
	job = type("J", (), {"job_id": "primary", "jobid": "fallback"})()
	assert _get_job_identifier(job) == "primary"


def test_get_job_identifier_falls_back_to_jobid() -> None:
	job = type("J", (), {"jobid": "uws-42"})()
	assert _get_job_identifier(job) == "uws-42"


def test_get_job_identifier_coerces_to_str() -> None:
	job = type("J", (), {"job_id": 99})()
	assert _get_job_identifier(job) == "99"


def test_get_job_identifier_missing_raises() -> None:
	with pytest.raises(AttributeError, match="job identifier"):
		_get_job_identifier(object())


def test_build_target_query_uses_uploaded_table() -> None:
	query = _build_target_query("sdss_targets", max_sep_arcsec=1.5)
	assert "FROM TAP_UPLOAD.sdss_targets AS targets" in query
	assert "targets.objid" in query
	assert "match_sep_arcsec" in query
	assert str(_arcsec_to_degrees(1.5)) in query


def test_select_nearest_matches_keeps_closest_source_per_objid() -> None:
	matches = pd.DataFrame(
		{
			"match_sep_arcsec": [0.8, 0.2, 0.4],
			"objid": [1, 1, 2],
			"source_id": [10, 11, 20],
		},
	)

	selected = _select_nearest_matches(matches)

	assert list(selected["objid"]) == [1, 2]
	assert list(selected["source_id"]) == [11, 20]


def test_iter_target_batches_splits_upload_into_offsets() -> None:
	upload = _prepare_upload_for_batch_test(5)

	batches = _iter_target_batches(upload, batch_size=2)

	assert [(index, start, len(batch)) for index, start, batch in batches] == [
		(1, 0, 2),
		(2, 2, 2),
		(3, 4, 1),
	]


def _prepare_upload_for_batch_test(size: int) -> Table:
	frame = pd.DataFrame(
		{
			"dec": [float(i) for i in range(size)],
			"objid": [100 + i for i in range(size)],
			"ra": [float(i) + 0.5 for i in range(size)],
		},
	)
	return _prepare_target_upload(frame)
