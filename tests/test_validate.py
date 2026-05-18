import pandas as pd

from src.core.schemas import SDSSRawRecord
from src.etl.validate import validate_dataframe


def _valid_rows(n: int = 5) -> list[dict]:
	return [
		{
			"class_label": "STAR",
			"dec": 45.0 + i,
			"g": 18.5,
			"i": 17.8,
			"objid": 1000 + i,
			"r": 18.1,
			"ra": 180.0 + i,
			"redshift": 0.001,
			"u": 19.2,
			"z_mag": 17.5,
		}
		for i in range(n)
	]


class TestValidateDataframe:
	def test_all_valid(self):
		df = pd.DataFrame(_valid_rows(10))
		valid, quarantine = validate_dataframe(df, SDSSRawRecord)
		assert len(valid) == 10
		assert quarantine.empty

	def test_invalid_class_quarantined(self):
		rows = _valid_rows(3)
		rows[1]["class_label"] = "PLANET"
		df = pd.DataFrame(rows)
		valid, quarantine = validate_dataframe(df, SDSSRawRecord)
		assert len(valid) == 2
		assert len(quarantine) == 1

	def test_quarantine_has_error_column(self):
		rows = _valid_rows(2)
		rows[0]["class_label"] = "COMET"
		df = pd.DataFrame(rows)
		_, quarantine = validate_dataframe(df, SDSSRawRecord)
		assert "_validation_errors" in quarantine.columns

	def test_empty_dataframe(self):
		df = pd.DataFrame(columns=list(_valid_rows(1)[0].keys()))
		valid, quarantine = validate_dataframe(df, SDSSRawRecord)
		assert valid.empty
		assert quarantine.empty

	def test_mixed_batch(self):
		rows = _valid_rows(5)
		rows[2]["class_label"] = "INVALID"
		rows[4]["class_label"] = "ALSO_BAD"
		df = pd.DataFrame(rows)
		valid, quarantine = validate_dataframe(df, SDSSRawRecord)
		assert len(valid) == 3
		assert len(quarantine) == 2
