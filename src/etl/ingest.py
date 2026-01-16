import os
import pandas as pd
from astroquery.sdss import SDSS
from astropy.table import Table

from src.core.models import CelestialBody, CelestialBodySchema
from src.utils.logger import setup_logger


# Initialize local logger
logger = setup_logger(__spec__.name)


class SDSSDataExtractor:
    """
    Handles data extraction from the SDSS database using high-level Python abstractions.
    No manual SQL required.
    """
    def __init__(self, data_release: int = 17):
        self.data_release = data_release

    def get_balanced_dataset(self, limit_per_class: int = 2000) -> pd.DataFrame:
        """
        Retrieves a balanced dataset of Stars, Galaxies, and Quasars.
        """
        logger.info(f"🚀 Initializing extraction: {limit_per_class} objects per class.")

        target_labels = ['STAR', 'GALAXY', 'QSO']
        collected_dfs = []

        for label in target_labels:
            df = self._fetch_class(label, limit_per_class)
            if not df.empty:
                collected_dfs.append(df)

        if not collected_dfs:
            logger.error("❌ No data could be retrieved. Check your internet connection or SDSS availability.")
            return pd.DataFrame()

        full_dataset = pd.concat(collected_dfs, ignore_index=True)
        self._validate_dataset(full_dataset)

        return full_dataset

    def _fetch_class(self, label: str, limit: int) -> pd.DataFrame:
        """
        Fetches data for a specific class using model-driven query generation.
        """
        logger.info(f"🔍 Fetching class: {label}...")

        # We leverage the model's ability to build its own SQL
        query = CelestialBody.build_sdss_query(limit=limit, label=label)

        try:
            result: Table | None = SDSS.query_sql(query, data_release=self.data_release)
            if result:
                return result.to_pandas()

            logger.warning(f"⚠️ Class {label} returned no results.")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ Extraction error for {label}: {e}")
            return pd.DataFrame()

    def _validate_dataset(self, df: pd.DataFrame):
        """
        Runs Pydantic validation on the entire dataframe to ensure schema compliance.
        """
        try:
            records = df.to_dict('records')
            [CelestialBodySchema.model_validate(r) for r in records]
            logger.info(f"✅ Data integrity verified: {len(df)} objects matches {CelestialBodySchema.__name__}")
        except Exception as e:
            logger.error(f"❌ Schema validation failed: {e}")


if __name__ == "__main__":
    extractor = SDSSDataExtractor()
    dataset = extractor.get_balanced_dataset(limit_per_class=2000)

    if not dataset.empty:
        os.makedirs("data", exist_ok=True)
        output_file = "data/sdss_raw.csv"
        dataset.to_csv(output_file, index=False)
        logger.info(f"💾 Success! Data saved to: {output_file}")
