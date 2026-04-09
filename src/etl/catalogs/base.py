from abc import ABC, abstractmethod

import pandas as pd


class CatalogExtractor(ABC):
	"""Common interface for astronomical catalog extraction."""

	@property
	@abstractmethod
	def catalog_name(self) -> str: ...

	@abstractmethod
	def extract(self, **kwargs: object) -> pd.DataFrame:
		"""Return a raw DataFrame from the remote catalog."""
		...
