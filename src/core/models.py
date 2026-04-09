from sqlalchemy import Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
	pass


class CelestialBody(Base):
	"""Raw SDSS source record stored relationally."""
	__tablename__ = "celestial_bodies"

	class_label: Mapped[str] = mapped_column(String(10))
	dec: Mapped[float] = mapped_column(Float)
	g: Mapped[float] = mapped_column(Float)
	i: Mapped[float] = mapped_column(Float)
	objid: Mapped[int] = mapped_column(Integer, primary_key=True)
	r: Mapped[float] = mapped_column(Float)
	ra: Mapped[float] = mapped_column(Float)
	redshift: Mapped[float] = mapped_column(Float)
	u: Mapped[float] = mapped_column(Float)
	z_mag: Mapped[float] = mapped_column(Float)

	SDSS_FIELD_REGISTRY: dict[str, str] = {
		"class_label": "s.class AS class_label",
		"dec": "p.dec",
		"g": "p.g",
		"i": "p.i",
		"objid": "p.objid",
		"r": "p.r",
		"ra": "p.ra",
		"redshift": "s.z AS redshift",
		"u": "p.u",
		"z_mag": "p.z AS z_mag",
	}

	@classmethod
	def build_sdss_query(cls, limit: int, label: str, fields: list[str] | None = None) -> str:
		target_fields = fields if fields else sorted(cls.SDSS_FIELD_REGISTRY.keys())
		selection = ", ".join(
			cls.SDSS_FIELD_REGISTRY[f]
			for f in target_fields
			if f in cls.SDSS_FIELD_REGISTRY
		)

		return f"""
		SELECT TOP {limit} {selection}
		FROM PhotoObj AS p
		JOIN SpecObj AS s ON s.bestobjid = p.objid
		WHERE s.zWarning = 0
		  AND p.clean = 1
		  AND s.class = '{label}'
		""".strip()


class CuratedCelestialBody(Base):
	"""ML-ready curated record with optional enrichment from federated catalogs."""
	__tablename__ = "curated_celestial_bodies"

	class_label: Mapped[str] = mapped_column(String(10))
	dec: Mapped[float] = mapped_column(Float)
	g: Mapped[float] = mapped_column(Float)
	gaia_parallax: Mapped[float | None] = mapped_column(Float, nullable=True)
	gaia_pmdec: Mapped[float | None] = mapped_column(Float, nullable=True)
	gaia_pmra: Mapped[float | None] = mapped_column(Float, nullable=True)
	gaia_source_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
	i: Mapped[float] = mapped_column(Float)
	objid: Mapped[int] = mapped_column(Integer, primary_key=True)
	r: Mapped[float] = mapped_column(Float)
	ra: Mapped[float] = mapped_column(Float)
	redshift: Mapped[float] = mapped_column(Float)
	source: Mapped[str] = mapped_column(String(20), default="sdss")
	u: Mapped[float] = mapped_column(Float)
	wise_w1: Mapped[float | None] = mapped_column(Float, nullable=True)
	wise_w2: Mapped[float | None] = mapped_column(Float, nullable=True)
	z_mag: Mapped[float] = mapped_column(Float)
