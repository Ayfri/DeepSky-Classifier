from sqlalchemy import String, Float, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from pydantic import BaseModel, ConfigDict


class Base(DeclarativeBase):
    pass


class CelestialBody(Base):
    """
    SQLAlchemy model representing a celestial body from SDSS.
    """
    __tablename__ = "celestial_bodies"

    # Unique SDSS Identifier
    objid: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Coordinates
    ra: Mapped[float] = mapped_column(Float)   # Right Ascension
    dec: Mapped[float] = mapped_column(Float)  # Declination

    # Spectral Filters (Features)
    u: Mapped[float] = mapped_column(Float) # Ultraviolet
    g: Mapped[float] = mapped_column(Float) # Green (Visible)
    r: Mapped[float] = mapped_column(Float) # Red (Visible)
    i: Mapped[float] = mapped_column(Float) # Infrared
    z_mag: Mapped[float] = mapped_column(Float) # Far Infrared

    # Major Feature: Redshift (Indicates distance/velocity)
    redshift: Mapped[float] = mapped_column(Float)

    # Classification Target
    class_label: Mapped[str] = mapped_column(String(10))

    @classmethod
    def build_sdss_query(cls, limit: int, label: str, fields: list[str] | None = None) -> str:
        """
        Engineers a specialized SQL query for the SDSS database.
        Allows selecting specific fields or defaults to all fields defined in the model.
        """
        # Internal registry mapping model attributes to SDSS table notation
        registry = {
            "objid": "p.objid",
            "ra": "p.ra",
            "dec": "p.dec",
            "u": "p.u",
            "g": "p.g",
            "r": "p.r",
            "i": "p.i",
            "z_mag": "p.z AS z_mag",
            "redshift": "s.z AS redshift",
            "class_label": "s.class AS class_label"
        }

        # Determine which fields to select
        target_fields = fields if fields else list(registry.keys())
        selection = ", ".join([registry[f] for f in target_fields if f in registry])

        return f"""
        SELECT TOP {limit} {selection}
        FROM PhotoObj AS p
        JOIN SpecObj AS s ON s.bestobjid = p.objid
        WHERE s.zWarning = 0
          AND p.clean = 1
          AND s.class = '{label}'
        """.strip()


class CelestialBodySchema(BaseModel):
    """
    Pydantic schema for validation and API documentation.
    """
    model_config = ConfigDict(from_attributes=True)

    objid: int
    ra: float
    dec: float
    u: float
    g: float
    r: float
    i: float
    z_mag: float
    redshift: float
    class_label: str
