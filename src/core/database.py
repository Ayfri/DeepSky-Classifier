from pathlib import Path

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session, sessionmaker

from src.core.models import Base
from src.utils.logger import setup_logger


logger = setup_logger(__name__)

DEFAULT_DB_PATH = Path("data") / "deepsky.db"


def get_engine(url: str | None = None) -> Engine:
	if url is None:
		DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
		url = f"sqlite:///{DEFAULT_DB_PATH}"

	engine = create_engine(url, echo=False)
	logger.info(f"Database engine created: {engine.url}")
	return engine


def init_schema(engine: Engine) -> None:
	Base.metadata.create_all(engine)
	logger.info("Database schema initialized")


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
	return sessionmaker(bind=engine)
