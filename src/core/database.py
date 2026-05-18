from collections.abc import Generator

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.core.models import Base
from src.core.settings import Settings, get_settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def get_engine(settings: Settings | None = None) -> Engine:
	"""Create a SQLAlchemy engine from runtime settings.

	Uses PostgreSQL (psycopg) when DB environment variables are set;
	falls back to SQLite for local development and tests with no config.
	"""
	cfg = settings or get_settings()
	url = cfg.effective_db_url

	engine_kwargs: dict = {"echo": False}
	if url.startswith("postgresql"):
		engine_kwargs["pool_size"] = cfg.db_pool_size
		engine_kwargs["max_overflow"] = cfg.db_max_overflow
		engine_kwargs["pool_pre_ping"] = True

	engine = create_engine(url, **engine_kwargs)
	logger.info(f"Database engine created: {engine.url}")
	return engine


def init_schema(engine: Engine) -> None:
	"""Create all ORM tables if they do not already exist."""
	Base.metadata.create_all(engine)
	logger.info("Database schema initialised")


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
	return sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_session(engine: Engine) -> Generator[Session]:
	"""Yield a transactional session, rolling back on error."""
	factory = get_session_factory(engine)
	session = factory()
	try:
		yield session
		session.commit()
	except Exception:
		session.rollback()
		raise
	finally:
		session.close()


def ping(engine: Engine) -> bool:
	"""Return True if the database is reachable."""
	try:
		with engine.connect() as conn:
			conn.execute(text("SELECT 1"))
		return True
	except Exception as exc:
		logger.warning(f"Database ping failed: {exc}")
		return False
