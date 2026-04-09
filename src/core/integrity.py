import hashlib
from pathlib import Path

from src.utils.logger import setup_logger


logger = setup_logger(__name__)

BLOCK_SIZE = 65536


def compute_sha256(filepath: Path) -> str:
	h = hashlib.sha256()
	with open(filepath, "rb") as f:
		for chunk in iter(lambda: f.read(BLOCK_SIZE), b""):
			h.update(chunk)
	return h.hexdigest()


def verify_integrity(filepath: Path, expected_sha256: str) -> bool:
	if not filepath.exists():
		logger.critical(f"File not found: {filepath}")
		return False

	computed = compute_sha256(filepath)
	if computed == expected_sha256:
		logger.info(f"Integrity verified: {filepath}")
		return True

	logger.error(
		f"Integrity check failed for {filepath}: "
		f"expected {expected_sha256}, got {computed}"
	)
	return False
