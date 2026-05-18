from pathlib import Path

from src.core.integrity import compute_sha256, verify_integrity


class TestComputeSha256:
	def test_known_hash(self, tmp_path: Path):
		f = tmp_path / "file.bin"
		f.write_bytes(b"hello world")
		digest = compute_sha256(f)
		assert len(digest) == 64
		assert all(c in "0123456789abcdef" for c in digest)

	def test_deterministic(self, tmp_path: Path):
		f = tmp_path / "file.bin"
		f.write_bytes(b"deterministic content")
		assert compute_sha256(f) == compute_sha256(f)

	def test_different_content_different_hash(self, tmp_path: Path):
		a = tmp_path / "a.bin"
		b = tmp_path / "b.bin"
		a.write_bytes(b"content_a")
		b.write_bytes(b"content_b")
		assert compute_sha256(a) != compute_sha256(b)


class TestVerifyIntegrity:
	def test_valid_hash_returns_true(self, tmp_path: Path):
		f = tmp_path / "data.bin"
		f.write_bytes(b"secure data")
		digest = compute_sha256(f)
		assert verify_integrity(f, digest) is True

	def test_wrong_hash_returns_false(self, tmp_path: Path):
		f = tmp_path / "data.bin"
		f.write_bytes(b"secure data")
		assert verify_integrity(f, "deadbeef" * 8) is False

	def test_missing_file_returns_false(self, tmp_path: Path):
		missing = tmp_path / "nonexistent.bin"
		assert verify_integrity(missing, "abc123") is False

	def test_corrupted_file_detected(self, tmp_path: Path):
		f = tmp_path / "file.bin"
		f.write_bytes(b"original content")
		digest = compute_sha256(f)
		f.write_bytes(b"tampered content")
		assert verify_integrity(f, digest) is False
