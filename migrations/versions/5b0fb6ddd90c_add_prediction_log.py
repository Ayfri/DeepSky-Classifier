"""add prediction_log

Revision ID: 5b0fb6ddd90c
Revises: bb24d4d489d1
Create Date: 2026-08-26 13:36:48.537379

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "5b0fb6ddd90c"
down_revision: str | Sequence[str] | None = "bb24d4d489d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
	"""Upgrade schema."""
	op.create_table(
		"prediction_log",
		sa.Column("features", sa.Text(), nullable=False),
		sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
		sa.Column("max_proba", sa.Float(), nullable=False),
		sa.Column("model_sha256", sa.String(length=64), nullable=False),
		sa.Column("predicted_class", sa.String(length=10), nullable=False),
		sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
		sa.PrimaryKeyConstraint("id"),
	)


def downgrade() -> None:
	"""Downgrade schema."""
	op.drop_table("prediction_log")
