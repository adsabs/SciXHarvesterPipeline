"""harvester_records_table

Revision ID: 2ccc7651e959
Revises: 6bfebb4af6ba
Create Date: 2023-02-08 16:19:29.622107

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '2ccc7651e959'
down_revision = '6bfebb4af6ba'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('harvester_records',
    sa.Column('id', postgresql.UUID(), nullable=False),
    sa.Column('s3_key', sa.String(), nullable=True),
    sa.Column('date', sa.DateTime(), nullable=True),
    sa.Column('etag', sa.String(), nullable=True),
    sa.Column('source', sa.Enum('ArXiV', 'APS', 'AAS', 'MNRAS', 'PNAAS', name='source'), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('harvester_records')
    # ### end Alembic commands ###