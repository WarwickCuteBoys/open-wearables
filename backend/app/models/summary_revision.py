from datetime import date
from uuid import UUID

from sqlalchemy import Date, ForeignKey, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel


class SummaryRevision(BaseDbModel):
    __tablename__ = "summary_revision"

    user_id: Mapped[UUID] = mapped_column(ForeignKey("user.id", ondelete="CASCADE"), primary_key=True)
    day: Mapped[date] = mapped_column(Date, primary_key=True)
    generation: Mapped[UUID] = mapped_column(Uuid, nullable=False)
