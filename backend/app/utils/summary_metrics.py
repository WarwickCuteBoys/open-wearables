from logging import getLogger
from time import monotonic
from typing import Any

from sqlalchemy import Engine, event

from app.utils.structured_logging import log_structured

logger = getLogger(__name__)


def install_summary_metrics(engine: Engine) -> None:
    @event.listens_for(engine, "before_cursor_execute")
    def before(conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
        if context.execution_options.get("summary_query"):
            context.summary_started = monotonic()

    @event.listens_for(engine, "after_cursor_execute")
    def after(conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
        if context.execution_options.get("summary_query"):
            log_structured(
                logger,
                "info",
                "Summary database query",
                event="summary_query",
                query_name=context.execution_options["summary_query"],
                elapsed_ms=round((monotonic() - context.summary_started) * 1000, 3),
            )
