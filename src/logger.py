"""Structured logging configuration."""

import sys
import structlog
from src.config import settings


def get_logger(name: str):
    """Get a configured logger instance.

    Args:
        name: Logger name (usually __name__ of the module)

    Returns:
        Configured structlog logger
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging_level=settings.log_level.upper()
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(name)
