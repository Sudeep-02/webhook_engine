import structlog
import logging
import sys


def setup_logging():
    # 1. Setup the standard library to just pass through to stdout
    # We don't need a JSON formatter here because structlog will do it
    log_handler = logging.StreamHandler(sys.stdout)

    root_logger = logging.getLogger()
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)

    # 2. Configure structlog to handle the JSON formatting
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,  # Useful for OpenTelemetry later
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),  # This handles your JSONB perfectly
        ],
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()


logger = setup_logging()
