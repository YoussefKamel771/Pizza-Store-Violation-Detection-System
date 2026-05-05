import logging
import enum
import sys

LOG_FORMAT_DEBUG = "%(levelname)s:%(message)s:%(pathname)s:%(funcName)s:%(lineno)d"

class LogLevels(str, enum.Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"
    WARNING = "WARNING"
    ERROR = "ERROR"

def configure_logging(log_level: str | LogLevels = LogLevels.INFO):
    # Normalize: extract value from enum or uppercase string
    if isinstance(log_level, LogLevels):
        log_level = log_level.value
    else:
        log_level = str(log_level).upper()

    log_levels = [level.value for level in LogLevels]

    if log_level not in log_levels:
        logging.basicConfig(level=logging.ERROR, force=True)
        logging.error("Invalid log level '%s', defaulting to ERROR", log_level)
        return

    if log_level == LogLevels.DEBUG.value:
        logging.basicConfig(
            level=log_level,
            format=LOG_FORMAT_DEBUG,
            handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
            force=True  # override any previous config
        )
        return

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler("app.log"),
            logging.StreamHandler(sys.stdout)
        ],
        force=True  # ← ensures this config wins even if called before
    )