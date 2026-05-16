import logging
import sys
from datetime import datetime


OK_LEVEL = 25
logging.addLevelName(OK_LEVEL, "OK")


def ok(self, message, *args, **kwargs):
    if self.isEnabledFor(OK_LEVEL):
        self._log(OK_LEVEL, message, args, **kwargs)


logging.Logger.ok = ok


class PlainFormatter(logging.Formatter):
    LEVEL_PREFIX = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARN",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
        OK_LEVEL: "OK",
    }

    def format(self, record):
        prefix = self.LEVEL_PREFIX.get(record.levelno, record.levelname)
        return f"[{prefix}] {record.getMessage()}"


def get_logger(name: str = "binance_funding") -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(PlainFormatter())
        log.addHandler(handler)
    return log


def divider(name: str = "", sep: str = "=", display_time: bool = True) -> None:
    stamp = f" {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC" if display_time else ""
    text = f" {name}{stamp} "
    width = max(12, (72 - len(text)) // 2)
    logger.info(f"{sep * width}{text}{sep * width}")


logger = get_logger()
