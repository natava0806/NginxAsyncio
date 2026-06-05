import logging
import sys


def setup_logger(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("MiniNginx")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Избегание дублирования логов, если логгер уже инициализирован
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


# Экспорт дефолтного логгера
logger = setup_logger()
