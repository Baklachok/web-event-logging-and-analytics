import logging

_LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    """
    Инициализация базовой конфигурации логирования.

    Вызывается из app.py один раз.
    """
    logging.basicConfig(
        level=level,
        format=_LOG_FORMAT,
    )


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)  # ← добавь это
    logger.propagate = True  # ← тоже важно
    return logger
