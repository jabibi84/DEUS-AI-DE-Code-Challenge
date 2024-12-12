import logging


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Configurar handler de consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formato de los mensajes
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # Evitar múltiples handlers
    if not logger.handlers:
        logger.addHandler(console_handler)

    return logger