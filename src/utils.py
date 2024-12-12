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

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(console_handler)

    return logger


def get_dtype(df,colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]


logger = get_logger(__name__)


def get_config(file_path: str) -> dict:
    """
    Reads a configuration file in JSON format and parses it into a dictionary.

    Args:
        file_path (str): Path to the JSON configuration file.

    Returns:
        dict: Parsed configuration as a dictionary.
    """
    logger.info("Getting Dataset configuration.")
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        logger.info("Configuration File loaded successfully.")
        return config

    except FileNotFoundError:
        logger.error(f"Error: Configuration file not found at {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error: Failed to decode JSON. {e}")
        raise
    except Exception as e:
        logger.error(f"Error: Failed to decode JSON. {e}")
