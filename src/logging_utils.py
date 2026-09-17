import logging
from pathlib import Path


def setup_logger():
    """
    Configure and return the pipeline logger.
    """

    log_directory = Path("logs")
    log_directory.mkdir(exist_ok=True)

    log_file = log_directory / "pipeline.log"

    logger = logging.getLogger("ecommerce_pipeline")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if the logger is initialized more than once
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger