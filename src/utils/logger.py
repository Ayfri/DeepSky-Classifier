import logging
import sys
from colorama import init, Fore, Style


# Initialize colorama for Windows support
init(autoreset=True)


class DeepSkyFormatter(logging.Formatter):
    """
    A professional-grade colored formatter for DeepSky-Classifier.
    Formats: [YYYY-MM-DD HH:MM:SS] | LEVEL | LOGGER | Message
    """
    LEVEL_COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        # Color for the log level
        level_color = self.LEVEL_COLORS.get(record.levelno, "")

        # Dim colors for metadata to keep focus on the level and message
        dim = Style.DIM
        reset = Style.RESET_ALL

        # Build the metadata parts
        date_str = f"{dim}[{self.formatTime(record, self.datefmt)}]{reset}"
        level_str = f"{level_color}{record.levelname:<8}{reset}"
        name_str = f"{Fore.MAGENTA}{record.name}{reset}"

        # Format the message
        message = record.getMessage()

        return f"{date_str} [{level_str}] [{name_str:<30}] {message}"


def setup_logger(name: str = "deepsky"):
    """
    Configures a logger with the DeepSky high-integrity formatter.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Prevent duplicate logs if common root is used
        logger.propagate = False

        handler = logging.StreamHandler(sys.stdout)
        formatter = DeepSkyFormatter(datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Singleton instance for general use
logger = setup_logger()
