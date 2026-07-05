import logging
import sys
from typing import ClassVar, override

from colorama import Fore, Style, init

init(autoreset=True)


class DeepSkyFormatter(logging.Formatter):
	"""Colored log formatter: [timestamp] | LEVEL | module | message."""

	LEVEL_COLORS: ClassVar[dict[int, str]] = {
		logging.DEBUG: Fore.CYAN,
		logging.INFO: Fore.GREEN,
		logging.WARNING: Fore.YELLOW,
		logging.ERROR: Fore.RED,
		logging.CRITICAL: Fore.RED + Style.BRIGHT,
	}

	@override
	def format(self, record: logging.LogRecord) -> str:
		level_color = self.LEVEL_COLORS.get(record.levelno, "")
		dim = Style.DIM
		reset = Style.RESET_ALL

		date_str = f"{dim}[{self.formatTime(record, self.datefmt)}]{reset}"
		level_str = f"{level_color}{record.levelname:<8}{reset}"
		name_str = f"{Fore.MAGENTA}{record.name}{reset}"
		line = f"{date_str} [{level_str}] [{name_str:<30}] {record.getMessage()}"

		if record.exc_info:
			line = f"{line}\n{self.formatException(record.exc_info)}"
		if record.stack_info:
			line = f"{line}\n{self.formatStack(record.stack_info)}"

		return line


def setup_logger(name: str = "deepsky") -> logging.Logger:
	"""Return a named logger with the DeepSky colored formatter, idempotent."""
	logger = logging.getLogger(name)

	if not logger.handlers:
		logger.setLevel(logging.INFO)
		logger.propagate = False

		handler = logging.StreamHandler(sys.stdout)
		handler.setFormatter(DeepSkyFormatter(datefmt="%Y-%m-%d %H:%M:%S"))
		logger.addHandler(handler)

	return logger


logger = setup_logger()
