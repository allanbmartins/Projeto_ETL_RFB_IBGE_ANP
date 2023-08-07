# Fonte - https://williamcanin.dev/blog/implementando-uma-classe-em-python-para-criar-logs/

import logging
from sys import platform


def unix_color(value):
    if platform.startswith("win"):
        return ""
    return value


class Colors:
    NONE = unix_color("\x1b[0m")
    BLACK = unix_color("\x1b[30m")
    MAGENTA = unix_color("\x1b[95m")
    BLUE = unix_color("\x1b[94m")
    GREEN = unix_color("\x1b[92m")
    RED = unix_color("\x1b[91m")
    YELLOW = unix_color("\x1b[93m")
    CYAN = unix_color("\x1b[96m")
    WHITE = unix_color("\x1b[97m")


class Logs(Colors):

    FILENAME = 'logs.log'  # "mylogs.log"
    DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"

    def __init__(self, filename=FILENAME, datefmt=DATE_FORMAT):
        self.filename = filename
        self.date_format = datefmt
        self.formated = "%(levelname)s:[%(asctime)s]: %(message)s"

        self.levels = {
            "exception": logging.exception,
            "info": logging.info,
            "warning": logging.warning,
            "error": logging.error,
            "debug": logging.debug,
            "critical": logging.critical
        }

    def record(self, msg, *args, exc_info=True, type="exception", colorize=False,
               **kwargs):
        for item in self.levels.keys():
            if item == type:
                if not colorize:
                    formated = self.formated
                else:
                    if item == "warning":
                        formated = (
                            f"{self.YELLOW}%(levelname)s:{self.GREEN}[%(asctime)s]"
                            f"{self.NONE}: %(message)s"
                        )
                    elif item == "error" or item == "exception":
                        formated = (
                            f"{self.RED}%(levelname)s:{self.GREEN}[%(asctime)s]"
                            f"{self.NONE}: %(message)s"
                        )
                    else:
                        formated = (
                            f"{self.CYAN}%(levelname)s:{self.GREEN}[%(asctime)s]"
                            f"{self.NONE}: %(message)s"
                        )
                logging.basicConfig(filename=self.filename, format=formated,
                                    datefmt=self.date_format, level=logging.INFO)
                if item == "exception":
                    return self.levels[item](msg, *args, exc_info=exc_info,
                                             **kwargs)
                else:
                    return self.levels[item](msg, *args, **kwargs)
        raise ValueError(
            f'Error implementing the method "{self.record.__name__}" in class Logs.'
        )
