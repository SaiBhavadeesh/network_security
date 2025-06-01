import sys
from networksecurity.logging import logger


class NetworkSecurityException(Exception):
    def __init__(self, error_message, error_details: sys) -> None:
        self.error_message = error_message
        _, _, exc_tb = error_details.exc_info()

        self.lineno = exc_tb.tb_lineno
        self.filename = exc_tb.tb_frame.f_code.co_filename

    def __str__(self) -> str:
        return f"Error occurred in python script name [{self.filename}] at line number [{self.lineno}] with error message [{str(self.error_message)}]"
