import logging

log_format = "{name} ({levelname:10}) :: {message}"
logging.basicConfig(format=log_format, style="{")
