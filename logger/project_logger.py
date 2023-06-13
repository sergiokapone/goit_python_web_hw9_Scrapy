import logging
import colorlog

color_formatter = colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
)


console_handler = logging.StreamHandler()
console_handler.setFormatter(color_formatter)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)