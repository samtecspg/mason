
from util.logger import logger

def banner(s: str):
    m = 2
    l = len(s) + (m +1)
    logger.info("+" + "-" * l + "+")
    logger.info("| " + s + " " * m + "|")
    logger.info("+" + "-" * l + "+")

