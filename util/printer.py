
from util.logger import logger

def banner(s: str, level: str = "info"):
    m = 2
    l = len(s) + (m +1)
    logger.at_level(level, ("+" + "-" * l + "+"))
    logger.at_level(level, ("| " + s + " " * m + "|"))
    logger.at_level(level, ("+" + "-" * l + "+"))

