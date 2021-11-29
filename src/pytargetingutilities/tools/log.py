import logging
import graypy
import sys


def setup_logging(
   name,
   loglevel
):
    """
    Configure a named logger
    Args:
        name (str): name of logger
        loglevel (int): log severity (DEBUG,INFO etc.)

    Returns:
    logger instance

    """
    return setup_logging_graylog(name, loglevel, False, "", 0)


def setup_logging_graylog(
   name,
   loglevel,
   use_graylog,
   graylog_host,
   graylog_port,
):
    """
    Configure a named logger
    Args:
        name (str): name of logger
        loglevel (int): log severity (DEBUG,INFO etc.)
        use_graylog (bool): enables graylog gelf logging
        graylog_host (str); IP/Host of graylog server
        graylog_port (int): Port of graylog server

    Returns:
    logger instance with graylog handler

    """
    logger = logging.getLogger(name)
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel,
        stream=sys.stdout,
        format=logformat,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if use_graylog:
        handler = graypy.GELFUDPHandler(graylog_host, graylog_port)
        logger.addHandler(handler)
        logger.setLevel(loglevel)
    return logger
