import logging

def configured_logger(logger_name):
    """Function that sets configs to a logger
    Parameters
    ----------
        logger_name (str): Name of the logger.
    Returns
    -------
        logger (Logger): Logger Instance.
    """
    logging.basicConfig(
        filename="./logs/logs.log",
        encoding="utf-8",
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    logger = logging.getLogger(logger_name)

    return logger