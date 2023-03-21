# TODO: Make a better logger

from wasabi import Printer

printer = Printer()


def log(msg, level="INFO", kill=False):
    """
    Log a message with a specified logging level and optionally raise a
    RuntimeError to stop program execution.

    The function supports three logging levels: INFO, WARN, and ``None``
    (default).

    Parameters:
    msg : str
        The message to be logged.
    level : str, optional
        The logging level, which can be "INFO", "WARN", or ``None``.
    kill : bool, optional
        If ``True``, raise a RuntimeError with the given message and stop
        program execution. Default is ``False``.

    Returns
    -------
    None
        The function logs the message and doesn't return any value.

    Raises
    ------
    RuntimeError
        If the parameter ``kill=True`` is set and the logging level is
        ``"WARN"``. The given message (``msg``) is provided as error message.
    """
    if level == "INFO":
        printer.info(f"Information: {msg}")
        return None

    if level == "WARN":
        if kill:
            raise RuntimeError(msg)

        printer.warn(f"Warning: {msg}")
        return None

    printer.good(msg)
    return None
