# TODO: Make a better logger

from wasabi import Printer

printer = Printer()


def log(msg, level="INFO", kill=False):
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
