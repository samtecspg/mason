import traceback

def message(e: Exception):
    return ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
