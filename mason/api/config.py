from typing import Optional

from mason.api.api_printer import ApiPrinter
from mason.clients.response import Response
from mason.resources.base import Resources
from mason.resources.printer import Printer
from mason.util.environment import MasonEnvironment
from mason.api.get import get

def config(config_id: str, set_current: bool = False, log_level: Optional[str] = None, env: Optional[MasonEnvironment] = None, printer: Printer = ApiPrinter()):
    environment = env or MasonEnvironment().initialize()
    response = Response()
    if set_current:
        result = Resources(environment).set_session_config(config_id)
        if isinstance(result, str):
            response.add_error(result)
            response.set_status(404)
        else:
            response.add_info(f"Set session config to {config_id}")
    else:
        get("configs", config_id, None, log_level, None, printer)


