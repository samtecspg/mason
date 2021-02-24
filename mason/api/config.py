from typing import Optional

from mason.api.api_printer import ApiPrinter
from mason.clients.response import Response
from mason.resources.base import Resources
from mason.resources.printer import Printer
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger

def config(config_id: Optional[str], set_current: bool = False, log_level: Optional[str] = None, env: Optional[MasonEnvironment] = None, printer: Printer = ApiPrinter()):
    environment = env or MasonEnvironment().initialize()
    logger.set_level(log_level)
    response = Response()
    if set_current and config_id:
        result = Resources(environment).set_session_config(config_id)
        if isinstance(result, str):
            response.add_error(result)
            response.set_status(404)
        else:
            response.add_info(f"Set session config to {config_id}")
            config_id = None
        
    res = Resources(environment)
    configs = res.get_resources("config", config_id)
    response = printer.print_resources(configs, "config", config_id, environment=environment)
    return response.with_status() 


