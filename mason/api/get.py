from typing import Optional

from mason.util.environment import MasonEnvironment
from mason.util.logger import logger

from mason.resources.printer import Printer
from mason.api.api_printer import ApiPrinter
from mason.resources import base

def get(resource_type: Optional[str], namespace: Optional[str] = None, command: Optional[str] = None, log_level: Optional[str] = "info", env: Optional[MasonEnvironment] = None,  printer: Printer = ApiPrinter()):
    environment: MasonEnvironment = env or MasonEnvironment().initialize()
    logger.set_level(log_level)
    resource_type = resource_type or "all"
    res = base.Resources(environment)
    
    all = res.get_resources(resource_type, namespace, command)
    response = printer.print_resources(all, resource_type, namespace, command)
    
    return response.with_status() 