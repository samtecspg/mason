from typing import Optional

from mason.clients.response import Response
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger
from mason.resources.base import Resources

def apply(file: str, overwrite: bool = False, log_level: Optional[str] = None, env: Optional[MasonEnvironment] = None):
    environment: MasonEnvironment = env or MasonEnvironment().initialize()
    logger.set_level(log_level)
    response = Response()
    
    all = Resources(environment).get_all(file)
    for r in all:
        new_response = r.save(environment.state_store, overwrite, response)
        response = new_response
    return response.with_status()
