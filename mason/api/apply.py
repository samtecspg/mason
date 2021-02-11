from typing import Optional

from mason.clients.response import Response
from mason.resources.base import get_all
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger

def apply(file: str, overwrite: bool = False, log_level: Optional[str] = None, env: Optional[MasonEnvironment] = None):
    environment: MasonEnvironment = env or MasonEnvironment().initialize()
    logger.set_level(log_level)
    response = Response()
    
    all = get_all(environment, file)
    for r in all:
        r.save(environment.state_store, overwrite, response)
    return response.with_status()
