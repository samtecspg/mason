from abc import abstractmethod
from typing import Optional

from returns.result import ResultE

from mason.clients.response import Response
from mason.state.base import MasonStateStore

class Saveable:
    
    @abstractmethod
    def __init__(self, source_path: Optional[str] = None):
        self.source_path = source_path

    @abstractmethod
    def save(self, state_store: MasonStateStore, overwrite: bool = False, response: Response = Response()) -> ResultE[Response]:
        raise Exception("Save not implemented")
