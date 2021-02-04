from abc import abstractmethod
from typing import Optional

from mason.state.base import MasonStateStore

class Saveable:
    
    @abstractmethod
    def __init__(self, source_path: Optional[str] = None):
        self.source_path = source_path

    @abstractmethod
    def save(self, state_store: MasonStateStore, overwrite: bool = False):
        raise Exception("Save not implemented")
