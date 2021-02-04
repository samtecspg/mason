from dataclasses import dataclass

from typistry.protos.invalid_object import InvalidObject

from mason.resources.saveable import Saveable
from mason.state.base import MasonStateStore
from mason.util.logger import logger

@dataclass
class InvalidResource(Saveable):
    invalid_obj: InvalidObject

    def save(self, state_store: MasonStateStore, overwrite: bool = False):
        logger.error(f"Invalid resource: {self.invalid_obj.message}: {self.invalid_obj.reference}")
