
from abc import abstractmethod

class ClientDag:
    
    @abstractmethod
    def to_json(self) -> str:
        raise NotImplementedError("to_json not Implemented")
