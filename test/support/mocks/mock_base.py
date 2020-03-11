
from abc import abstractmethod

class MockBase:

    @abstractmethod
    def method(self, operation_name, kwarg):
        raise NotImplementedError("Mock methods not implemented")


