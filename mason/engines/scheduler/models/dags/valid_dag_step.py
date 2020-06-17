from functools import total_ordering
from typing import List, Optional

from mason.clients.response import Response
from mason.operators.operator_response import OperatorResponse
from mason.operators.valid_operator import ValidOperator
from mason.util.environment import MasonEnvironment

@total_ordering
class ValidDagStep:

    def __init__(self, id: str, operator: ValidOperator, dependencies: List[str], retry_method: Optional[str], retry_max: Optional[int] = None):
        self.id = id
        self.operator = operator
        self.dependencies = dependencies
        self.retry_method = retry_method
        self.retry_max = retry_max or 7  #(for exponential 5 seconds  * (2^7) is roughly 10 minutes)
        self.retries = 1
        
    def __ge__(self, other: 'ValidDagStep'):
        return self.id > other.id

    def __le__(self, other: 'ValidDagStep'):
        return not self.__ge__(other) 

    def run(self, env: MasonEnvironment, response: Response) -> OperatorResponse:
        return self.operator.run(env, response)
    
    def retryable(self):
        return (self.retries < self.retry_max)
