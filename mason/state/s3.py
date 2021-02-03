from typing import Union

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.state.base import MasonStateStore
from mason.workflows.workflow import Workflow

class S3StateStore(MasonStateStore):

    def save(self, item: Union[Config, Workflow, Operator]):
        pass


