from typing import List, Optional, Union

from util.list import flatten
from util.logger import logger
from workflows.dags.dag_step import DagStep, from_dict

class Dag:

    def __init__(self, steps: List[DagStep]):
        self.steps = steps

    def validate(self):
        #  TODO: validate that it is indeed a DAG
        return True

def validate_dag(dag_config: List[dict]) -> Optional[Dag]:
    steps = flatten(list(map(lambda step: from_dict(step), dag_config)))
    dag = Dag(steps)
    validation: Union[bool, str] = dag.validate()
    if validation == True:
        return dag
    else:
        logger.error(f"Invalid DAG definition: {validation}")
        return None

