from typing import Tuple, Optional, List, Union

from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.util.logger import logger
from mason.util.list import sequence, flatten, flatten_array

from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.executed_dag_step import ExecutedDagStep
from mason.util.environment import MasonEnvironment
from mason.engines.storage.models.path import Path
from mason.clients.response import Response
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.local.local_client import LocalClient

class LocalSchedulerClient(SchedulerClient):
    
    #  This is a local synchronous scheduler.   For asynchronous see AsyncLocal (WIP)

    def __init__(self, config: dict):
        self.client: LocalClient = LocalClient(config)
        self.dag: Optional[ValidDag] = None
    
    def register_dag(self, schedule_name: str, valid_dag: ValidDag, response: Response) -> Tuple[str, Response]:
        response.add_info("Registering DAG in local memory")
        self.dag = valid_dag
        return (schedule_name, response)

    def register_schedule(self, database_name: str, path: Path, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        dag = self.dag
        if dag:
            response.add_info(f"Running dag \n{self.dag.display()}")
            roots: List[ValidDagStep] = self.dag.roots()
            
            # TODO: Thread this with number of threads equal to max available len(list)

            message = ", ".join(list(map(lambda n: n.id, roots)))
            logger.debug(f"Running steps: {message}")

            exec_steps = list(map(lambda r: ExecutedDagStep(r, r.run(env, response)), roots))
            next: List[Union[ValidDagStep, InvalidDagStep]] = flatten_array(list(map(lambda e: e.next_steps(dag, env), exec_steps)))
            nxt, invalid = sequence(next, ValidDagStep, InvalidDagStep)
            
            nxt = list(set(nxt))
            message = ", ".join(list(map(lambda n: n.id, nxt)))
            logger.debug(f"Running steps: {message}")

            for e in exec_steps:
                response = e.operator_response.to_response(response)

            for i in invalid:
                response.add_error(i.reason)

            while len(nxt) > 0:
                exec_steps = list(map(lambda r: ExecutedDagStep(r, r.run(env, response)), nxt))
                next: List[Union[ValidDagStep, InvalidDagStep]] = flatten_array(list(map(lambda e: e.next_steps(dag, env), exec_steps)))
                nxt, invalid = sequence(next, ValidDagStep, InvalidDagStep)
                
                if len(nxt) > 0:
                    nxt = list(set(nxt))
                    message = ", ".join(list(map(lambda n: n.id, nxt)))
                    logger.debug(f"Running steps: {message}")

                    for e in exec_steps:
                        response = e.operator_response.to_response(response)

                    for i in invalid:
                        response.add_error(i.reason)

        return response
            

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

