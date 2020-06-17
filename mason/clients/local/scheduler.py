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
    
    #  returns next_steps, invalid_steps, pending_steps, all_finished_steps
    def progress_steps(self, dag: ValidDag, env: MasonEnvironment, response: Response, steps: List[ValidDagStep], pending_steps: List[ExecutedDagStep], executed_steps: List[ExecutedDagStep]) -> Tuple[List[ValidDagStep], List[InvalidDagStep], List[ExecutedDagStep], List[ExecutedDagStep]]:
        # TODO: Thread this with number of threads equal to max available len(list)
        message = ", ".join(list(map(lambda n: n.id, steps)))
        logger.debug(f"Running steps: {message}")
        
        st = sorted(steps)
        new_executed_steps = list(map(lambda r: ExecutedDagStep(r, r.run(env, response)), st)) + pending_steps
        executed_steps += new_executed_steps
        next_steps: List[Union[ValidDagStep, InvalidDagStep, ExecutedDagStep]] = flatten_array(list(map(lambda e: e.next_steps(dag, env, executed_steps), new_executed_steps)))
        valid: List[ValidDagStep] = []
        invalid: List[InvalidDagStep] = []
        pending: List[ExecutedDagStep] = []
        for s in next_steps:
            if isinstance(s, ValidDagStep):
                valid.append(s)
            elif isinstance(s, InvalidDagStep):
                invalid.append(s)
            else:
                pending.append(s)

        for e in new_executed_steps:
            response = e.operator_response.to_response(response)

        for i in invalid:
            response.add_error(i.reason)
            

        return list(set(valid)), list(set(invalid)), list(set(pending)), list(set(executed_steps))

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        dag = self.dag
        if dag:
            response.add_info(f"Running dag \n{dag.display()}")
            roots: List[ValidDagStep] = dag.roots()
            
            valid, invalid, pending, executed = self.progress_steps(dag, env, response, roots, [], [])
            
            while len(valid) > 0:
                valid, invalid,pending, executed = self.progress_steps(dag, env, response, valid, pending, executed)

        return response
            

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

