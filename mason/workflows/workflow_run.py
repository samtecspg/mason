from importlib import import_module
from typing import List, Union

from mason.clients.response import Response
from mason.engines.scheduler.models.dags.executed_dag_step import ExecutedDagStep
from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.util.environment import MasonEnvironment
from mason.util.list import flatten_array
from mason.util.string import to_class_case
from mason.workflows.invalid_workflow import InvalidWorkflow
from mason.workflows.workflow_definition import WorkflowDefinition

class WorkflowRun:
    
    def __init__(self, dag: ValidDag):
        self.dag = dag
        self.next_steps: List[ValidDagStep] = sorted(dag.roots())
        self.pending_steps: List[ExecutedDagStep] = [] 
        self.executed_steps: List[ExecutedDagStep] = []
        self.invalid_steps: List[InvalidDagStep] = []
        
    def run(self, env: MasonEnvironment, response: Response) -> Response:
        response.add_info(f"Running dag \n{self.dag.display()}")
        
        while not self.finished():
            self.step(env)
            
        for step in sorted(self.executed_steps):
            response = response.merge(step.operator_response.response)

        if len(self.invalid_steps) > 0:
            response.add_error(f"Workflow failed")
            for i in self.invalid_steps:
                response.add_error(i.reason)
            response.set_status(400)

        return response
        
    def step(self, env: MasonEnvironment):
        executed_steps = list(map(lambda s: ExecutedDagStep(s, s.run(env)), self.next_steps))
        self.executed_steps += list(set(executed_steps))
        next_steps: List[Union[ValidDagStep, InvalidDagStep, ExecutedDagStep]] = flatten_array(list(map(lambda e: self.get_next_steps(e, env), executed_steps)))

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
        
        self.next_steps = list(set(valid))
        self.pending_steps = list(set(pending))
        self.invalid_steps += list(set(invalid))

    def finished(self) -> bool:
        return len(self.next_steps) == 0

    def get_next_steps(self, executed_step: ExecutedDagStep, env: MasonEnvironment) -> List[Union[ValidDagStep, InvalidDagStep, ExecutedDagStep]]:
        next_steps: List[ValidDagStep]
        next_steps = self.dag.get_next_steps(executed_step.step)

        if not executed_step.step.id in list(map(lambda e: e.id, next_steps)):
            ns = next_steps
        else:
            ns = []
            
        return list(map(lambda s: self.check_step(executed_step, s, env), ns))

    def check_step(self, current: ExecutedDagStep, next: ValidDagStep, env: MasonEnvironment) -> Union[ValidDagStep, InvalidDagStep, ExecutedDagStep]:
        all_steps: Union[ValidDagStep, InvalidDagStep, ExecutedDagStep]

        definition = self.module(env)
        if isinstance(definition, WorkflowDefinition):
            stepped = definition.step(current, next) 
            if isinstance(stepped, FailedDagStep):
                all_steps = stepped.retry()
            elif isinstance(stepped, InvalidDagStep):
                all_steps = stepped
            elif isinstance(stepped, ExecutedDagStep):
                all_steps = stepped
            else:
                # check that all dependencies are satisfied, otherwise return the step as pending
                dep = stepped.dependencies
                executed_step_ids = list(map(lambda e: e.step.id, self.executed_steps))
                unsatisfied_steps = set(dep).difference(set(executed_step_ids))
                if len(unsatisfied_steps) > 0:
                    return current
                else:
                    return stepped
        else:
            all_steps = InvalidDagStep(f"Invalid Workflow Definition. {definition.reason}")

        return all_steps

    def module(self, env: MasonEnvironment) -> Union[WorkflowDefinition, InvalidWorkflow]:
        try:
            mod = import_module(env.workflow_module + f".{self.dag.namespace}.{self.dag.command}")
            try:
                classname = to_class_case(f"{self.dag.namespace}_{self.dag.command}")
                workflow_class = getattr(mod, classname)()
                if isinstance(workflow_class, WorkflowDefinition):
                    return workflow_class
                else:
                    return InvalidWorkflow("Invalid Workflow definition.  See operators/workflow_definition.py")
            except AttributeError as e:
                return InvalidWorkflow(f"Workflow has no attribute {classname}")
        except ModuleNotFoundError as e:
            return InvalidWorkflow(f"Module not found: {env.operator_module}")

