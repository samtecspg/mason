from typing import Union

from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep

from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.executed_dag_step import ExecutedDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep


def step(current: ExecutedDagStep, next: ValidDagStep) -> Union[ValidDagStep, InvalidDagStep, FailedDagStep]:
    current_step_id = current.step.id
    object = current.operator_response.object
    
    if current_step_id == "step_1":
        if isinstance(object, ExecutedJob):
            next.operator.parameters.set_required("job_id", object.id)
            return next
        else:
            return InvalidDagStep("ExecutedJob not returned from step_1", current.operator_response)
    elif current_step_id == "step_2":
        if isinstance(object, ExecutedJob):
            next.operator.parameters.set_required("job_id", object.id)
            return next
        elif isinstance(object, InvalidJob):
            return FailedDagStep(f"Executed job not returned from step_2: {object.reason}", current.step, current.operator_response) 
        else:
            return InvalidDagStep(f"Invalid object returned from step_2: {object}")
    elif current_step_id == "step_3":
        if isinstance(object, ExecutedJob):
            next.operator.parameters.set_required("job_id", object.id)
            return InvalidDagStep("Query Succesful, not cleaning up table", current.operator_response)
        elif isinstance(object, InvalidJob):
            return next
        else:
            return InvalidDagStep(f"Invalid object returned from step_3: {object}")
    else:
        raise NotImplementedError(f"Step transition not implemented {current_step_id}->{next.id}")
