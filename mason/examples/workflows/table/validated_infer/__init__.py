from typing import Union

from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep

from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.executed_dag_step import ExecutedDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep


def step(current: ExecutedDagStep, next: ValidDagStep) -> Union[ValidDagStep, InvalidDagStep, FailedDagStep]:
    if current.step.id == "step_1":
        executed_job = current.operator_response.object
        if isinstance(executed_job, ExecutedJob):
            next.operator.parameters.set_required("job_id", executed_job.id)
            return next
        else:
            return InvalidDagStep("ExecutedJob not returned from step_1", current.operator_response)
    elif current.step.id == "step_2":
        job = current.operator_response.object
        if isinstance(job, ExecutedJob):
            next.operator.parameters.set_required("job_id", job.id)
            return next
        elif isinstance(job, InvalidJob):
            return FailedDagStep(f"Executed job not returned from step_2: {job.reason}", current.step, current.operator_response) 
        else:
            return InvalidDagStep(f"Invalid object returned from step_2: {job}")
    elif current.step.id == "step_3":
        job = current.operator_response.object
        if isinstance(job, ExecutedJob):
            next.operator.parameters.set_required("job_id", job.id)
            return InvalidDagStep("Query Succesful, not cleaning up table", current.operator_response)
        elif isinstance(job, InvalidJob):
            return next
        else:
            return InvalidDagStep(f"Invalid object returned from step_3: {job}")
    else:
        raise NotImplementedError(f"Step transition not implemented {current.step.id}->{next.id}")
