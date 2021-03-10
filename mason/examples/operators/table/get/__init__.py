from mason_dask.jobs.executed import ExecutedJob

from mason.clients.engines.execution import ExecutionClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs.infer_job import InferJob
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableGet(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, resp: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        
        metastore = config.metastore()
        storage = config.storage()
        execution = config.execution()
        
        if isinstance(metastore, MetastoreClient) and isinstance(storage, StorageClient) and isinstance(execution, ExecutionClient):
            job = InferJob(database_name, table_name, metastore, storage, read_headers)
            
            if execution.is_async():
                InferJob(database_name, table_name)
                execution.run_job()
                table, reseponse = self.metastore.get_table(self.database_name, self.table_name, {"read_headers": self.read_headers}, response)

            run, response = config.execution().run_job(job)
            oR = OperatorResponse(response, run)
        else:
            response = Response().add_error("BAD")
            oR = OperatorResponse(response)
        return oR 

