from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs.summary_job import SummaryJob
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableSummarize(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, resp: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        
        metastore = config.metastore()
        storage = config.storage()
        
        if isinstance(metastore, MetastoreClient) and isinstance(storage, StorageClient):
            job = SummaryJob(database_name, table_name, metastore, storage, read_headers)
            run, response = config.execution().run_job(job)
            oR = OperatorResponse(response, run)
        else:
            # TODO
            response = Response().add_error("BAD")
            oR = OperatorResponse(response)
        return oR 