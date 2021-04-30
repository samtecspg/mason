from typing import Set, Union

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs import InvalidJob
from mason.engines.execution.models.jobs.executed_job import ExecutedJob
from mason.engines.execution.models.jobs.merge_job import MergeJob
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.metastore.models.table.table import Table
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import DelayedOperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableMerge(OperatorDefinition):

    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> DelayedOperatorResponse:
        SUPPORTED_SCHEMAS = {
            "parquet",
            "csv",
            "json",
            "jsonl"
        }

        # TODO: Replace db_name, tb_name with protocal path, ie s3://bucket/path, athena://database:table
        database_name = parameters.get_required("database_name")
        table_name = parameters.get_required("table_name")
        output_path = parameters.get_required("output_path")
        read_headers = parameters.get_optional("read_headers")
        
        table, response = config.metastore().get_table(database_name, table_name, {"read_headers": read_headers}, response)
        
        final: Union[ExecutedJob, InvalidJob]

        if isinstance(table, Table):
            final = InvalidJob("No conflicting schemas found. Merge Unecessary")
        else:
            conflicting_table = table.conflicting_table()
            if conflicting_table:
                schemas = conflicting_table.schema_conflict.unique_schemas
                schema_types: Set[str] = set(map(lambda schema: schema.type, schemas))
                if len(schemas) > 0 and schema_types.issubset(SUPPORTED_SCHEMAS):
                    if len(schema_types) == 1:
                        schema_type = next(iter(schema_types))
                        inp = config.storage().table_path(database_name, table_name)
                        outp = config.storage().path(output_path)
                        credentials = config.metastore().credentials()
                        if isinstance(credentials, MetastoreCredentials):
                            job = MergeJob(schema_type, inp, outp, credentials)
                            final, response = config.execution().run_job(job, response)
                        else:
                            final = InvalidJob("MetastoreCredentials not found")
                    else:
                        final = InvalidJob("Mixed schemas not supported at this time.")
                else:
                    final = InvalidJob(f"Unsupported schemas for merge operator: {', '.join(list(schema_types.difference(SUPPORTED_SCHEMAS)))}")
            else:
                final = InvalidJob(f"No conflicting schemas found at {database_name},{table_name}. Merge unecessary. Invalid Schemas {table.message()}")

        return DelayedOperatorResponse(final, response)