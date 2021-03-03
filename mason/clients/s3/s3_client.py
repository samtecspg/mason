from botocore.errorfactory import ClientError
from typing import Optional, List, Union, Tuple, Any
import s3fs
from pandas_profiling import ProfileReport
from returns.result import Result, Success, Failure
from s3fs import S3FileSystem

from mason.clients.aws_client import AWSClient
from mason.clients.engines.execution import ExecutionClient
from mason.clients.local.execution import LocalExecutionClient
from mason.clients.local.local_client import LocalClient
from mason.clients.response import Response
from mason.engines.metastore.models.deferred.table.summary import DeferredTableSummary
from mason.engines.metastore.models.deferred.table.table import DeferredTable
from mason.engines.metastore.models.table import InvalidTable, Table, TableNotFound, InvalidTables, TableList
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.storage.models.path import Path, construct
from mason.util.exception import message
from mason.util.logger import logger
from mason.engines.metastore.models.schemas import schemas
from mason.engines.metastore.models.schemas import check_schemas as CheckSchemas
from mason.engines.metastore.models.schemas.schema import Schema, InvalidSchema, EmptySchema, SchemaConflict
from mason.util.list import get, sequence

class S3Client(AWSClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def client(self) -> S3FileSystem:
        s3 = s3fs.S3FileSystem(key=self.access_key, secret=self.secret_key, client_kwargs={'region_name': self.aws_region})

        return s3

    def parse_responses(self, s3_response: dict):
        error = s3_response.get('Error', {}).get('Code', '')
        status = s3_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        message = s3_response.get('Error', {}).get('Message')
        return error, status, message


    def parse_table_list_data(self, s3_response: dict):
        single_items = list(map(lambda x: self.parse_item(x), s3_response.get('Contents', [])))
        prefixes = list(map(lambda x: self.parse_prefixes(x), s3_response.get('CommonPrefixes', [])))
        return {
            "items": single_items,
            "prefixes": prefixes
        }

    def parse_prefixes(self, s3_response: dict):
        return s3_response.get("Prefix")

    def parse_item(self, s3_response: dict):
        table_parsed = {
            "name": s3_response.get("Key"),
            "updated_at": s3_response.get("LastModified"),
            "size": s3_response.get("Size")
        }
        return table_parsed

    def parse_items(self, s3_response: dict):
        return list(map(lambda x: self.parse_item(x), s3_response.get('Contents', [])))

    def list_objects(self, database_name: str, response: Response) -> Tuple[Result[dict, str], Response]:
        try:
            split = database_name.split("/", 1)
            result = self.client().s3.list_objects(Bucket=split[0], Prefix=(get(split, 1) or '/'), Delimiter='/')
            response.add_response(result)
            return Success(result), response
        except Exception as e:
            if isinstance(e, ClientError):
                result = e.response
                error = result.get("Error", {})
                code = error.get("Code", "")
                if code == "NoSuchBucket":
                    response.set_status(404)
                    return Failure(f"The specified bucket does not exist: {database_name}"), response
            return Failure(message(e)), response

    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]:
        tables, response = self.list_objects(database_name, response)
        result: Result[dict, InvalidTables] = tables.alt(lambda e: InvalidTables([], e))
        
        def parse_response(result: dict, response: Response) -> Result[TableList, InvalidTables]:
            contents: Optional[List[dict]] = result.get("Contents")
            prefixes: Optional[List[dict]] = result.get("CommonPrefixes")
            
            if contents:
                tables: List[Union[Table, InvalidTables]] = []
                for c in contents:
                    key: Optional[str] = c.get("Key")
                    if key:
                        table, response = self.get_table(database_name.split("/")[0], key, response=response)
                        tables.append(table)
                valid, invalid = sequence(tables, Table, InvalidTables) 
                if len(valid) > 0:
                    return Success(TableList(valid))
                else:
                    invalid_tables: List[InvalidTable] = []
                    for i in invalid:
                        invalid_tables += (i.invalid_tables)
                        
                    return Failure(InvalidTables(invalid_tables, f"No valid tables at {database_name}"))
            elif prefixes:
                for p in prefixes:
                    response.add_data(p)
                return Failure(InvalidTables([], f"No valid tables at {database_name}.  Try appending '/' or specify deeper key."))
            else:
                return Failure(InvalidTables([], "No Data returned from AWS"))

        # TODO:  response is not pure here
        final = result.bind(lambda r: parse_response(r, response))

        return final, response
    
    def expand_path(self, path: Path, execution: Optional[ExecutionClient], response: Response = Response()) -> Tuple[List[Path], Response]:
        paths: List[Path] = []
        if execution and isinstance(execution, LocalExecutionClient):
            full_path = path.full_path()
            response.add_info(f"Fetching keys at {full_path}")
            keys = self.client.client().find(full_path)
            response.add_response({'keys': keys})

            if len(keys) > 0:
                paths = list(map(lambda k: Path(k, "s3"), keys))
        else:
            #  Spark, Dask  -> just return the path since they expand themselves? 
            paths = []
            response.add_error("Only Local execution client is supported for expand_path currently")

        return paths, response

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        path = construct([database_name, table_name], "s3")
        execution = options.get("execution")
        paths, response = self.expand_path(path, execution, (response or Response()))
        response.add_debug(f"{len(paths)} sub paths at {path}")
        
        if isinstance(execution, LocalExecutionClient):
            #  TODO: Check limits on sample_size for local client
            sample_size = options.get("sample_size")
            if sample_size:
                import random
                try: 
                    ss = int(sample_size)
                except TypeError:
                    response.add_warning(f"Invalid sample size (int): {sample_size}")
                    ss = 3

                response.add_warning(f"Sampling keys to determine schema. Sample size: {ss}.")
                if ss < len(paths):
                    paths = random.sample(paths, ss)
                
            #  TODO:  Add limits to sample size for local client
            if len(paths) > 0:
                try:
                    valid, invalid_schemas = sequence(list(map(lambda path: schemas.from_file(self.client().open(path.full_path()), options), paths)), Schema, InvalidSchema)
                    non_empty = [v for v in valid if not isinstance(v, EmptySchema)]
                    validated, paths =  CheckSchemas.find_conflicts(non_empty)
                    table = CheckSchemas.get_table(table_name, validated, paths)
                    invalid_tables = list(map(lambda i: InvalidTable("Invalid Schema", invalid_schema=i), invalid_schemas))
                    if isinstance(table, Table):
                        final = table
                    else:
                        invalid_tables.append(table)
                        final = InvalidTables(invalid_tables)
                except (ClientError, PermissionError) as e:
                    final = InvalidTables([InvalidTable(f"Not able to infer table: {message(e)}")])
            else:
                response.set_status(404)
                final = InvalidTables([TableNotFound(f"No keys at {path}")])
        else:
            final = InvalidTables([], "Only local execution client supported currently for s3 metastore")
        return final, response
    
    def populate_table(self):
            

    # def summarize_local_table(self, path: str, name: str, options: Optional[dict] = None, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
    #     table, response = self.infer_table(path, name, options, response)
    #     if isinstance(table, Table):
    #         df = table.as_df()
    #         import pdb; pdb.set_trace()
    #         report: ProfileReport = ProfileReport(df, True, title="Profiling Report", lazy=False)
    #         return TableSummary(), response
    #     else:
    #         return table, response
        
    def summarize_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        pass
        # execution: Optional[Any] = None
        # path = database_name + "/" + table_name
        # path_obj = self.get_path(path)
        # final: Union[TableSummary, InvalidTables]
        # 
        # if options:
        #     execution = options.get("execution")
        # table_name = self.get_name(table_name, path)
        # 
        # if execution and isinstance(execution, LocalExecutionClient):
        #     final, response = self.summarize_local_table(path, table_name, options, response)
        # elif execution: 
        #     final, response = DeferredTableSummary(table_name, path_obj, self.credentials()).run(execution, response)
        # else:
        #     final = InvalidTables([], "S3Client is not a full metastore.  Requires an execution client to infer tables.")
        # 
        # return final, response
        
    def get_name(self, name: Optional[str], path: str) -> str:
        if not name or name == "":
            return path.rstrip("/").split("/")[-1]
        else:
            return name

    def infer_table(self, path_str: str, name: Optional[str], options: Optional[dict] = None, resp: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        # execution: Optional[Any] = None
        # path_obj = self.get_path(path)
        # response: Response = resp or Response()
        # 
        # if options:
        #     execution = options.get("execution")
        # table_name = self.get_name(name, path)
        # 
        # if execution and isinstance(execution, LocalExecutionClient):
        #     # TODO:  Pull and respect number of threads from local execution
        #     opt = options or {}
        #     logger.info(f"Fetching keys at {path}")
        #     
        #     path = path_obj.full_path()
        #     keys, response = self.list_keys(path, response)
        # 
        #     response.add_debug(f"{len(keys)} keys at {path}")
        # 
        #     final: Union[Table, InvalidTables]
        #     
        #     sample_size = opt.get("sample_size")
        #     if sample_size:
        #         import random
        #         try: 
        #             ss = int(sample_size)
        #         except TypeError:
        #             response.add_warning(f"Invalid sample size (int): {sample_size}")
        #             ss = 3
        #             
        #         response.add_warning(f"Sampling keys to determine schema. Sample size: {ss}.")
        #         if ss < len(keys):
        #             keys = random.sample(keys, ss)
        # 
        #     if len(keys) > 0:
        #         try:
        #             valid, invalid_schemas = sequence(list(map(lambda key: schemas.from_file(self.client().open(key.full_path()), opt), keys)), Schema, InvalidSchema)
        #             non_empty = [v for v in valid if not isinstance(v, EmptySchema)]
        #             validated, paths = CheckSchemas.find_conflicts(non_empty)
        #             table = CheckSchemas.get_table(table_name, validated, paths)
        #             invalid_tables = list(map(lambda i: InvalidTable("Invalid Schema", invalid_schema=i), invalid_schemas))
        #             if isinstance(table, Table):
        #                 final = table
        #             else:
        #                 invalid_tables.append(table)
        #                 final = InvalidTables(invalid_tables)
        #         except (ClientError, PermissionError) as e:
        #             final = InvalidTables([InvalidTable(f"Not able to infer table: {message(e)}")])
        #     else:
        #         response.set_status(404)
        #         final = InvalidTables([TableNotFound(f"No keys at {path}")])
        # elif execution:
        #     final, resp = DeferredTable(table_name, path_obj, self.credentials()).run(execution, response)
        # else:
        #     final = InvalidTables([], "S3Client is not a full metastore.  Requires an execution client to infer tables.")
        #     
        # return final, resp or Response()
    
    # def list_keys(self, path: str,  response: Optional[Response] = None) -> Tuple[List[Path], Response]:
    #     resp: Response = response or Response()
    #     keys = self.client().find(path)
    #     resp.add_response({'keys': keys})
    #     if len(keys) > 0:
    #         paths = list(map(lambda k: self.get_path(k), keys))
    #     else:
    #         paths = []
    #     
    #     return paths, resp

    # def path(self, path: str):
    #     return Path(path.replace("s3://", ""), "s3")

    def save_to(self, inpath: Path, outpath: Path, response: Response):
            try:
                self.client().upload(inpath.path_str, outpath.path_str)
            except Exception as e:
                response.add_error(f"Error saving {inpath} to {outpath.path_str}")
                response.add_error(message(e))
                
            return response
            
            
