from botocore.errorfactory import ClientError
from typing import Optional, List, Union, Tuple, Any
import s3fs
from pandas import DataFrame
from returns.result import Result, Success, Failure
from s3fs import S3FileSystem

from mason.clients.aws_client import AWSClient
from mason.clients.response import Response
from mason.engines.metastore.models.table import InvalidTable, Table, TableNotFound, InvalidTables, TableList
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.storage.models.path import Path, construct
from mason.util.exception import message
from mason.engines.metastore.models.schemas import schemas
from mason.engines.metastore.models.schemas import check_schemas as CheckSchemas
from mason.engines.metastore.models.schemas.schema import Schema, InvalidSchema, EmptySchema
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
    
    def expand_path(self, path: Path, response: Response = Response(), sample_size: Optional[Any] = None) -> Tuple[List[Path], Response]:
        paths: List[Path] = []
        full_path = path.full_path()
        response.add_info(f"Fetching keys at {full_path}")
        keys = self.client().find(full_path)
        response.add_response({'keys': keys})

        if len(keys) > 0:
            paths = list(map(lambda k: Path(k, "s3"), keys))

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

        return paths, response

    def get_table(self, database_name: str, table_name: str, options: dict = {}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        path = construct([database_name, table_name], "s3")
        paths, response = self.expand_path(path, (response or Response()), options.get("sample_size"))
        response.add_debug(f"{len(paths)} sub paths sampled at {path.full_path()}")
        final: Union[Table, InvalidTables]
            
        #  TODO:  Add limits to sample size for local client
        if len(paths) > 0:
            try:
                valid, invalid_schemas = sequence(list(map(lambda path: schemas.from_path(path, self.client(), options), paths)), Schema, InvalidSchema)
                non_empty = [v for v in valid if not isinstance(v, EmptySchema)]
                validated, paths = CheckSchemas.find_conflicts(non_empty)
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
            final = InvalidTables([TableNotFound(f"No keys at {path.full_path()}")])
            
        return final, response
    
    def summarize_table(self, table: Table, path: Path, options = {}, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        df: Optional[DataFrame] = schemas.df_from_path(path, self.client(), options, response)
        if isinstance(df, DataFrame):
            # summary_query = """SELECT test_column_1 from df"""
            # summary = ps.sqldf(summary_query)
            # table.schema.columns()
            print("HERE")
            return InvalidTables([], "Could not initialize dataframe."), response
        else:
            return InvalidTables([], "Could not initialize dataframe."), response

       import great_expectations as ge
        
        
        # return TableSummary(), response
        # blank/nulls
        # duplicates
        # shape
        # outlier
        # sql-rule
        
        
    def get_name(self, name: Optional[str], path: str) -> str:
        if not name or name == "":
            return path.rstrip("/").split("/")[-1]
        else:
            return name
        
    def save_to(self, inpath: Path, outpath: Path, response: Response):
        try:
            self.client().upload(inpath.path_str, outpath.path_str)
        except Exception as e:
            response.add_error(f"Error saving {inpath} to {outpath.path_str}")
            response.add_error(message(e))
            
        return response
            
            
