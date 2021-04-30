from typing import Tuple, List, Optional, Union

from fsspec.spec import AbstractBufferedFile

from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.clients.s3.s3_client import S3Client
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.table import Table
from mason.engines.metastore.models.table.tables import infer
from mason.engines.storage.models.path import Path, construct

class S3StorageClient(StorageClient):

    def __init__(self, client: S3Client):
        self.client: S3Client = client 

    def path(self, path: str) -> Path:
        return construct([path], "s3") 
    
    def open(self, path: Path) -> AbstractBufferedFile:
        return self.client.client().open(path.full_path())
        
    def table_path(self, database_name: str, table_name: str) -> Path:
        return self.client.table_path(database_name, table_name)

    def save_to(self, inpath: str, outpath: str, response: Response) -> Response:
        inp: Path = Path(inpath) # TODO:  allow saving between paths of different storage clients
        outp: Path = self.path(outpath)
        # TODO:
        return self.client.save_to(inp, outp, response)
    
    def expand_path(self, path: Path, response: Response = Response(), sample_size: int = 3) -> Tuple[List[Path], Response]:
        return self.client.expand_path(path, response, sample_size)

    def infer_table(self, path: Path, table_name: Optional[str], options: dict={}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        return infer(path, self, table_name, options, response)
