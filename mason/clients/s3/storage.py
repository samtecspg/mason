from typing import Tuple, List, Optional, Union

from fsspec.spec import AbstractBufferedFile

from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.clients.s3.s3_client import S3Client
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.table import Table
from mason.engines.metastore.models.table.tables import infer
from mason.engines.storage.models.path import Path, InvalidPath, parse_path

class S3StorageClient(StorageClient):

    def __init__(self, client: S3Client):
        self.client: S3Client = client 
    
    def open(self, path: Path) -> AbstractBufferedFile:
        return self.client.client().open(path.full_path())
        
    def get_path(self, path: str) -> Union[Path, InvalidPath]:
        return parse_path(path, "s3")

    def save_to(self, inpath: Path, outpath: Path, response: Response) -> Response:
        return self.client.save_to(inpath, outpath, response)
    
    def expand_path(self, path: Path, response: Response = Response(), sample_size: int = 3) -> Tuple[List[Path], Response]:
        return self.client.expand_path(path, response, sample_size)

    def infer_table(self, path: Path, table_name: Optional[str], options: dict={}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        return infer(path, self, table_name, options, response)
