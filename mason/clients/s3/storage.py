from typing import Tuple, List

from mason.clients.engines.execution import ExecutionClient
from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.clients.s3.s3_client import S3Client
from mason.engines.storage.models.path import Path, construct

class S3StorageClient(StorageClient):

    def __init__(self, client: S3Client):
        self.client: S3Client = client 

    def path(self, path: str) -> Path:
        return construct([path], "s3") 
        
    def table_path(self, database_name: str, table_name: str) -> Path:
        return construct([database_name, table_name], "s3")

    # def infer_table(self, path: str,  name: Optional[str] = None, options: Optional[dict] = None, response: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
    #     return self.client.get_table((self.path(path) or Path("", "s3")).path_str, name, options, response)

    def save_to(self, inpath: str, outpath: str, response: Response) -> Response:
        inp: Path = Path(inpath) # TODO:  allow saving between paths of different storage clients
        outp: Path = self.path(outpath)
        # TODO:
        return self.client.save_to(inp, outp, response)
    
    def expand_path(self, path: Path, execution: ExecutionClient, response: Response = Response()) -> Tuple[List[Path], Response]:
        return self.client.expand_path(path, execution, response)
