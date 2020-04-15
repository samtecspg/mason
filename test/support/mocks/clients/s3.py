
from botocore.errorfactory import ClientError # type: ignore
from fsspec.implementations.local import LocalFileSystem # type: ignore

from definitions import from_root

class S3ClientMock:
    def list_objects(self, Bucket:str, Prefix: str, Delimiter: str):
        if Bucket == "crawler-poc":
            return {'CommonPrefixes':[{'Prefix':'/user-data/'}],'Delimiter':'/','EncodingType':'url','IsTruncated':False,'Marker':'','MaxKeys':1000,'Name':'lake-working-copy-feb-20-2020','Prefix':'/','ResponseMetadata':{'HTTPHeaders':{'content-type':'application/xml','date':'Tue,24''Mar''2020''04:49:53''GMT','server':'AmazonS3','transfer-encoding':'chunked','x-amz-bucket-region':'us-east-1','x-amz-id-2':'ILdl+HMfa03glzOhQnvuDBnUDsbc+JvMPmPqle6n6LaRrno0ZbnkC8yh8Ey4luBj4W5qW1h+Ox0=','x-amz-request-id':'C2D08B65B1491B10'},'HTTPStatusCode':200,'HostId':'ILdl+HMfa03glzOhQnvuDBnUDsbc+JvMPmPqle6n6LaRrno0ZbnkC8yh8Ey4luBj4W5qW1h+Ox0=','RequestId':'C2D08B65B1491B10','RetryAttempts':0}}
        elif Bucket == "bad-database":
            raise ClientError({'Error':{'BucketName':'bad-database','Code':'NoSuchBucket','Message':'The specified bucket does not exist'},'ResponseMetadata':{'HTTPHeaders':{'content-type':'application/xml','date':'Tue,24''Mar''2020''05:15:07''GMT','server':'AmazonS3','transfer-encoding':'chunked','x-amz-id-2':'TH8GrpyeWs7aEepmT19+L8Vxz7MS0OpFtTilbKDjOEW8UFkT8OibUpYUaB6GDbs0ni/EpoTDTbw=','x-amz-request-id':'C283C98B4D134592'},'HTTPStatusCode':404,'HostId':'TH8GrpyeWs7aEepmT19+L8Vxz7MS0OpFtTilbKDjOEW8UFkT8OibUpYUaB6GDbs0ni/EpoTDTbw=','RequestId':'C283C98B4D134592','RetryAttempts':0}}, "ListObjects")
        else:
            raise Exception(f"Unmocked S3 API endpoint: {Bucket}")


    def list_objects_v2(self, Bucket:str, Prefix: str):
        if Bucket == "crawler-poc" and Prefix == "catalog_poc_data":
            return {'ResponseMetadata': {'HTTPStatusCode': 200}}
        elif Bucket == "bad-database" and Prefix == "catalog_poc_data":
            raise ClientError({'Error':{'BucketName':'crawler-poc','Code':'NoSuchBucket','Message':'The specified bucket does not exist'},'ResponseMetadata':{'HTTPHeaders':{'content-type':'application/xml','date':'Tue,24''Mar''2020''06:02:42''GMT','server':'AmazonS3','transfer-encoding':'chunked','x-amz-id-2':'YtrUTIME88+RIC3yuv4dvnnuoRvaRpCZfTuqjqBbjmy1/hUqnT9JHBvk3rg2LJEmjUIMrX8N0B0=','x-amz-request-id':'6AA61AA2C52B1F5B'},'HTTPStatusCode':404,'HostId':'YtrUTIME88+RIC3yuv4dvnnuoRvaRpCZfTuqjqBbjmy1/hUqnT9JHBvk3rg2LJEmjUIMrX8N0B0=','RequestId':'6AA61AA2C52B1F5B','RetryAttempts':0}}, "ListObjects")
        elif Bucket == "crawler-poc" and Prefix == "bad-table":
            return {'ResponseMetadata': {'HTTPStatusCode': 200}}
        else:
            raise Exception(f"Unmocked S3 API endpoint: {Bucket} {Prefix}")


class S3Mock:

    def __init__(self):
        self.s3 = S3ClientMock()

    def open(self, key: str):
        if (key == "crawler-poc/catalog_poc_data/test1.csv" or key == "crawler-poc/catalog_poc_data/test2.csv"):
            fs = LocalFileSystem()
            return fs.open(from_root('/test/sample_data/sample.snappy.parquet'))
        else:
            raise Exception(f"Unmocked S3 API endpoint: {key}")


    def find(self, path: str):
        if path == "crawler-poc/catalog_poc_data":
            return ["crawler-poc/catalog_poc_data/test1.csv", "crawler-poc/catalog_poc_data/test2.csv"]
        elif path == "bad-database/catalog_poc_data":
            return []
        elif path == "crawler-poc/bad-table":
            return []
        else:
            raise Exception(f"Unmocked S3 API endpoint: {path}")



