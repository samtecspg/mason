
import boto3 # type: ignore

class S3Client:
    def __init__(self, s3_config: dict):
        self.region = s3_config.get("region")
        self.client = boto3.client('s3', region_name=self.region)

    def path(self, path: str):
        return "s3://" + path

    # TODO: Validate that the specified path exists before bothering other clients with it
    # def validate_path(self, path: str):
    #     try:
    #         s3 = boto3.resource('s3')
    #         object = s3.Object('bucket_name', 'key')
    #     except ClientError as e:
    #         result = e.response
