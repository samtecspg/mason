

class AthenaMock:

    def start_query_execution(*args, **kwargs):
        if kwargs["QueryExecutionContext"]["Database"] == "access_denied":
            return {'Error': {'Message': 'User: arn:aws:iam::062325279035:user/kops is not authorized to perform: athena:CreateNamedQuery on resource: arn:aws:athena:us-east-1:062325279035:workgroup/mason', 'Code': 'AccessDeniedException'}, 'ResponseMetadata': {'RequestId': '5fb1a7af-8478-46fe-b3d1-1efce725d879', 'HTTPStatusCode': 400, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Fri, 24 Apr 2020 13:31:08 GMT', 'x-amzn-requestid': '5fb1a7af-8478-46fe-b3d1-1efce725d879', 'content-length': '209', 'connection': 'keep-alive'}, 'RetryAttempts': 0}}
        elif (kwargs["QueryExecutionContext"]["Database"] == "good_database") and (kwargs["QueryString"] == "SELECT * from good_table limit 5") and kwargs["WorkGroup"] == "mason":
            return {"QueryExecutionId": "test", 'NamedQueryId': '1c7c2bb9-8393-436a-b131-1f83392ff565', 'ResponseMetadata': {'RequestId': '4bad720f-dcf6-4999-a8ef-1a73f0942756', 'HTTPStatusCode': 200,  'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Fri, 24 Apr 2020 13:50:17 GMT', 'x-amzn-requestid': '4bad720f-dcf6-4999-a8ef-1a73f0942756', 'content-length': '55', 'connection': 'keep-alive'},'RetryAttempts': 0}}
        else:
            raise Exception(f"Unmocked athena api call made")


    def get_query_results(*args, **kwargs):
        if kwargs["QueryExecutionId"] == "good_job_id":
            return {
                'UpdateCount': 0,
                'ResultSet':
                    {'Rows': [{'Data': [{'VarCharValue': 'index'}]}],
                     'ResultSetMetadata':
                         {'ColumnInfo': [ {'CatalogName': 'hive', 'SchemaName': '', 'TableName': '', 'Name': 'index', 'Label': 'index', 'Type': 'bigint', 'Precision': 19, 'Scale': 0, 'Nullable': 'UNKNOWN', 'CaseSensitive': False}]}
                     },
                'ResponseMetadata': {'RequestId': 'test-id', 'HTTPStatusCode': 200}
            }
        elif kwargs["QueryExecutionId"] == "bad_job_id":
            return {'Error': {'Code': 'InvalidRequestException',
                        'Message': 'QueryExecution bad_job_id was not found'},
                        'ResponseMetadata': {'HTTPHeaders': {'connection': 'keep-alive'}, 'HTTPStatusCode': 400, 'RequestId': 'test-id',
                                   'RetryAttempts': 0}}
        else:
            raise Exception(f"Unmocked athena api call made")


