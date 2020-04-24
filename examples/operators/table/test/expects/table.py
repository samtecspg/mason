import datetime
from dateutil.tz import tzlocal


def index(client_type: str, exists=True):
    # TODO: Unify s3 and glue responses here
    if client_type == "glue":
        if exists:
            tables = {'Tables': [
                {'Name': 'catalog_poc_data', 'CreatedAt': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),
                 'CreatedBy': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler',
                 'DatabaseName': 'crawler-poc', 'Schema': {'SchemaType': 'glue',
                                                           'Columns': [{'Name': 'index', 'Type': 'bigint'},
                                                                       {'Name': 'ts', 'Type': 'string'}]}}]}
            return ({'Data':[tables],'Errors':[],'Info':[],'Warnings':[]},200)
        else:
            return ({'Errors': ['Database bad-database not found'], 'Info': [], 'Warnings': []}, 404)
    elif client_type == "s3":
        if exists:
            return ({'Data': [{'Prefixes': [{'Prefix': '/user-data/'}]}],
              'Errors': [],
              'Info': [],
              'Warnings': []},
             200)
        else:
            return ({'Errors': ['The specified bucket does not exist: bad-database'], 'Info': [],  'Warnings': []}, 404)

def get(client_type: str, switch: int):
    if client_type == "glue":
        if switch == 1:
            return ({'Data':[{'CreatedAt':datetime.datetime(2020,2,26,12,57,31,tzinfo=tzlocal()),'CreatedBy':'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler','DatabaseName':'crawler-poc','Name':'catalog_poc_data','Schema':{'Columns':[{'Name':'index','Type':'bigint'},{'Name':'break1maxqty','Type':'double'},{'Name':'break1minqty','Type':'double'},{'Name':'break1price','Type':'double'},{'Name':'break2maxqty','Type':'double'},{'Name':'break2minqty','Type':'double'},{'Name':'break2price','Type':'double'},{'Name':'break3maxqty','Type':'double'},{'Name':'break3minqty','Type':'double'},{'Name':'break3price','Type':'double'},{'Name':'break4maxqty','Type':'double'},{'Name':'break4minqty','Type':'double'},{'Name':'break4price','Type':'double'},{'Name':'break5maxqty','Type':'double'},{'Name':'break5minqty','Type':'double'},{'Name':'break5price','Type':'double'},{'Name':'maxprice','Type':'double'},{'Name':'minprice','Type':'double'},{'Name':'packaging','Type':'string'},{'Name':'partnumber','Type':'string'},{'Name':'qtyperpackage','Type':'double'},{'Name':'source','Type':'string'},{'Name':'stockqty','Type':'double'},{'Name':'timestamp','Type':'string'},{'Name':'virtualstockqty','Type':'double'},{'Name':'break6maxqty','Type':'double'},{'Name':'break6minqty','Type':'double'},{'Name':'break6price','Type':'double'},{'Name':'break7maxqty','Type':'double'},{'Name':'break7minqty','Type':'double'},{'Name':'break7price','Type':'double'},{'Name':'ts','Type':'string'}],'SchemaType':'glue'}}],'Errors':[],'Info':[],'Warnings':[]},200)
        elif switch == 2:
            return ({'Warnings': [], 'Errors': ['Database bad-database or table catalog_poc_data not found'], 'Info': []}, 404)
        elif switch == 3:
            return ({'Warnings': [], 'Errors': ['Database crawler-poc or table bad-table not found'], 'Info': []}, 404)
        else:
            return {}
    elif client_type == "s3":
        if switch == 1:
            return ({'Data': [{'Schema': {'Columns': [{'ConvertedType': 'REQUIRED',  'Name': 'test_column_1', 'RepititionType': None, 'Type': 'INT32'}, {'ConvertedType': 'UTF8', 'Name': 'test_column_2','RepititionType': 'OPTIONAL','Type': 'BYTE_ARRAY'}],'SchemaType': 'parquet'}}],'Errors': [], 'Info': [], 'Warnings': []}, 200)
        elif switch == 2:
            #  TODO: fix this case to be more verbose
            return ({'Data': [{'Schema': []}], 'Errors': [], 'Info': [], 'Warnings': []}, 200)
        elif switch == 3:
            return ({'Data': [{'Schema': []}], 'Errors': [], 'Info': [], 'Warnings': []}, 200)

def post(exists = True):
    if exists:
        return ({'Errors': [], 'Info': ['Refreshing Table Crawler: test_crawler'], 'Warnings': ['Table crawler test_crawler already exists. Skipping '  'creation.']},201)
    else:
        return ({'Warnings': [], 'Errors': [],'Info': ['Created table crawler test_crawler_new.', 'Refreshing Table Crawler: test_crawler_new']}, 201)

def refresh(refreshing = True):
    data = [{'CreatedAt': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),                       'CreatedBy': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler',                       'DatabaseName': 'crawler-poc', 'Name': 'catalog_poc_data', 'Schema': {            'Columns': [{'Name': 'index', 'Type': 'bigint'}, {'Name': 'break1maxqty', 'Type': 'double'},                        {'Name': 'break1minqty', 'Type': 'double'}, {'Name': 'break1price', 'Type': 'double'},                        {'Name': 'break2maxqty', 'Type': 'double'}, {'Name': 'break2minqty', 'Type': 'double'},                        {'Name': 'break2price', 'Type': 'double'}, {'Name': 'break3maxqty', 'Type': 'double'},                        {'Name': 'break3minqty', 'Type': 'double'}, {'Name': 'break3price', 'Type': 'double'},                        {'Name': 'break4maxqty', 'Type': 'double'}, {'Name': 'break4minqty', 'Type': 'double'},                        {'Name': 'break4price', 'Type': 'double'}, {'Name': 'break5maxqty', 'Type': 'double'},                        {'Name': 'break5minqty', 'Type': 'double'}, {'Name': 'break5price', 'Type': 'double'},                        {'Name': 'maxprice', 'Type': 'double'}, {'Name': 'minprice', 'Type': 'double'},                        {'Name': 'packaging', 'Type': 'string'}, {'Name': 'partnumber', 'Type': 'string'},                        {'Name': 'qtyperpackage', 'Type': 'double'}, {'Name': 'source', 'Type': 'string'},                        {'Name': 'stockqty', 'Type': 'double'}, {'Name': 'timestamp', 'Type': 'string'},                        {'Name': 'virtualstockqty', 'Type': 'double'}, {'Name': 'break6maxqty', 'Type': 'double'},                        {'Name': 'break6minqty', 'Type': 'double'}, {'Name': 'break6price', 'Type': 'double'},                        {'Name': 'break7maxqty', 'Type': 'double'}, {'Name': 'break7minqty', 'Type': 'double'},                        {'Name': 'break7price', 'Type': 'double'}, {'Name': 'ts', 'Type': 'string'}],            'SchemaType': 'glue'}}]
    if refreshing:
        return ({'Data': data, 'Warnings': ['Table crawler test_crawler_refreshing is already refreshing.'], 'Errors': [], 'Info': []}, 202)
    else:
        return ({'Data': data, 'Errors': [],
        'Info': ['Refreshing Table Crawler: test_crawler'],
        'Warnings': []},
        201)


