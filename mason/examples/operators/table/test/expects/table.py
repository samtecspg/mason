import datetime
from dateutil.tz import tzlocal

from mason.definitions import from_root


def index(client_type: str, exists=True):
    # TODO: Unify s3 and glue responses here
    if client_type == "glue":
        if exists:
            tables = {'Tables': [
                {'Name': 'test-table', 'CreatedAt': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),
                 'CreatedBy': 'crawler:test_crawler',
                  'Schema': {'SchemaType': 'glue',
                                                           'Columns': [{'Name': 'index', 'Type': 'bigint'},
                                                                       {'Name': 'ts', 'Type': 'string'}]}}]}
            return ({'Data':[tables]},200)
        else:
            return ({'Errors': ['Database bad-database not found']}, 404)
    elif client_type == "s3":
        if exists:
            return ({'Data': [{'Prefix': '/test_path/'}], 'Errors': ["No valid tables at test-database.  Try appending '/' or specify deeper key."]},
             200)
        else:
            return ({'Errors': ['The specified bucket does not exist: bad-database']}, 404)

def get(client_type: str, switch: int):
    if client_type == "glue":
        if switch == 1:
            return ({'Data':[{'CreatedAt':datetime.datetime(2020,2,26,12,57,31,tzinfo=tzlocal()),'CreatedBy':'crawler:test_crawler','Name':'test-table','Schema':{'Columns':[{'Name':'index','Type':'bigint'},{'Name':'break1maxqty','Type':'double'},{'Name':'break1minqty','Type':'double'},{'Name':'break1price','Type':'double'},{'Name':'break2maxqty','Type':'double'},{'Name':'break2minqty','Type':'double'},{'Name':'break2price','Type':'double'},{'Name':'break3maxqty','Type':'double'},{'Name':'break3minqty','Type':'double'},{'Name':'break3price','Type':'double'},{'Name':'break4maxqty','Type':'double'},{'Name':'break4minqty','Type':'double'},{'Name':'break4price','Type':'double'},{'Name':'break5maxqty','Type':'double'},{'Name':'break5minqty','Type':'double'},{'Name':'break5price','Type':'double'},{'Name':'maxprice','Type':'double'},{'Name':'minprice','Type':'double'},{'Name':'packaging','Type':'string'},{'Name':'partnumber','Type':'string'},{'Name':'qtyperpackage','Type':'double'},{'Name':'source','Type':'string'},{'Name':'stockqty','Type':'double'},{'Name':'timestamp','Type':'string'},{'Name':'virtualstockqty','Type':'double'},{'Name':'break6maxqty','Type':'double'},{'Name':'break6minqty','Type':'double'},{'Name':'break6price','Type':'double'},{'Name':'break7maxqty','Type':'double'},{'Name':'break7minqty','Type':'double'},{'Name':'break7price','Type':'double'},{'Name':'ts','Type':'string'}],'SchemaType':'glue'}}]},200)
        elif switch == 2:
            return ({'Errors': ['Database bad-database or table test-table not found']}, 404)
        elif switch == 3:
            return ({'Errors': ['Database test-database or table bad-table not found']}, 404)
        else:
            return {}
    elif client_type == "s3":
        if switch == 1:
            return ({'Data': [{'CreatedAt': '', 'CreatedBy': 'mason', 'Name': 'test-table', 'Schema':{'Columns': [{'Name': 'type', 'Type': 'object'}, {'Name': 'price', 'Type': 'float64'}], 'SchemaType': 'csv'}}], 'Info': ['Fetching keys at s3://test-database/test-table'],'Warnings': ['Sampling keys to determine schema. Sample size: 3.']}, 200)
        elif switch == 2:
            return ({'Errors': ['No keys at s3://bad-database/test-table'], 'Info': ['Fetching keys at s3://bad-database/test-table'], 'Warnings': ['Sampling keys to determine schema. Sample size: 3.']}, 404)
        elif switch == 3:
            return ({'Errors': ['No keys at s3://test-database/bad-table'],'Info': ['Fetching keys at s3://test-database/bad-table'], 'Warnings': ['Sampling keys to determine schema. Sample size: 3.']}, 404)
    elif client_type == "local":
        if switch == 1:
            return ({'Data': [{'CreatedAt': '', 'CreatedBy': 'mason', 'Name': 'csv_sample.csv', 'Schema': {'Columns': [{'Name': "type", 'Type': 'object'}, {'Name': "price", 'Type': 'float64'}],'SchemaType': 'csv'}}]},200)
        elif switch == 2:
            return ({'Errors': ['Invalid Schema:File not found: /test/bad/csv_sample.csv','Invalid Schema: No valid schemas found']},404)
        elif switch == 3:
            return ({'Errors': ['Invalid Schema:File not found: /test/sample_data/bad.csv','Invalid Schema: No valid schemas found']}, 404)

def post(exists = True):
    if exists:
        info = ['Registering workflow dag test_crawler with glue.',
         'Registered schedule test_crawler',
         'Triggering schedule: test_crawler',
         'Refreshing Table Crawler: test_crawler']
        return ({'Info': info, 'Warnings': ['Table crawler test_crawler already exists. Skipping '  'creation.']},201)
    else:
        info = ['Registering workflow dag test_crawler_new with glue.',
                'Created table crawler test_crawler_new.',
                'Registered schedule test_crawler_new',
                'Triggering schedule: test_crawler_new',
                'Refreshing Table Crawler: test_crawler_new']
        return ({'Info': info}, 201)

def refresh(refreshing = True):
    if refreshing:
        return ({'Warnings': ['Table crawler test_crawler_refreshing is already refreshing.']}, 202)
    else:
        return ({'Info': ['Refreshing Table Crawler: test_crawler']}, 201)


def parameters(config_id: str):
    if config_id == "1":
        return [
            f"database_name:{from_root('/test/sample_data/')},table_name:csv_sample.csv,read_headers:true",
            f"database_name:{from_root('/test/bad/')},table_name:csv_sample.csv,",
            f"database_name:{from_root('/test/sample_data/')},table_name:bad.csv"
        ]
    elif config_id == "3":
        return [
            "database_name:test-database,table_name:test-table,read_headers:true",
            "database_name:bad-database,table_name:test-table",
            "database_name:test-database,table_name:bad-table"
        ]
    else:
        return [
            "database_name:test-database,table_name:test-table,read_headers:true",
            "database_name:test-database,table_name:test-table,read_headers:true,output_path:test-database/test-summary",
            "database_name:test-database,table_name:bad-table"
        ]

        
    