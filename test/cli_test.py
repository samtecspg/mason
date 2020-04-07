
from click.testing import CliRunner
from cli import config, operator, register
from definitions import from_root
import os
import pytest #type: ignore
import shutil
import re

#  TODO: Figure out how to remove references to /Users/kyle/dev from these tests for any user, remove basename from output

def ansi_escape(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def assert_multiline(s1: str, s2: str):
    e = re.compile(r"(\s|\n|\t)")
    f1 = e.sub('', ansi_escape(s1))
    f2 = e.sub('', s2)
    assert (f1 == f2)

def print_result(result):
    print(result.output)
    print(result.exception)

# @pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        shutil.rmtree(".tmp/")

    def test_config_0(self):
        runner = CliRunner()
        result = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_0.yaml'), '-l', 'info'])
        print(result.output)
        expects = """
        Set log level to info
        +-------------------------------+
        | Creating MASON_HOME at .tmp/  |
        +-------------------------------+
        +-------------------------------------------------------+
        | Creating OPERATOR_HOME at .tmp/registered_operators/  |
        +-------------------------------------------------------+
        +-----------------------------------------------+
        | Creating CONFIG_HOME at .tmp/configurations/  |
        +-----------------------------------------------+
        +--------------------+
        | Config Validation  |
        +--------------------+

        Schema error /Users/kyle/dev/mason/configurations/schema.json: 'bad' is not one of ['glue', 's3']

        Invalid Config Schema: /Users/kyle/dev/mason/examples/operators/table/test_configs/config_0.yaml
        """
        assert_multiline(expects, result.output)

    def test_config_1(self):
      runner = CliRunner()
      result1 = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_1.yaml'), '-l', 'info'])
      expect1 = """
        Set log level to info
        +-------------------------------+
        | Creating MASON_HOME at .tmp/  |
        +-------------------------------+
        +-------------------------------------------------------+
        | Creating OPERATOR_HOME at .tmp/registered_operators/  |
        +-------------------------------------------------------+
        +-----------------------------------------------+
        | Creating CONFIG_HOME at .tmp/configurations/  |
        +-----------------------------------------------+
        +----------------------+
        | Configuration Valid  |
        +----------------------+
        Configuration: {'metastore': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'scheduler': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': '', 'configuration': {}}}

        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/operators/table/test_configs/config_1.yaml to .tmp/configurations/
      """
      print(result1.output)
      assert_multiline(expect1, result1.output)

    def test_config_2(self):
        runner = CliRunner()
        result1 = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_2.yaml'), '-l', 'info'])
        expects1 = """
        Set log level to info
        +-------------------------------+
        | Creating MASON_HOME at .tmp/  |
        +-------------------------------+
        +-------------------------------------------------------+
        | Creating OPERATOR_HOME at .tmp/registered_operators/  |
        +-------------------------------------------------------+
        +-----------------------------------------------+
        | Creating CONFIG_HOME at .tmp/configurations/  |
        +-----------------------------------------------+
        +----------------------+
        | Configuration Valid  |
        +----------------------+
        Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': '', 'configuration': {}}, 'execution': {'client_name': 'spark', 'configuration': {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}}}

        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/operators/table/test_configs/config_2.yaml to .tmp/configurations/
        """
        assert_multiline(result1.output, expects1)

        result2 = runner.invoke(register, [from_root('/examples/operators/table/')])
        expects2 = """
        Set log level to info
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/merge/operator.yaml
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/refresh/operator.yaml
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/get/operator.yaml
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/list/operator.yaml
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/infer/operator.yaml
        Registering operators at /Users/kyle/dev/mason/examples/operators/table/ to .tmp/registered_operators/table/
        """
        assert_multiline(result2.output, expects2)

        result3 = runner.invoke(operator, [])
        expects3 = """
            Set log level to info
            +----------------------+
            | Configuration Valid  |
            +----------------------+
            Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': '', 'configuration': {}}, 'execution': {'client_name': 'spark', 'configuration': {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}}}
            Neither parameter string nor parameter path provided.

            +---------------------------------------------------------+
            | Available Operator Methods: .tmp/registered_operators/  |
            +---------------------------------------------------------+

            namespace    command    description                                                                               parameters
            -----------  ---------  ----------------------------------------------------------------------------------------  ----------------------------------------------------------------
            table        merge      Merge metastore tables                                                                    {}
            table        refresh    Refresh metastore tables                                                                  {'required': ['database_name', 'table_name']}
            table        get        Get metastore table contents                                                              {'required': ['database_name', 'table_name']}
            table        list       Get metastore tables                                                                      {'required': ['database_name']}
            table        infer      Registers a schedule for infering the table then does a one time trigger of the refresh.  {'required': ['database_name', 'storage_path', 'schedule_name']}
        """
        assert_multiline(result3.output, expects3)

        result4 = runner.invoke(operator, ["table", "get", "-p", "database_name:lake-working-copy-feb-20-2020,table_name:sample", "-l", "trace"], catch_exceptions=False)
        expects4 = """
        Set log level to trace
        Reading configurations at .tmp/configurations/
        +----------------------+
        | Configuration Valid  |
        +----------------------+
        Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': '', 'configuration': {}}, 'execution': {'client_name': 'spark', 'configuration': {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}}}

        +--------------------+
        | Parsed Parameters  |
        +--------------------+
        {'table_name': 'sample', 'database_name': 'lake-working-copy-feb-20-2020'}


        +-------------------------+
        | Parameters Validation:  |
        +-------------------------+
        Validated: ['table_name', 'database_name']

        Fetching keys in lake-working-copy-feb-20-2020 sample
        Key lake-working-copy-feb-20-2020/sample/_SUCCESS
        Key lake-working-copy-feb-20-2020/sample/part-00000-1ab330bd-b4f8-4b65-b0d6-d670479602ea-c000.snappy.parquet
        Key lake-working-copy-feb-20-2020/sample/part-00001-1ab330bd-b4f8-4b65-b0d6-d670479602ea-c000.snappy.parquet
        +--------------------+
        | Operator Response  |
        +--------------------+
        {
         "Errors": [],
         "Info": [],
         "Warnings": [],
         "Data": {
          "Schema": {
           "SchemaType": "parquet",
           "Columns": [
            {
             "Name": "test_column_1",
             "Type": "INT32",
             "ConvertedType": "REQUIRED",
             "RepititionType": null
            },
            {
             "Name": "test_column_2",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            }
           ]
          }
         },
         "_client_responses": []
        }       
        """
        assert_multiline(result4.output, expects4)

        result5 = runner.invoke(operator, ["table", "get", "-p", "database_name:lake-working-copy-feb-20-2020,table_name:merged", "-l", "trace"], catch_exceptions=False)
        expects5 = """
        Set log level to trace
        Reading configurations at .tmp/configurations/
        +----------------------+
        | Configuration Valid  |
        +----------------------+
        Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': '', 'configuration': {}}, 'execution': {'client_name': 'spark', 'configuration': {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}}}

        +--------------------+
        | Parsed Parameters  |
        +--------------------+
        {'table_name': 'merged', 'database_name': 'lake-working-copy-feb-20-2020'}


        +-------------------------+
        | Parameters Validation:  |
        +-------------------------+
        Validated: ['table_name', 'database_name']

        Fetching keys in lake-working-copy-feb-20-2020 merged
        Key lake-working-copy-feb-20-2020/merged/_SUCCESS
        Key lake-working-copy-feb-20-2020/merged/part-00000-83438cb1-3701-49dc-baac-ccf9f2cabb2c-c000.snappy.parquet
        Key lake-working-copy-feb-20-2020/merged/part-00001-83438cb1-3701-49dc-baac-ccf9f2cabb2c-c000.snappy.parquet
        +--------------------+
        | Operator Response  |
        +--------------------+
        {
         "Errors": [],
         "Info": [],
         "Warnings": [],
         "Data": {
          "Schema": {
           "SchemaType": "parquet",
           "Columns": [
            {
             "Name": "AppId",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Collection",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TransactionTimeUtc",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Operation",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ModelVersion",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipmentId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipmentReference",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "CustomerId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Freight_ShipVia",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Freight_Carrier",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Freight_ServiceLevel",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Freight_TransportMode",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Freight_FreightCharge",
             "Type": "DOUBLE",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipmentType",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "IsFreightForwarded",
             "Type": "BOOLEAN",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "LeadTrackingNumber",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipFromFacility_Code",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipFromFacility_PackingLocation",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_LocationId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipTo_Description",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_AddressLine1",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_AddressLine2",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_AddressLine3",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_Country",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_CountryId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipTo_Address_Id",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipTo_Address_Locality",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_PostalCode",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_CompanyId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipTo_Address_Name",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_StateId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipTo_Address_StateCode",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_Address_CountryCode",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipTo_FacilityCode",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TotalQtyShipped",
             "Type": "DOUBLE",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ConsolidationMethod",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "CompletedByUserId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShippedByUserId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Status_StatusId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Status_Description",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Status_ChangedDate",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "PrecisionConfig_Client",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "PrecisionConfig_Locale",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "PrecisionConfig_PackingLocation",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "PrecisionConfig_Language",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "PortOfLading",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ShipDate",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "ReadyToShipDate",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "RealShipDate",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "DispatchDate",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "EndOfDayData_EndOfDayId",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "EndOfDayData_EodStartedDate",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_BillingId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Billing_CustomerBilling_CustomerId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Billing_CustomerBilling_Address_AddressId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Billing_CustomerBilling_Address_AddressName",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_Address_Division",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_Address_AddressLine1",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_Address_AddressLine2",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_Address_AddressLine3",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_Address_PostalCode",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_Address_City",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_Address_IsoCountryCode",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_Address_StateCode",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_IsBillTax",
             "Type": "BOOLEAN",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Billing_CustomerBilling_TermId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Billing_CustomerBilling_IsDistributor",
             "Type": "BOOLEAN",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Billing_CustomerBilling_IsoCurrencyCode",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Billing_CustomerBilling_CompanyId",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Billing_CustomerBilling_IsSAB",
             "Type": "BOOLEAN",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "Billing_IsBilled",
             "Type": "BOOLEAN",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "ShipAttention",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_SoldToAddress_CountryIso",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_SoldToAddress_Name",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_SoldToAddress_Line1",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_SoldToAddress_Line3",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_SoldToAddress_City",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_ShipToAddress_CountryIso",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_ShipToAddress_Name",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_ShipToAddress_Line1",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_ShipToAddress_City",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_ShipToAddress_State",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_BillToAddress_CountryIso",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_BillToAddress_Name",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_BillToAddress_Line1",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_BillToAddress_Line3",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_BillToAddress_City",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_SoldToAddress_Line2",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_ShipToAddress_Line2",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_BillToAddress_Line2",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_SoldToAddress_State",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_BillToAddress_State",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "Itn",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "TranslationInfo_ShipToAddress_Line3",
             "Type": "BYTE_ARRAY",
             "ConvertedType": "UTF8",
             "RepititionType": "OPTIONAL"
            },
            {
             "Name": "year",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "month",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            },
            {
             "Name": "day",
             "Type": "INT32",
             "ConvertedType": "OPTIONAL",
             "RepititionType": null
            }
           ]
          }
         },
         "_client_responses": []
        }
        """
        assert_multiline(result5.output, expects5)

        # result6 = runner.invoke(operator, ["table", "merge", "-l", "trace"], catch_exceptions=False)
        # print(result6.output)


    # def test_config_3(self):
    #     runner = CliRunner()


