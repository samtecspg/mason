from typing import Union, List

import dask
from dask.dataframe import DataFrame
import dask.dataframe as dd
from dask.bytes import open_files
from dask.delayed import delayed
from dask_kubernetes import KubeCluster, make_pod_spec

from distributed import Client, fire_and_forget
from fsspec.core import OpenFile
from pandas import DataFrame as PDataFrame
import pandas as pd
from pyexcelerate import Workbook

def run(spec: dict, scheduler: str):
    
    class CompleteDaskJob:
        def __init__(self, message: str = ""):
            self.message = message

    class InvalidDaskJob():
        def __init__(self, message: str = ""):
            self.message = message

    def _write_excel(df: PDataFrame, fil: OpenFile, *, depend_on=None, **kwargs):
        with fil as f:
            values = [df.columns] + list(df.values)
            wb = Workbook()
            wb.new_sheet('sheet 1', data=values)
            wb.save(f)
        return None

    class DaskFormatJob():
        VALID_TEXT_FORMATS = ["csv", "csv-crlf"]
        VALID_JSON_FORMATS = ["json", "jsonl"]
        VALID_INPUT_FORMATS = VALID_TEXT_FORMATS + VALID_JSON_FORMATS + ["parquet"]
        VALID_OUTPUT_FORMATS = ["csv", "json", "xlsx", "parquet"]

        def __init__(self, job_spec: dict):
            self.input_paths: List[str] = job_spec.get('input_paths', [])
            self.input_format: str = job_spec.get('input_format', "")
            self.output_format: str = job_spec.get('output_format', "")
            self.line_terminator: str = job_spec.get('line_terminator', "")
            self.output_path: str = job_spec.get('output_path', "")
            self.partition_columns: List[str] = job_spec.get('partition_columns', [])
            self.filter_columns: List[str] = job_spec.get('filter_columns', [])

        def validate(self) -> Union[InvalidDaskJob, 'DaskFormatJob']:
            validation: Union[InvalidDaskJob, 'DaskFormatJob']
            if not (self.input_format in self.VALID_INPUT_FORMATS):
                validation = InvalidDaskJob(self.invalid_input_format())
            elif not (self.output_format in self.VALID_OUTPUT_FORMATS):
                validation = InvalidDaskJob(self.invalid_output_format())
            else:
                validation = self
            return validation

        def invalid_input_format(self) -> str:
            return f"Input Format {self.input_format} not supported for format implementation. Must be one of {','.join(self.VALID_INPUT_FORMATS)}"

        def invalid_output_format(self) -> str:
            return f"Output Format {self.output_format} not supported for format implementation. Must be one of {','.join(self.VALID_OUTPUT_FORMATS)}"

        def df(self) -> Union[DataFrame, InvalidDaskJob]:

            paths = self.input_paths
            df: DataFrame
            if self.input_format in self.VALID_TEXT_FORMATS:
                df = dd.read_csv(paths, lineterminator=self.line_terminator)
                final = df
            elif self.input_format == "parquet":
                df = dd.read_parquet(paths)
                final = df
            elif self.input_format in self.VALID_JSON_FORMATS:
                df = dd.read_json(paths)
                final = df
            else:
                final = InvalidDaskJob(self.invalid_input_format())
            return final

        def df_to(self, df: DataFrame) -> Union[CompleteDaskJob, InvalidDaskJob]:

            writer = pd.ExcelWriter('test_out.xlsx', engine='xlsxwriter')
            to_excel_chunk = delayed(_write_excel, pure=False)

            def to_xlsx(df: DataFrame, output_path: str):
                dfs = df.repartition(partition_size="10MB").to_delayed()
                def name_function(i: int):
                    return f"part_{i}.xlsx"
                
                files = open_files(
                    output_path,
                    mode="wb",
                    num=df.npartitions,
                    name_function=name_function
                )
                
                def replace_path(f: OpenFile) -> OpenFile:
                    p = f.path
                    f.path = p.replace(".xlsx.part", ".xlsx")
                    return f
                
                files = list(map(lambda f: replace_path(f), files))
                values = [to_excel_chunk(dfs[0], files[0])]
                values.extend(
                    [to_excel_chunk(d, f) for d, f in zip(dfs[1:], files[1:])]
                )
                delayed(values).compute()

            final: Union[CompleteDaskJob, InvalidDaskJob]
            if self.output_format == "csv":
                dd.to_csv(df, self.output_path)
                final = CompleteDaskJob()
            elif self.output_format == "parquet":
                dd.to_parquet(df, self.output_path)
                final = CompleteDaskJob()
            elif self.output_format == "json":
                dd.to_json(df, self.output_path)
                final = CompleteDaskJob()
            elif self.output_format == "xlsx":
                to_xlsx(df, self.output_path)
                final = CompleteDaskJob()
            else:
                final = InvalidDaskJob(self.invalid_output_format())
            return final

        def check_columns(self, df: DataFrame, columns: List[str]) -> Union[bool, InvalidDaskJob]:
            keys = df.dtypes.keys()
            diff = set(columns).difference(set(df.dtypes.keys()))
            if len(diff) == 0:
                return True
            else:
                return InvalidDaskJob(
                    f"Filter columns {', '.join(diff)} not a subset of {', '.join(keys)}")

        def run_job(self) -> Union[CompleteDaskJob, InvalidDaskJob]:
            df: Union[CompleteDaskJob, InvalidDaskJob] = self.df()

            if isinstance(df, DataFrame):
                if len(self.partition_columns) > 0:
                    check = self.check_columns(df, self.partition_columns)
                    if isinstance(check, InvalidDaskJob):
                        df = check
                    else:
                        df = df.shuffle(self.partition_columns)

                if len(self.filter_columns) > 0:
                    check = self.check_columns(df, self.filter_columns)
                    if isinstance(check, InvalidDaskJob):
                        df = check
                    else:
                        df = df[self.filter_columns]

            if isinstance(df, DataFrame):
                final: Union[CompleteDaskJob, InvalidDaskJob] = self.df_to(df)
            else:
                final = df

            if isinstance(final, InvalidDaskJob):
                raise Exception(final.message)
            else:
                return final

    dask_job = DaskFormatJob(spec)
    mode = "async"
    adaptive = True
    
    # TODO:  Abstract out mason-dask so local scheduler can inherit job
    if scheduler == "local":
        client = Client()
        dask_job.run_job()
    else:
        dask.config.set({'distributed.scheduler.allowed-failures': 50})
        spl = scheduler.split(":")
        
        if adaptive:
            host = spl[0]
            port = spl[1]

            pod_spec = make_pod_spec(
                image='daskdev/dask:latest', 
                memory_limit = '4G', 
                memory_request = '4G', 
                cpu_limit = 2, 
                cpu_request = 2, 
                env = { 'EXTRA_PIP_PACKAGES': 'git+https://github.com/dask/distributed s3fs pyexcelerate --upgrade', 'EXTRA_CONDA_PACKAGES': 'fastparquet -c conda-forge'}
            )
            cluster = KubeCluster(pod_spec)
            cluster.port = port
            cluster.host = host
            cluster.adapt(minimum=0, maximum=100)
            client = Client(cluster)
        else:
            client = Client(scheduler)
            
        future = client.submit(dask_job.run_job)
        if mode == "sync":
            client.gather(future)
        else:
            fire_and_forget(future)

