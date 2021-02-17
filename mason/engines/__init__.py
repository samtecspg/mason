from typing import Optional, Union
from os import environ
import re

from botocore.configloader import raw_config_parse
from botocore.exceptions import ConfigNotFound

from mason.util.logger import logger

def safe_interpolate_environment(config_doc: dict, credential_file:str = "~/.aws/credentials") -> Optional[dict]:
    aws_profile = environ.get('AWS_PROFILE') or "default"

    try:
        config = raw_config_parse(credential_file).get(aws_profile)
    except ConfigNotFound as e:
        logger.warning("AWS Config not found")
        config = None

    return {k: interpolate_value(v, config) for k, v in config_doc.items()}

def interpolate_value(value: Union[str, dict], credentials: Optional[dict]) -> Optional[Union[str, dict]]:

    SAFE_KEYS = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_REGION',
        'MASON_HOME',
        'KUBECONFIG',
        'GLUE_ROLE_ARN',
        'DASK_SCHEDULER'
        'AIRLFLOW_SCHEDULER',
        'AIRFLOW_USER',
        'AIRFLOW_PASSWORD',
        'DASK_SCHEDULER'
    ]

    r = re.compile(r'^\{\{[A-Z0-9_]+\}\}$')
    interpolated: Optional[Union[str, dict]]
    if isinstance(value, str):
        # TODO: Fix type
        if r.match(value):
            key = value.replace("{{", "").replace("}}", "")
            if key in SAFE_KEYS:
                sub = None
                if credentials:
                    if key == 'AWS_ACCESS_KEY_ID':
                        sub = credentials.get("aws_access_key_id") 
                    elif key == 'AWS_SECRET_ACCESS_KEY':
                        sub = credentials.get("aws_secret_access_key")
                    elif key == 'AWS_REGION':
                        sub = credentials.get("aws_region")
                    
                interpolated = sub or environ.get(key)
                
                if interpolated is None:
                    logger.warning(
                        f"Undefined environment interpolation for key {{{key}}}.  Check that {key} is defined in your .env")
            else:
                logger.error(f"Unpermitted Interpolation for key {{{key}}}.  Must be one of {','.join(SAFE_KEYS)}")
                interpolated = None
        else:
            interpolated = value
    elif isinstance(value, dict):
        interpolated = {k: interpolate_value(v, credentials) for (k, v) in value.items()}
    else:
        interpolated = value

    return interpolated


