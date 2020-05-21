
from util.logger import logger
from typing import Optional, Union
from os import environ
import re

def safe_interpolate_environment(config_doc: dict):
    return {k: interpolate_value(v) for k, v in config_doc.items()}

def interpolate_value(value: Union[str, dict]) -> Optional[Union[str, dict]]:

    SAFE_KEYS = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_REGION',
        'MASON_HOME',
        'KUBECONFIG',
        'GLUE_ROLE_ARN'
    ]

    r = re.compile(r'^\{\{[A-Z0-9_]+\}\}$')
    interpolated: Optional[Union[str, dict]]
    if not value.__class__.__name__ == "dict":  # TODO: deal with nested configuration structures
        # TODO: Fix type
        v: str = value  # type: ignore
        if r.match(v):
            key = v.replace("{{", "").replace("}}", "")
            if key in SAFE_KEYS:
                interpolated = environ.get(key)
                if interpolated is None:
                    logger.error(
                        f"Undefined environment interpolation for key {{{key}}}.  Check that {key} is defined in your .env")
            else:
                logger.error(f"Unpermitted Interpolation for key {{{key}}}.  Must be one of {','.join(SAFE_KEYS)}")
                interpolated = None
        else:
            interpolated = v
    else:
        interpolated = value

    return interpolated


