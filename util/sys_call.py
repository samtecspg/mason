from typing import List
from subprocess import Popen, PIPE, STDOUT
from util.logger import logger

def run_sys_call(command: List[str]):
    with Popen(command, stdout=PIPE) as proc:
        stdin = proc.stdin.read().decode("utf-8").split("\n") if proc.stdin is not None else []
        stdout = proc.stdout.read().decode("utf-8").split("\n") if proc.stdout is not None else []
        stderr = proc.stderr.read().decode("utf-8").split("\n") if proc.stderr is not None else []

        if len(stdin) >0 :
            logger.debug(f"STDIN {stdin}")
        if len(stdout) > 0 :
            logger.debug(f"STDOUT: {stdout}")
        elif len(stderr) > 0:
            logger.error(f"STDERR: {str(stderr)}")
        return stdout, stderr
