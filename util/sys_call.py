from typing import List
from clients.response import Response
from subprocess import Popen, PIPE
from util.logger import logger

def run_sys_call(command: List[str], response: Response):
    with Popen(command, stdout=PIPE) as proc:
        stdin = proc.stdin.read() if proc.stdin is not None else None
        stdout = proc.stdout.read() if proc.stdout is not None else None
        stderr = proc.stderr.read() if proc.stderr is not None else None

        if stdin:
            logger.info(f"STDIN {str(stdin)}")
        if stdout:
            logger.info(f"STDOUT: {str(stdout)}")
            response.add_response({"stdout": stdout.decode()})
        elif stderr:
            logger.info(f"STDERR: {str(stderr)}")
            response.add_error(str(stderr))
        return stdout, stderr
