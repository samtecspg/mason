from typing import List
import threading
import subprocess

from clients.response import Response

def run_sys_call(command: List[str], response: Response):
    sys_call = SysCall(command)
    sys_call.run()
    stdout = (sys_call.stdout or "").replace("\n", '')
    stderr = (sys_call.stderr or "").replace("\n", '')

    if len(stdout) > 0:
        response.add_info({"Logs": "\n".join(stdout[-100:])})
        response.add_response({"STDOUT": stdout})
    if len(stderr) > 0:
        response.add_error(stderr)
        response.add_response({"STDERR": stdout})
        response.set_status(500)

    return response

class SysCall(threading.Thread):
    def __init__(self, command: List[str]):
        self.stdout = None
        self.stderr = None
        self.command = command
        threading.Thread.__init__(self)

    def run(self):
        p = subprocess.Popen(self.command,
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        self.stdout, self.stderr = p.communicate()

