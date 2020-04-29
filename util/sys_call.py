from typing import List
import threading
import subprocess

from clients.response import Response

def run_sys_call(command: List[str]):
    sys_call = SysCall(command)
    sys_call.run()
    stdout = (sys_call.stdout or b"").decode("utf-8").replace("\n", '')
    stderr = (sys_call.stderr or b"").decode("utf-8").replace("\n", '')


    return stdout, stderr

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

