from typing import List, Optional, Tuple
import threading
import subprocess

def run_sys_call(command: List[str]) -> Tuple[List[str], List[str]]:
    sys_call = SysCall(command)
    sys_call.run()
    stdout = (sys_call.stdout or b"").decode("utf-8").split("\n")
    stderr = (sys_call.stderr or b"").decode("utf-8").split("\n")

    return stdout, stderr

class SysCall(threading.Thread):
    def __init__(self, command: List[str]):
        self.stdout: Optional[bytes] = None
        self.stderr: Optional[bytes] = None
        self.command = command
        threading.Thread.__init__(self)

    def run(self):
        p = subprocess.Popen(self.command,
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        self.stdout, self.stderr = p.communicate()

