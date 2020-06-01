import os

def root():

    d = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return d

def from_root(path: str):
    return root() + path
