import os

def root():
    return os.path.dirname(os.path.abspath(__file__))

def from_root(path: str):
    return root() + path