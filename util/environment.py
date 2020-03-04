import os

HOME = os.path.expanduser('~')

def get_mason_home():
    try:
        return os.environ['MASON_HOME']
    except KeyError:
        return os.path.join(HOME, '.mason/')

MASON_HOME = get_mason_home()
CONFIG_HOME = MASON_HOME + "config.yaml"
OPERATOR_HOME = MASON_HOME + "operators/"
