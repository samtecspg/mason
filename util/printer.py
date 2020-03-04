
def pprint(d: dict):
    for key, value in d.items():
        print('{}: {}'.format(key, value))

def banner(s: str):
    m = 2
    l = len(s) + (m +1)
    print("+" + "-" * l + "+")
    print("| " + s + " " * m + "|")
    print("+" + "-" * l + "+")
