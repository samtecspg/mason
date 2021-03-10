import cProfile
import pstats


def profiler(func):
    def wrapper(*args, **kwargs):
        with cProfile.Profile() as pr:
            result = func(*args, **kwargs)
        pstats.Stats(pr).sort_stats("tottime").print_stats()
        return result
    return wrapper

