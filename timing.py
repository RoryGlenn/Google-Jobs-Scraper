import time
from contextlib import ContextDecorator
import logging


logger = logging.getLogger(__name__)


class TimeIt(ContextDecorator):
    """The `TimeIt` class is a context manager that measures the execution time of a block of code and
    prints the elapsed time.

    Parameters
    ----------
    func
        The `func` parameter is a function that we want to decorate with the timing functionality.

    Returns
    -------
        The timing_decorator function returns the wrapper function.

    """

    def __init__(self, func):
        self.func = func

    def __enter__(self):
        self.start_time = time.perf_counter()

    def __exit__(self, exc_type, exc_value, traceback):
        end_time = time.perf_counter()
        elapsed_time = end_time - self.start_time

        if isinstance(self.func, str):
            logger.debug(f"{self.func} took {elapsed_time:.6f} seconds to run.")
        else:
            logger.debug(
                f"{self.func.__name__} took {elapsed_time:.6f} seconds to run."
            )

    def __call__(self, *args, **kwargs):
        start_time = time.perf_counter()
        result = self.func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.debug(f"{self.func.__name__} took {elapsed_time:.6f} seconds to run.")
        return result
