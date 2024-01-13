"""
Small utilities for working with progressbars
"""
from contextlib import contextmanager

from tqdm.cli import tqdm


class DummyPbar:
    def update(*args, **kwargs):
        pass


@contextmanager
def tqdm_if_verbose(*args, verbose: bool = True, **kwargs):
    if verbose:
        with tqdm(*args, **kwargs) as pbar:
            yield pbar
    else:
        yield DummyPbar()
