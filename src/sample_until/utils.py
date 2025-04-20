import inspect
from typing import Callable


def num_required_args(func: Callable) -> int:
    """Number of required arguments of a function.

    Raises an Exception if the signature of `func` cannot be inspected.
    """
    sig = inspect.signature(func)
    params = sig.parameters.values()

    required_params = [
        p
        for p in params
        if p.default is inspect.Parameter.empty
        and p.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]

    return len(required_params)
