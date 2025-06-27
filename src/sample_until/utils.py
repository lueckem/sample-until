import inspect
import itertools
import multiprocessing as mp
from typing import Callable, Iterable, Optional, Sized
from warnings import warn

from .stopping_conditions import StoppingCondition, create_stopping_conditions


def sanitize_inputs(
    f: Callable,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: Optional[int] = None,
) -> tuple[Callable, Iterable, int, list[StoppingCondition]]:
    """Sanitize and check inputs, and create stopping conditions.

    Throws an error if the inputs are invalid.
    """
    # Check if f accepts a valid number of arguments
    _check_f_valid(f, f_args)

    # Replace f with a function f1 that always accepts one argument
    if f_args is None:
        f_args = itertools.repeat(None)
        f1 = lambda _: f()
    else:
        f1 = f

    # Check and set num_workers
    num_workers = _set_num_workers(num_workers)

    stopping_conditions = create_stopping_conditions(
        num_workers, duration_seconds, num_samples, memory_percentage
    )

    # Check that at least one stopping condition is provided
    _check_stopping_conditions(stopping_conditions, f_args)

    return f1, f_args, num_workers, stopping_conditions


def check_fold_function(fold_function: Callable):
    num_args = 2
    try:
        num_args = _num_required_args(fold_function)
    except:
        warn("Could not determine how many arguments fold_function requires.")
    if num_args != 2:
        raise ValueError("fold_function must accept exactly 2 arguments.")


def _check_f_valid(f: Callable, f_args: Optional[Iterable]):
    num_args = -1
    try:
        num_args = _num_required_args(f)
    except:
        warn("Could not determine how many arguments f requires.")

    if num_args == 0 and f_args is not None:
        raise ValueError("f accepts no arguments but f_args was provided")
    if num_args == 1 and f_args is None:
        raise ValueError("f_args has to be provided")
    if num_args > 1:
        raise ValueError("f is not allowed to accept more than 1 argument")


def _set_num_workers(num_workers: Optional[int]) -> int:
    if num_workers is None:
        return 1
    if num_workers == -1:
        return mp.cpu_count()
    if num_workers <= 0:
        raise ValueError("num_workers has to be >= 1")
    return num_workers


def _check_stopping_conditions(
    stopping_conditions: list[StoppingCondition], f_args: Optional[Iterable]
):
    if len(stopping_conditions) == 0:
        if f_args is None or isinstance(
            f_args, (itertools.repeat, itertools.cycle, itertools.count)
        ):
            raise ValueError("provide at least one stopping condition")

        if not isinstance(f_args, Sized):
            warn(
                "Could not determine if `f_args` is finite. Program may run indefinitely."
            )


def _num_required_args(func: Callable) -> int:
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
