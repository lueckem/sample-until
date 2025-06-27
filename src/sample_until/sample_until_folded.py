import multiprocessing as mp
from itertools import islice
from typing import Any, Callable, Iterable, Optional

from .stopping_conditions import StoppingCondition, stop
from .utils import sanitize_inputs


def sample_until_folded(
    f: Callable,
    fold_function: Callable,
    fold_initial: Any,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: Optional[int] = None,
) -> Any:
    """
    Run `f` repeatedly until one of the given conditions is met and aggregate its outputs.

    The function `f` should either accept no arguments, or exactly one argument that is generated for each sample via `f_args`.
    If `f_args` is finite, running out of arguments is also a stopping condition.
    The stopping conditions might not be respected exactly,
    e.g., the elapsed time can be slightly longer than `duration_seconds` and the output list
    may contain slightly more or less samples than `num_samples`.

    The outputs of `f` are folded together (accumulated) via the `fold_function` into `acc`,
    i.e., `acc = fold_function(acc, f())` with initial value `acc = fold_initial`.
    For example, to sum up all outputs of `f`, use `fold_function(acc, x) = acc + x`.

    Args:
        f: Function to sample.
        fold_function: Function used for accumulating results.
        fold_initial: Initial value for the accumulation.
        f_args: Iterable that generates input arguments for `f`.
        duration_seconds: Stop after time elapsed.
        num_samples: Stop after number of samples acquired.
        memory_percentage: Stop after system memory exceeds percentage, e.g., `0.8`.
        num_workers: Number of processes (defaults to 1). Pass `-1` for number of cpus.

    Returns:
        Accumulated result `acc`.
    """
    f1, f_args, num_workers, stopping_conditions = sanitize_inputs(
        f, f_args, duration_seconds, num_samples, memory_percentage, num_workers
    )

    # no multiprocessing
    if num_workers == 1:
        return _sample_until_folded(
            f1, f_args, stopping_conditions, fold_function, fold_initial
        )

    # multiprocessing
    return 0


def _sample_until_folded(
    f: Callable,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
    fold_function: Callable,
    fold_initial: Any,
) -> list:
    acc = fold_initial
    i = 0
    for a in f_args:
        acc = fold_function(acc, f(a))
        i += 1

        if stop(stopping_conditions, i):
            return acc

    print("Stopped because all f_args were used.")
    return acc
