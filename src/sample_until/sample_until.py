import itertools
import multiprocessing as mp
from itertools import islice
from typing import Callable, Iterable, Optional, Sized
from warnings import warn

from .stopping_conditions import StoppingCondition, create_stopping_conditions, stop
from .utils import num_required_args


def sample_until(
    f: Callable,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: Optional[int] = None,
) -> list:
    """
    Run `f` repeatedly until one of the given conditions is met and collect its outputs.

    The function `f` should either accept no arguments, or exactly one argument that is generated for each sample via `f_args`.
    If `f_args` is finite, running out of arguments is also a stopping condition.
    The stopping conditions might not be respected exactly,
    e.g., the elapsed time can be slightly longer than `duration_seconds` and the output list
    may contain slightly more or less samples than `num_samples`.

    Args:
        f: Function to sample.
        f_args: Iterable that generates input arguments for `f`.
        duration_seconds: Stop after time elapsed.
        num_samples: Stop after number of samples acquired.
        memory_percentage: Stop after system memory exceeds percentage, e.g., `0.8`.
        num_workers: Number of processes (defaults to 1). Pass `-1` for number of cpus.

    Returns:
        List of collected samples.
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

    # no multiprocessing
    if num_workers == 1:
        return _sample_until(f1, f_args, stopping_conditions)

    # multiprocessing
    manager = mp.Manager()
    output_queue = manager.Queue()
    processes = [
        mp.Process(
            target=_worker,
            args=(
                f1,
                islice(f_args, i, None, num_workers),
                stopping_conditions,
                output_queue,
            ),
        )
        for i in range(num_workers)
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    # Gather results
    all_samples = output_queue.get()
    while not output_queue.empty():
        all_samples.extend(output_queue.get())

    return all_samples


def _check_f_valid(f: Callable, f_args: Optional[Iterable]):
    num_args = -1
    try:
        num_args = num_required_args(f)
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


def _sample_until(
    f: Callable,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
) -> list:
    samples = []
    for a in f_args:
        samples.append(f(a))

        if stop(stopping_conditions, samples):
            return samples

    print("Stopped because all f_args were used.")
    return samples


def _worker(
    f: Callable,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
    output: mp.Queue,
):
    local_samples = _sample_until(f, f_args, stopping_conditions)
    output.put(local_samples)
