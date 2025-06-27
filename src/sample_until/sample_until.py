import multiprocessing as mp
from itertools import islice
from typing import Any, Callable, Iterable, Optional

from .stopping_conditions import StoppingCondition, stop
from .utils import sanitize_inputs


def sample_until(
    f: Callable,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: Optional[int] = None,
    fold_function: Optional[Callable] = None,
    fold_initial: Optional[Any] = None,
):
    """
    Run `f` repeatedly until one of the given conditions is met and collect its outputs.

    The function `f` should either accept no arguments, or exactly one argument that is generated for each sample via `f_args`.
    If `f_args` is finite, running out of arguments is also a stopping condition.
    The stopping conditions might not be respected exactly,
    e.g., the elapsed time can be slightly longer than `duration_seconds` and the output list
    may contain slightly more or less samples than `num_samples`.

    Optionally, ...

    Args:
        f: Function to sample.
        f_args: Iterable that generates input arguments for `f`.
        duration_seconds: Stop after time elapsed.
        num_samples: Stop after number of samples acquired.
        memory_percentage: Stop after system memory exceeds percentage, e.g., `0.8`.
        num_workers: Number of processes (defaults to 1). Pass `-1` for number of cpus.

    Returns:
        List of collected samples or ...
    """
    f1, f_args, num_workers, stopping_conditions = sanitize_inputs(
        f, f_args, duration_seconds, num_samples, memory_percentage, num_workers
    )

    # no multiprocessing
    if num_workers == 1:
        if fold_function is None:
            return _sample_until(f1, f_args, stopping_conditions)
        else:
            return _sample_until_fold(
                f1, f_args, stopping_conditions, fold_function, fold_initial
            )

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


def _sample_until(
    f: Callable,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
) -> list:
    samples = []
    for a in f_args:
        samples.append(f(a))

        if stop(stopping_conditions, len(samples)):
            return samples

    print("Stopped because all f_args were used.")
    return samples


def _sample_until_fold(
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


def _worker(
    f: Callable,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
    output: mp.Queue,
):
    local_samples = _sample_until(f, f_args, stopping_conditions)
    output.put(local_samples)
