import multiprocessing as mp
from itertools import islice
from typing import Any, Callable, Iterable, Optional

from .stopping_conditions import StoppingCondition, stop
from .utils import check_fold_function, sanitize_inputs

DONE = object()


def sample_until_folded(
    f: Callable,
    fold_function: Callable,
    fold_initial: Any,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: Optional[int] = None,
) -> tuple[Any, int]:
    """
    Run `f` repeatedly until one of the given conditions is met and aggregate its outputs.

    The function `f` should either accept no arguments, or exactly one argument that is generated for each sample via `f_args`.
    If `f_args` is finite, running out of arguments is also a stopping condition.
    The stopping conditions might not be respected exactly,
    e.g., the elapsed time can be slightly longer than `duration_seconds`.

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
        Accumulated result `acc` and number of iterations.
    """
    f1, f_args, num_workers, stopping_conditions = sanitize_inputs(
        f, f_args, duration_seconds, num_samples, memory_percentage, num_workers
    )

    # check if it accepts exactly 2 arguments
    check_fold_function(fold_function)

    # no multiprocessing
    if num_workers == 1:
        return _sample_until_folded(
            f1, fold_function, fold_initial, f_args, stopping_conditions
        )

    # multiprocessing
    manager = mp.Manager()
    output_queue = manager.Queue()
    aggregator_queue = manager.Queue()

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

    aggregator = mp.Process(
        target=_aggregate,
        args=(output_queue, aggregator_queue, fold_function, fold_initial, num_workers),
    )
    aggregator.start()

    for p in processes:
        p.start()

    for p in processes:
        p.join()
        output_queue.put(DONE)

    aggregator.join()
    return aggregator_queue.get_nowait()


def _sample_until_folded(
    f: Callable,
    fold_function: Callable,
    fold_initial: Any,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
):
    acc = fold_initial
    i = 0
    for a in f_args:
        acc = fold_function(acc, f(a))
        i += 1

        if stop(stopping_conditions, i):
            return acc, i

    print("Stopped because all f_args were used.")
    return acc, i


def _aggregate(
    output_queue: mp.Queue,
    aggregator_queue: mp.Queue,
    fold_function: Callable,
    fold_initial: Any,
    num_workers: int,
):
    finished_workers = 0
    acc = fold_initial
    i = 0

    while finished_workers < num_workers:
        item = output_queue.get()
        if item is DONE:
            finished_workers += 1
        else:
            acc = fold_function(acc, item)
            i += 1

    aggregator_queue.put((acc, i))


def _worker(
    f: Callable,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
    output_queue: mp.Queue,
):
    i = 0
    for a in f_args:
        x = f(a)
        output_queue.put(x)
        i += 1

        if stop(stopping_conditions, i):
            return

    print("Stopped because all f_args were used.")
    return
