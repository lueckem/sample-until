import multiprocessing as mp
from itertools import islice
from typing import Any, Callable, Iterable, Optional
from warnings import warn

from .stopping_conditions import StoppingCondition, create_stopping_conditions, stop
from .utils import check_fold_function, sanitize_inputs

# TODO: Printing. (Verbose or not verbose)

# TODO: prettier warnings


class DoneSignal:
    pass


def folded_sample_until(
    f: Callable,
    fold_function: Callable,
    fold_initial: Any,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: int = 1,
    batch_size: int = 1,
) -> tuple[Any, int]:
    """
    Run `f` repeatedly until one of the given conditions is met and aggregate its outputs.

    The function `f` should either accept no arguments, or exactly one argument that is generated for each sample via `f_args`.
    If `f_args` is finite, running out of arguments is also a stopping condition.
    The stopping conditions might not be respected exactly,
    e.g., the elapsed time can be slightly longer than `duration_seconds`.

    The outputs of `f` are folded together (accumulated) via the `fold_function` into `acc`,
    i.e., `acc = fold_function(acc, f())` with initial value `acc = fold_initial`.
    For example, to sum up all outputs of `f`, the `fold_function(acc, x)` should return `acc + x`.

    If `num_workers > 1`, there will be `1` folding process and `num_workers - 1` sampling processes
    that send their generated samples to the folding process.

    Args:
        f: Function to sample.
        fold_function: Function used for accumulating results.
        fold_initial: Initial value for the accumulation.
        f_args: Iterable that generates input arguments for `f`.
        duration_seconds: Stop after time elapsed.
        num_samples: Stop after number of samples acquired.
        memory_percentage: Stop after system memory exceeds percentage, e.g., `0.8`.
        num_workers: Number of processes. Pass `-1` for number of cpus.
        batch_size: Only if num_workers > 1: send samples to folding process in batches.

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
    num_workers -= 1  # one process is reserved for the aggregator
    # recreate stopping conditions because of the new worker count
    stopping_conditions = create_stopping_conditions(
        num_workers, duration_seconds, num_samples, memory_percentage
    )
    output_queue = mp.Queue(2 * num_workers)
    aggregator_queue = mp.Queue()

    processes = [
        mp.Process(
            target=_worker,
            args=(
                f1,
                islice(f_args, i, None, num_workers),
                stopping_conditions,
                batch_size,
                output_queue,
            ),
        )
        for i in range(num_workers)
    ]

    aggregator = mp.Process(
        target=_aggregate,
        args=(
            output_queue,
            aggregator_queue,
            fold_function,
            fold_initial,
            num_workers,
        ),
    )
    aggregator.start()

    for p in processes:
        p.start()

    for p in processes:
        p.join()
        output_queue.put(DoneSignal())

    # get output from aggregation process
    while True:
        crashed = False
        warned = False
        try:
            output = aggregator_queue.get(timeout=20)
            aggregator.join()
            return output
        except:
            if not aggregator.is_alive():
                crashed = True
        if crashed:
            raise RuntimeError("Folding process crashed!")
        if not warned:
            warn(
                "Waiting for the folding process to finish folding. If the program does not terminate, check your folding function."
            )
            warned = True


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
    warned = False

    while finished_workers < num_workers:
        if not warned and output_queue.full():
            warn(
                "Accumulation queue is full! This indicates that the folding process can not keep up with the incoming samples. The sampling processes have to wait for free slots in the queue."
            )
            warned = True
        item = output_queue.get()
        if isinstance(item, DoneSignal):
            finished_workers += 1
        else:  # item is a batch of samples
            i += len(item)
            for x in item:
                acc = fold_function(acc, x)

    aggregator_queue.put((acc, i))


def _worker(
    f: Callable,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
    batch_size: int,
    output_queue: mp.Queue,
):
    i = 0
    batch = []
    for a in f_args:
        batch.append(f(a))
        if len(batch) >= batch_size:
            output_queue.put(batch)
            batch = []
        i += 1

        if stop(stopping_conditions, i):
            if len(batch) > 0:
                output_queue.put(batch)
            return

    print("Stopped because all f_args were used.")
    if len(batch) > 0:
        output_queue.put(batch)
