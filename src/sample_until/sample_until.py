import multiprocessing as mp
from itertools import islice
from typing import Callable, Iterable, Optional

from .stopping_conditions import StoppingCondition, stop
from .utils import sanitize_inputs


def sample_until(
    f: Callable,
    f_args: Optional[Iterable] = None,
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: int = 1,
    verbose: bool = False,
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
        num_workers: Number of processes. Pass `-1` for number of cpus.
        verbose: Print due to which condition the sampling stopped.

    Returns:
        List of collected samples.
    """
    f1, f_args, num_workers, stopping_conditions = sanitize_inputs(
        f, f_args, duration_seconds, num_samples, memory_percentage, num_workers
    )

    # no multiprocessing
    if num_workers == 1:
        return _sample_until(f1, f_args, stopping_conditions, verbose)

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
                verbose,
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
    verbose: bool,
) -> list:
    samples = []
    for a in f_args:
        samples.append(f(a))

        if stop(stopping_conditions, len(samples), verbose):
            return samples

    print("Stopped because all f_args were used.")
    return samples


def _worker(
    f: Callable,
    f_args: Iterable,
    stopping_conditions: list[StoppingCondition],
    output: mp.Queue,
    verbose: bool,
):
    local_samples = _sample_until(f, f_args, stopping_conditions, verbose)
    output.put(local_samples)
