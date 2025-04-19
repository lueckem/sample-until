import itertools
import math
import multiprocessing as mp
import sys
import time
from itertools import islice
from typing import Callable, Iterable, Optional, Sized
from warnings import warn

import psutil

from .utils import _num_required_args


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

    # Check that at least one stopping condition is provided
    if duration_seconds is None and num_samples is None and memory_percentage is None:
        if f_args is None or isinstance(
            f_args, (itertools.repeat, itertools.cycle, itertools.count)
        ):
            raise ValueError("provide at least one stopping condition")

        if not isinstance(f_args, Sized):
            warn(
                "Could not determine if `f_args` is finite. Program may run indefinitely."
            )

    # Replace None with large values
    if duration_seconds is None:
        duration_seconds = float("inf")
    if num_samples is None:
        num_samples = sys.maxsize
    if memory_percentage is None:
        memory_percentage = 1.0
    if num_workers is None:
        num_workers = 1
    if num_workers == -1:
        num_workers = mp.cpu_count()

    # Check for validity
    if duration_seconds <= 0:
        raise ValueError("duration_seconds has to be > 0")
    if num_samples < 0:
        raise ValueError("num_samples has to be > 0")
    if memory_percentage < 0 or memory_percentage > 1:
        raise ValueError("memory_percentage has to be between 0 and 1")
    if num_workers < 1:
        raise ValueError("num_workers has to be >= 1 or -1")

    start_time = time.time()

    if num_workers == 1:
        if f_args is None:
            return _sample_until(
                f, start_time, duration_seconds, num_samples, memory_percentage
            )
        return _sample_until_f_args(
            f, f_args, start_time, duration_seconds, num_samples, memory_percentage
        )

    manager = mp.Manager()
    output_queue = manager.Queue()

    if f_args is None:
        processes = [
            mp.Process(
                target=_worker,
                args=(
                    f,
                    start_time,
                    duration_seconds,
                    math.ceil(num_samples / num_workers),
                    memory_percentage,
                    output_queue,
                ),
            )
            for _ in range(num_workers)
        ]
    else:
        processes = [
            mp.Process(
                target=_worker_f_args,
                args=(
                    f,
                    islice(f_args, i, None, num_workers),
                    start_time,
                    duration_seconds,
                    math.ceil(num_samples / num_workers),
                    memory_percentage,
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
    start_time: float,
    duration: float,
    num_samples: int,
    memory_percentage: float,
) -> list:
    samples = []
    while (
        (time.time() - start_time) < duration
        and len(samples) < num_samples
        and psutil.virtual_memory()[2] / 100.0 < memory_percentage
    ):
        samples.append(f())
    return samples


def _sample_until_f_args(
    f: Callable,
    f_args: Iterable,
    start_time: float,
    duration: float,
    num_samples: int,
    memory_percentage: float,
) -> list:
    samples = []
    for a in f_args:
        if (
            (time.time() - start_time) >= duration
            or len(samples) >= num_samples
            or psutil.virtual_memory()[2] / 100.0 >= memory_percentage
        ):
            break
        samples.append(f(a))
    return samples


def _worker(
    f: Callable,
    start_time: float,
    duration: float,
    num_samples: int,
    memory_percentage: float,
    output: mp.Queue,
):
    local_samples = _sample_until(
        f, start_time, duration, num_samples, memory_percentage
    )
    output.put(local_samples)


def _worker_f_args(
    f: Callable,
    f_args: Iterable,
    start_time: float,
    duration: float,
    num_samples: int,
    memory_percentage: float,
    output: mp.Queue,
):
    local_samples = _sample_until_f_args(
        f, f_args, start_time, duration, num_samples, memory_percentage
    )
    output.put(local_samples)
