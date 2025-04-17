import math
import multiprocessing as mp
import sys
import time
from typing import Any, Callable, Optional

import psutil


def sample_until(
    f: Callable[[], Any],
    duration_seconds: Optional[float] = None,
    num_samples: Optional[int] = None,
    memory_percentage: Optional[float] = None,
    num_workers: Optional[int] = None,
) -> list[Any]:
    """
    Run `f()` repeatedly until one of the given conditions is met and collect its outputs.

    The conditions might not be respected exactly,
    e.g., the elapsed time can be slightly longer than `duration_seconds` and the output list
    may contain slightly more or less samples than `num_samples`.

    Args:
        f: Function to sample.
        duration_seconds: Stop after time elapsed.
        num_samples: Stop after number of samples acquired.
        memory_percentage: Stop after system memory exceeds percentage, e.g., `0.8`.
        num_workers: Number of processes (defaults to 1). Pass `-1` for number of cpus.

    Returns:
        List of collected samples.
    """
    # Check that at least one stopping condition is provided
    if duration_seconds is None and num_samples is None and memory_percentage is None:
        raise ValueError("provide at least one stopping condition")

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
        return _sample_until_time_elapsed(
            f, start_time, duration_seconds, num_samples, memory_percentage
        )

    manager = mp.Manager()
    output_queue = manager.Queue()
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

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    # Gather results
    all_samples = output_queue.get()
    while not output_queue.empty():
        all_samples.extend(output_queue.get())

    return all_samples


def _sample_until_time_elapsed(
    f: Callable[[], Any],
    start_time: float,
    duration: float,
    num_samples: int,
    memory_percentage: float,
):
    samples = []
    while (
        (time.time() - start_time) < duration
        and len(samples) < num_samples
        and psutil.virtual_memory()[2] / 100.0 < memory_percentage
    ):
        samples.append(f())
    return samples


def _worker(
    f: Callable[[], Any],
    start_time: float,
    duration: float,
    num_samples: int,
    memory_percentage: float,
    output: mp.Queue,
):
    local_samples = _sample_until_time_elapsed(
        f, start_time, duration, num_samples, memory_percentage
    )
    output.put(local_samples)
