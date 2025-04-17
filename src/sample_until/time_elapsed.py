import multiprocessing as mp
import time
from typing import Any, Callable, Optional


def sample_until_time_elapsed(
    f: Callable[[], Any], duration_seconds: float, num_workers: Optional[int] = None
) -> list[Any]:
    """
    Run `f()` repeatedly for `duration_seconds` and collect its outputs.

    Args:
        f: Function to sample.
        duration_seconds: Total time in seconds to sample for.
        num_workers: Number of processes (defaults to 1). Pass `-1` for number of cpus.

    Returns:
        List of collected samples.
    """
    if num_workers is None:
        num_workers = 1
    if num_workers == -1:
        num_workers = mp.cpu_count()

    # Check if arguments are valid
    if num_workers < 1:
        raise ValueError("num_workers has to be >= 1 or -1")
    if duration_seconds <= 0:
        raise ValueError("duration_seconds has to be > 0")

    start_time = time.time()

    if num_workers == 1:
        return _sample_until_time_elapsed(f, duration_seconds, start_time)

    manager = mp.Manager()
    output_queue = manager.Queue()
    processes = [
        mp.Process(target=_worker, args=(f, duration_seconds, start_time, output_queue))
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
    duration: float,
    start_time: float,
):
    samples = []
    while (time.time() - start_time) < duration:
        samples.append(f())
    return samples


def _worker(
    f: Callable[[], Any],
    duration: float,
    start_time: float,
    output: mp.Queue,
):
    local_samples = _sample_until_time_elapsed(f, duration, start_time)
    output.put(local_samples)
