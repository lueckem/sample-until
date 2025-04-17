import multiprocessing as mp
import time
from typing import Any, Callable, Optional


def sample_until_time_elapsed(
    f: Callable[[], Any], duration_seconds: float, num_workers: Optional[int] = None
) -> list[Any]:
    """
    Run `f()` repeatedly for `duration_seconds`.

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

    start_time = time.time()

    if num_workers == 1:
        all_samples = []
        while (time.time() - start_time) < duration_seconds:
            all_samples.append(f())
        return all_samples

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


def _worker(
    f: Callable[[], Any],
    duration: float,
    start_time: float,
    output: mp.Queue,
):
    local_samples = []
    while (time.time() - start_time) < duration:
        local_samples.append(f())
    output.put(local_samples)
