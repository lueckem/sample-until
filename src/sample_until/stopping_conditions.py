import time
from dataclasses import dataclass
from math import ceil
from typing import Optional, Protocol

import psutil


class StoppingCondition(Protocol):
    # Return if the sampling should be stopped
    def stop(self, num_samples: int) -> bool: ...

    # Return a message explaining why the sampling was stopped
    def stop_message(self) -> str: ...


def create_stopping_conditions(
    num_workers: int,
    duration_seconds: Optional[float],
    num_samples: Optional[int],
    memory_percentage: Optional[float],
) -> list[StoppingCondition]:
    stopping_conditions = []
    if duration_seconds is not None:
        stopping_conditions.append(TimeElapsed(time.time(), duration_seconds))
    if num_samples is not None:
        # divide samples between workers
        num_samples = ceil(num_samples / num_workers)
        stopping_conditions.append(NumSamples(num_samples))
    if memory_percentage is not None:
        stopping_conditions.append(MemoryPercentage(memory_percentage))
    return stopping_conditions


def stop(stopping_conditions: list[StoppingCondition], num_samples: int) -> bool:
    for sc in stopping_conditions:
        if sc.stop(num_samples):
            print(sc.stop_message())
            return True
    return False


@dataclass
class TimeElapsed:
    start_time: float
    duration_seconds: float

    def __post_init__(self):
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds has to be > 0")

    def stop(self, _: list) -> bool:
        return (time.time() - self.start_time) >= self.duration_seconds

    def stop_message(self) -> str:
        return "Stopped because time elapsed."


@dataclass
class NumSamples:
    num_samples: int

    def __post_init__(self):
        if self.num_samples <= 0:
            raise ValueError("num_samples has to be > 0")

    def stop(self, num_samples: int) -> bool:
        return num_samples >= self.num_samples

    def stop_message(self) -> str:
        return "Stopped because number of samples reached."


@dataclass
class MemoryPercentage:
    memory_percentage: float

    def __post_init__(self):
        if self.memory_percentage < 0 or self.memory_percentage > 1:
            raise ValueError("memory_percentage has to be between 0 and 1")

    def stop(self, _: list) -> bool:
        return psutil.virtual_memory()[2] / 100.0 >= self.memory_percentage

    def stop_message(self) -> str:
        return "Stopped because memory usage exceeded."
