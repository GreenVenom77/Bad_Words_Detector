from dataclasses import dataclass
import multiprocessing
from time import time
from typing import Any


@dataclass
class ChunkFilteringInfo:
    filtering_time: float
    number_of_healthy: int
    number_of_unhealthy: int


@dataclass
class ChunkInfo:
    reading_time: float
    filtering_info: ChunkFilteringInfo


def elapsed(starting_time: float):
    """Return elapsed time from the starting time to the current time in format 00:00:00
    like 04:32:12"""
    curr_time = time()
    elapsed_time = curr_time - starting_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02.0f}:{minutes:02.0f}:{seconds:05.2f}"
