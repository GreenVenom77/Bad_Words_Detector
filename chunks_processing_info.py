from dataclasses import dataclass
from queue import Queue
from time import time
from typing import Any


@dataclass
class ChunkFilteringInfo:
    filtering_time: float
    number_of_healthy: int
    number_of_unhealthy: int


class ChunkInfo(ChunkFilteringInfo):

    def __init__(self, reading_time: float, chunk_filter: ChunkFilteringInfo):
        super().__init__(
            filtering_time=chunk_filter.filtering_time,
            number_of_healthy=chunk_filter.number_of_healthy,
            number_of_unhealthy=chunk_filter.number_of_unhealthy,
        )
        self.reading_time = reading_time


def merge_chunks_info(
    reading_queue: Queue[float],
    filtering_queue: Queue[tuple[int, ChunkFilteringInfo]],
) -> list[ChunkInfo]:
    chunks_info = list[float | ChunkInfo]()
    # get all reading info
    while not reading_queue.empty():
        chunks_info.append(reading_queue.get())
    # merge each filter info with its reading time
    for _ in range(len(chunks_info)):
        index, filtering_info = filtering_queue.get()
        reading_info = chunks_info[index]
        chunks_info[index] = ChunkInfo(reading_info, filtering_info)
    return chunks_info


def elapsed(starting_time: float):
    """Return elapsed time from the starting time to the current time in format 00:00:00
    like 04:32:12"""
    curr_time = time()
    elapsed_time = curr_time - starting_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02.0f}:{minutes:02.0f}:{seconds:05.2f}"
