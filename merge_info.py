from queue import Queue
from typing import Iterable

from time_helper import ChunkFilteringInfo, ChunkInfo


def merge_chunks_info_multiple_filters(
    reading_info: Queue[float],
    filtering_info: Iterable[Queue[tuple[int, ChunkFilteringInfo]]],
) -> list[ChunkInfo]:
    chunks_filtering_info = list[tuple[int, ChunkFilteringInfo]]()
    chunks_info = list[ChunkInfo]()
    for info in filtering_info:
        while not info.empty():
            chunks_filtering_info.append(info.get())
    chunks_filtering_info.sort(key=lambda tuple: tuple[0])
    for info in chunks_filtering_info:
        chunks_info.append(ChunkInfo(reading_info.get(), info[1]))
    return chunks_info


def merge_chunks_info(
    reading_info: Queue[float], filtering_info: Queue[tuple[int, ChunkFilteringInfo]]
) -> list[ChunkInfo]:
    chunks_info = list[ChunkInfo]()
    while not reading_info.empty():
        chunks_info.append(ChunkInfo(reading_info.get(), filtering_info.get()[1]))
    return chunks_info


def merge_chunks_info_multiple_filters2(
    reading_info: Queue[float],
    filtering_info: Queue[tuple[int, ChunkFilteringInfo]],
) -> list[ChunkInfo]:
    chunks_filtering_info = list[tuple[int, ChunkFilteringInfo]]()
    chunks_info = list[ChunkInfo]()
    while not filtering_info.empty():
        chunks_filtering_info.append(filtering_info.get())
    chunks_filtering_info.sort(key=lambda tuple: tuple[0])
    for info in chunks_filtering_info:
        chunks_info.append(ChunkInfo(reading_info.get(), info[1]))
    return chunks_info
