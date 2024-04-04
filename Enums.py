from enum import Enum


class FilterMode(Enum):
    Regex = 0
    AhoCorasick = 1


class ProcessingMode(Enum):
    MultiThreading = 0
    Multiprocessing = 1
    ProcessesPool = 2
