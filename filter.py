from abc import ABC, abstractmethod


class TextFilter(ABC):
    @abstractmethod
    def filter(self) -> bool:
        pass


class AhoCorasickFilter(TextFilter):
    def __init__(self):
        pass

    def filter(self) -> bool:
        pass


class RegexFilter(TextFilter):
    def __init__(self):
        pass

    def filter(self) -> bool:
        pass
