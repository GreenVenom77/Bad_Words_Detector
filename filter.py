from abc import ABC, abstractmethod
from functools import reduce
import re
import ahocorasick
from pandas import DataFrame, Series


# interface (abstract class)
class TextFilter(ABC):
    @abstractmethod
    def __init__(self, bad_words: list[str]):
        pass

    @abstractmethod
    def prepare() -> None:
        """you must call once before you calling filter function"""
        pass

    @abstractmethod
    def filter(self, chunk: DataFrame) -> tuple[int, int]:
        """Filters Healthy and UnHealthy Rows count"""
        pass


class AhoCorasickFilter(TextFilter):
    def __init__(self, bad_words: list[str]):
        self.bad_words = bad_words

    def prepare(self) -> None:
        # making the trie and aho search
        self.automaton = ahocorasick.Automaton()
        for word in map(lambda x: x.lower(), self.bad_words):
            self.automaton.add_word(word, word)
        self.automaton.make_automaton()

    def filter(self, chunk: DataFrame) -> tuple[int, int]:
        # health_filter is a series of bool values like =[0,1,0,1,1,...]
        # each item represent if row is healthy or not
        # so the sample [0,1,0,1,1] means
        # that there is two heathy rows whose index 0,2 and 3 healthy rows 1,3,4
        health_filter = reduce(
            lambda x, y: x & y,
            [
                chunk[column]
                .astype(str)
                .apply(lambda field: len(list(self.automaton.iter(field.lower()))) == 0)
                for column in chunk.columns
            ],
        )
        healthy_rows_number = sum(health_filter)
        unhealthy_rows_number = len(health_filter) - healthy_rows_number
        return healthy_rows_number, unhealthy_rows_number

    def __repr__(self) -> str:
        return "AhoCorasick"


class RegexFilter(TextFilter):
    def __init__(self, bad_words: list[str]):
        self.bad_words = bad_words

    def prepare(self) -> None:
        self.pattern = "|".join(map(re.escape, self.bad_words))

    def filter(self, chunk: DataFrame) -> tuple[int, int]:
        # health_filter is a series of bool values like =[0,1,0,1,1,...]
        # each item represent if row is healthy or not
        # so the sample [0,1,0,1,1] means
        # that there is two heathy rows whose index 0,2 and 3 healthy rows 1,3,4
        health_filter = reduce(
            lambda x, y: x & y,
            [
                ~chunk[column]
                .astype(str)
                .str.contains(self.pattern, regex=True, flags=re.I, na=False)
                for column in chunk.columns
            ],
        )
        healthy_rows_number = sum(health_filter)
        unhealthy_rows_number = len(health_filter) - healthy_rows_number
        return healthy_rows_number, unhealthy_rows_number

    def __repr__(self) -> str:
        return "Regex"
