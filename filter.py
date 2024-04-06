from abc import ABC, abstractmethod
from functools import reduce
import re
import ahocorasick
from pandas import DataFrame


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
    def filter(self, chunk: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Filters Healthy and UnHealthy Rows"""
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

    def filter(self, chunk: DataFrame) -> tuple[DataFrame, DataFrame]:
        health_filter = reduce(
            lambda x, y: x & y,
            [
                chunk[column]
                .astype(str)
                .apply(lambda field: len(list(self.automaton.iter(field.lower()))) == 0)
                for column in chunk.columns
            ],
        )
        return chunk[health_filter], chunk[~health_filter]

    def __repr__(self) -> str:
        return "AhoCorasick"


class RegexFilter(TextFilter):
    def __init__(self, bad_words: list[str]):
        self.bad_words = bad_words

    def prepare(self) -> None:
        self.pattern = "|".join(map(re.escape, self.bad_words))

    def filter(self, chunk: DataFrame) -> tuple[DataFrame, DataFrame]:

        health_filter = reduce(
            lambda x, y: x & y,
            [
                ~chunk[column]
                .astype(str)
                .str.contains(self.pattern, regex=True, flags=re.I, na=False)
                for column in chunk.columns
            ],
        )
        return chunk[health_filter], chunk[~health_filter]

    def __repr__(self) -> str:
        return "Regex"
