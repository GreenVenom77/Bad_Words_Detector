import re
from abc import ABC, abstractmethod
import ahocorasick
from pandas import DataFrame


# interface (abstract class)
class TextFilter(ABC):

    @abstractmethod
    def prepare(self, bad_words: list[str]) -> None:
        """you must call once before you calling filter function"""
        pass

    @abstractmethod
    def is_unhealthy(self, field: str) -> bool:
        pass

    def filter(self, chunk: DataFrame) -> tuple[int, int]:
        """Filters Healthy and UnHealthy Rows count"""
        health_filter = chunk.apply(
            lambda row: not any(
                map(
                    lambda field: self.is_unhealthy(str(field)),
                    row,
                )
            ),
            axis=1,
        )
        healthy_rows_number = sum(health_filter)
        unhealthy_rows_number = len(health_filter) - healthy_rows_number
        return healthy_rows_number, unhealthy_rows_number


class AhoCorasickFilter(TextFilter):
    def prepare(self, bad_words: list[str]) -> None:
        self.automaton = ahocorasick.Automaton()
        for word in map(lambda x: x.lower(), bad_words):
            self.automaton.add_word(word, word)
        self.automaton.make_automaton()

    def is_unhealthy(self, field: str) -> bool:
        return len(list(self.automaton.iter(field.lower()))) != 0


class RegexFilter(TextFilter):

    def prepare(self, bad_words: list[str]) -> None:
        self.pattern = re.compile(
            "|".join(map(re.escape, bad_words)), flags=re.IGNORECASE
        )

    def is_unhealthy(self, field: str) -> bool:
        return self.pattern.search(field) is not None
