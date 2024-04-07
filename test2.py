from dataclasses import dataclass
from time import time
from typing import List
from enum import Enum


@dataclass
class Args:
    data_file: str
    bad_words_file: str
    head_columns: List[int]
    specify_columns: List[int]

    chunk_size: int
    additional_field: float = time()  # Define the additional field with a default value


# Example usage:
args = Args(
    data_file="data.csv",
    bad_words_file="bad_words.txt",
    head_columns=[1, 2, 3],
    specify_columns=[4, 5, 6],
    chunk_size=100,
)
print(args)
