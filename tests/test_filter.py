import os
import pandas as pd

os.chdir("..")
from filter import AhoCorasickFilter, RegexFilter


def test_filters(test_data, badwords_list):
    bad_words = test_data["badwords_list"]

    aho_filter = AhoCorasickFilter()
    regex_filter = RegexFilter()

    aho_filter.prepare(bad_words)
    regex_filter.prepare(bad_words)
