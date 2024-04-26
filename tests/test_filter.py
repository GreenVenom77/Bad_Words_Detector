import os
import pandas as pd

os.chdir('..')
from filter import AhoCorasickFilter, RegexFilter


def test_filters(test_data, badwords_list):
    bad_words = test_data['badwords_list']
    AhoCorasickFilter(bad_words)
    RegexFilter(bad_words)
    # Clean up the generated files after all tests are done
