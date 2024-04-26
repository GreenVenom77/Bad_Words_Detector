import sys
import pytest
import os

os.chdir('..')

from filter import AhoCorasickFilter


def test_AhoCorasickFilter(test_data, args):
    args = test_data['args']
    AhoCorasickFilter(args.bad_words_file)
    # Clean up the generated files after all tests are done
