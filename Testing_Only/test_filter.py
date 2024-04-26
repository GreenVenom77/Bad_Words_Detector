import pytest
import os

from filter import AhoCorasickFilter


def test_AhoCorasickFilter(test_data):
    args = test_data['args']
    AhoCorasickFilter(args.bad_words_file)
    # Clean up the generated files after all tests are done
    os.remove(args.output_csv_path)
    os.remove(args.output_rar_path)