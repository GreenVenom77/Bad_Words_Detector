import os
import sys
import pytest

os.chdir('..')

from main import setup_producer_consumer


def test_setup_producer_consumer(test_data, args):
    args = test_data['args']
    producer, consumer = setup_producer_consumer(args)
    assert producer is not None
    assert consumer is not None

