import pytest

from concurrent_model import MultiThreadingModel, MultiProcessingModel, ProcessesPoolModel
from main import setup_concurrent_model, setup_producer_consumer
from Enums import ProcessingMode, FilterMode
from arguments import Args


@pytest.fixture
def args():
    args = Args()
    args.processing_mode = ProcessingMode.MultiThreading
    args.filter_mode = FilterMode.AhoCorasick
    args.bad_words_file = "bad_words.txt"  # Provide the path to your test file
    return args


def test_setup_concurrent_model():
    assert isinstance(setup_concurrent_model(ProcessingMode.MultiThreading), MultiThreadingModel)
    assert isinstance(setup_concurrent_model(ProcessingMode.MultiProcessing), MultiProcessingModel)
    assert isinstance(setup_concurrent_model(ProcessingMode.ProcessesPool), ProcessesPoolModel)


def test_setup_producer_consumer(args):
    producer, consumer = setup_producer_consumer(args)
    assert producer is not None
    assert consumer is not None