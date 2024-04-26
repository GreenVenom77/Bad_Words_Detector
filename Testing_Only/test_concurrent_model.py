import pytest

from concurrent_model import MultiThreadingModel, MultiProcessingModel, ProcessesPoolModel
from main import setup_concurrent_model
from Enums import ProcessingMode


def test_setup_concurrent_model():
    assert isinstance(setup_concurrent_model(ProcessingMode.MultiThreading), MultiThreadingModel)
    assert isinstance(setup_concurrent_model(ProcessingMode.MultiProcessing), MultiProcessingModel)
    assert isinstance(setup_concurrent_model(ProcessingMode.ProcessesPool), ProcessesPoolModel)