import csv
import os
import random
import sys

import pytest
import rarfile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import modules from the root of the repository
from concurrent_model import MultiThreadingModel, MultiProcessingModel, ProcessesPoolModel
from main import setup_concurrent_model, setup_producer_consumer
from Enums import ProcessingMode, FilterMode
from arguments import Args



@pytest.fixture(scope="session")
def generate_test_csv():
    num_rows = 1000000  # Adjust the number of rows as needed
    output_csv_path = "./Outputs/test_data.csv"  # Specify the output CSV path
    output_rar_path = "./Outputs/test_data.rar"  # Specify the output RAR path

    # Check if the output directory exists, create it if not
    output_dir = os.path.dirname(output_csv_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Function to generate a row with or without bad words
    def generate_row(bad_words):
        col1 = random.choice(["value", random.choice(bad_words)])
        col2 = random.choice(["value", random.choice(bad_words)])
        col3 = random.choice(["value", random.choice(bad_words)])
        return [col1, col2, col3]

    # Read bad words from the file
    with open("./BadWords.csv", 'r') as f:
        bad_words = [line.strip() for line in f]

    # Generate CSV file
    with open(output_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['col1', 'col2', 'col3'])  # Header
        for _ in range(num_rows):
            writer.writerow(generate_row(bad_words))

    # Compress CSV to RAR
    with rarfile.RarFile(output_rar_path, 'w') as rf:
        rf.write(output_csv_path, arcname=os.path.basename(output_csv_path))

    # Return the path to the generated RAR file
    yield output_rar_path

    # Clean up the generated files
    os.remove(output_csv_path)
    os.remove(output_rar_path)


@pytest.fixture
def args():
    args = Args(
        data_file="./Outputs/test_data.rar",
        bad_words_file="./BadWords.csv",
        columns=[0, 1, 2],
        filter_mode=FilterMode.AhoCorasick,
        processing_mode=ProcessingMode.MultiThreading,
        chunk_size=1000,
        rounding_place=2
    )
    return args


def test_setup_concurrent_model():
    assert isinstance(setup_concurrent_model(ProcessingMode.MultiThreading), MultiThreadingModel)
    assert isinstance(setup_concurrent_model(ProcessingMode.MultiProcessing), MultiProcessingModel)
    assert isinstance(setup_concurrent_model(ProcessingMode.ProcessesPool), ProcessesPoolModel)


def test_setup_producer_consumer(args):
    producer, consumer = setup_producer_consumer(args)
    assert producer is not None
    assert consumer is not None