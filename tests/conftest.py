import csv
import os
import random
import sys
import pytest
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from Enums import ProcessingMode, FilterMode
from arguments import Args


@pytest.fixture(scope="session")
def generate_test_data():
    num_rows = 1300000  # Adjust the number of rows as needed
    output_csv_path = "./TestOutputs/test_data.csv"  # Specify the output CSV path
    output_rar_path = "./TestOutputs/test_data.rar"  # Specify the output RAR path

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
    os.system(f'rar a {output_rar_path} {output_csv_path}')

    # Return the path to the generated RAR file and the total number of rows
    yield output_rar_path, num_rows


@pytest.fixture()
def badwords_list():
    badwords_list: list[str] = (
        pd.read_csv("./BadWords.csv", header=None).iloc[:, 0].tolist()
    )
    return badwords_list


@pytest.fixture
def args():
    args = Args(
        data_file="./TestOutputs/test_data.rar",
        bad_words_file="./BadWords.csv",
        columns=[0, 1, 2],
        filter_mode=FilterMode.AhoCorasick,
        processing_mode=ProcessingMode.ProcessesPool,
        chunk_size=10000,
        rounding_place=2
    )
    return args


@pytest.fixture
def test_data(args, generate_test_data, badwords_list):
    return {'args': args, 'generate_test_csv': generate_test_data, 'badwords_list': badwords_list}
