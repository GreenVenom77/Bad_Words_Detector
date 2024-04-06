from queue import Queue
from typing import Any, Dict, Iterator
import pandas as pd
import time
from rarfile import RarFile
import logging
from time_helper import ChunkInfo, elapsed

# i/o bounds
logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


class Producer:
    def __init__(
        self,
        filename: str,
        columns: list[int],
        chunk_size: int,
        input_queue: Queue[tuple[int, pd.DataFrame]],
        statistics_dict: dict[str, Any],
    ):
        """
        A class to read chunks of data from a RAR file and put them into a queue for further processing.

        Attributes:
            filename (str): The path to the RAR file.
            columns (List[int]): List of column indices to be read from the CSV.
            chunk_size (int): Size of each chunk to be read.
            input_queue (multiprocessing.Queue | Queue): The queue to put the chunks of data into.
            statistics_dict (dict[str, Any] | DictProxy[str, Any]): A dictionary for time statistics.
        """

        self.file_name = filename
        self.chunk_size = chunk_size
        self.input_queue = input_queue
        self.statistics_dict = statistics_dict
        self.columns = columns

    def read_chunks(self) -> Iterator[pd.DataFrame]:
        """
        A generator function to read chunks of data from the RAR file.

        Yields:
            pd.DataFrame: A DataFrame containing a chunk of data.
        """
        logging.info("Initializing RarFile object...")
        with RarFile(self.file_name) as rar_ref:
            logging.info("RarFile object initialized successfully.")
            with rar_ref.open(rar_ref.namelist()[0]) as file:
                # (chunk size) parameter represents rows count for each chunk
                chunks = pd.read_csv(
                    file,
                    usecols=self.columns,
                    chunksize=self.chunk_size,
                    iterator=True,
                )
                start_time = time.time()
                for chunk in chunks:
                    end_time = time.time()
                    yield chunk
                    chunks_info: list[float | ChunkInfo] = self.statistics_dict[
                        "chunks_info"
                    ]
                    chunks_info.append(round(end_time - start_time, 4))
                    start_time = time.time()

    def run(self):
        """
        Run the producer to read chunks of data from the RAR file and put them into the input queue.
        """
        elapsed_time: str = elapsed(self.statistics_dict["start_time"])
        logging.info("%s  Producer: start reading chunks.", elapsed_time)
        # Process chunks of data until there are no more chunks
        for number, chunk in enumerate(self.read_chunks()):
            self.input_queue.put((number, chunk))
            elapsed_time = elapsed(self.statistics_dict["start_time"])
            logging.info(
                "%s  Producer:read %s chunks and send it into input queue.",
                elapsed_time,
                number + 1,
            )

        else:
            # add number of founded chunks to statistics dict
            self.statistics_dict["number of chunks"] = number + 1
        # Signal end of input and compute read time statistics
        self.input_queue.put(None)
        elapsed_time: str = elapsed(self.statistics_dict["start_time"])
        logging.info("%s  Producer: finish reading all chunks.", elapsed_time)
