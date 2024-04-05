from queue import Queue
from typing import Any, Dict, Iterator
import pandas as pd
import time
from rarfile import RarFile
import logging
from time_helper import elapsed

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
        input_queue: Queue[pd.DataFrame],
        time_dict: dict[str, Any],
    ):
        """
        A class to read chunks of data from a RAR file and put them into a queue for further processing.

        Attributes:
            filename (str): The path to the RAR file.
            columns (List[int]): List of column indices to be read from the CSV.
            chunk_size (int): Size of each chunk to be read.
            input_queue (multiprocessing.Queue | Queue): The queue to put the chunks of data into.
            time_dict (dict[str, Any] | DictProxy[str, Any]): A dictionary for time statistics.
        """

        self.__file_name = filename
        self.__chunk_size = chunk_size
        self.__input_queue = input_queue
        self.__time_dict = time_dict
        self.__columns = columns

    def __read_chunks(self) -> Iterator[pd.DataFrame]:
        """
        A generator function to read chunks of data from the RAR file.

        Yields:
            pd.DataFrame: A DataFrame containing a chunk of data.
        """
        logging.info("Initializing RarFile object...")
        with RarFile(self.__file_name) as rar_ref:
            logging.info("RarFile object initialized successfully.")
            with rar_ref.open(rar_ref.namelist()[0]) as file:
                # (chunk size) parameter represents rows count for each chunk
                chunks = pd.read_csv(
                    file,
                    usecols=self.__columns,
                    chunksize=self.__chunk_size,
                    iterator=True,
                )
                start_time = time.time()
                for chunk in chunks:
                    end_time = time.time()
                    yield chunk
                    self.__time_dict["reading"].append(end_time - start_time)
                    start_time = time.time()

    def run(self):
        """
        Run the producer to read chunks of data from the RAR file and put them into the input queue.
        """
        elapsed_time: str = elapsed(self.__time_dict["start_time"])
        logging.info("%s  Producer: start reading chunks.", elapsed_time)
        # Process chunks of data until there are no more chunks
        for number, chunk in enumerate(self.__read_chunks()):
            self.__input_queue.put(chunk)
            elapsed_time = elapsed(self.__time_dict["start_time"])
            logging.info(
                "%s  Producer:read %s chunks and send it into input queue.",
                elapsed_time,
                number + 1,
            )
        else:
            # add number of founded chunks to statistics dict
            self.__time_dict["number of chunks"] = number + 1
        # Signal end of input and compute read time statistics
        self.__input_queue.put(None)
        elapsed_time: str = elapsed(self.__time_dict["start_time"])
        logging.info("%s  Producer: finish reading all chunks.", elapsed_time)
