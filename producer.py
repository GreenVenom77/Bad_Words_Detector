import multiprocessing.process
import multiprocessing.queues
from queue import Queue
from typing import Any, Dict, Iterator
from pandas import DataFrame, read_csv
from time import time
from rarfile import RarFile
import logging
from arguments import Args
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
        input_queue: Queue[tuple[int, DataFrame]] | Any,
        reading_info_queue: Queue[float] | Any,
        args: Args,
    ):
        self.input_queue = input_queue
        self.reading_info_queue = reading_info_queue
        self.args = args

    def read_chunks(self) -> Iterator[DataFrame]:
        """
        A generator function to read chunks of data from the RAR file.

        Yields:
            pd.DataFrame: A DataFrame containing a chunk of data.
        """
        logging.info("Initializing RarFile object...")
        with RarFile(self.args.data_file) as rar_ref:
            logging.info("RarFile object initialized successfully.")
            with rar_ref.open(rar_ref.namelist()[0]) as file:
                chunks = read_csv(
                    file,
                    usecols=self.args.specify_columns,
                    chunksize=self.args.chunk_size,
                    iterator=True,
                )
                start_time = time()
                for chunk in chunks:
                    end_time = time()
                    yield chunk
                    self.reading_info_queue.put(round(end_time - start_time, 4))
                    start_time = time()

    def run(self):
        """
        Run the producer to read chunks of data from the RAR file and put them into the input queue.
        """

        logging.info(
            "%s  Producer: start reading chunks.", elapsed(self.args.starting_time)
        )
        # Process chunks of data until there are no more chunks
        for number, chunk in enumerate(self.read_chunks()):
            self.input_queue.put((number, chunk))
            logging.info(
                "%s  Producer:read %s chunks and send it into input queue.",
                elapsed(self.args.starting_time),
                number + 1,
            )

        # Signal end of input and compute read time statistics
        self.input_queue.put(None)  # type: ignore
        logging.info(
            "%s  Producer: finish reading all chunks.", elapsed(self.args.starting_time)
        )
