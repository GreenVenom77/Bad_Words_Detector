from queue import Queue
import logging
from queue import Queue
from time import time
from typing import Any, Iterator

from pandas import DataFrame, read_csv
from rarfile import RarFile

from arguments import Args
from chunks_processing_info import elapsed


class Producer:
    def __init__(
        self,
        args: Args,
    ):
        self.args = args

    def run(
        self,
        chunks_queue: Queue[tuple[int, DataFrame]] | Any,
        reading_info_queue: Queue[float] | Any,
    ):
        logging.info(
            "%s  Producer: start reading chunks.", elapsed(self.args.starting_time)
        )
        # Process chunks of data until there are no more chunks
        logging.info("Initializing RarFile object...")
        with RarFile(self.args.data_file) as rar_ref:
            logging.info("RarFile object initialized successfully.")
            with rar_ref.open(rar_ref.namelist()[0]) as file:
                chunks = read_csv(
                    file,
                    usecols=self.args.columns,
                    chunksize=self.args.chunk_size,
                    iterator=True,
                )
                start_time = time()
                for index, chunk in enumerate(chunks):
                    end_time = time()

                    reading_info_queue.put(
                        round(end_time - start_time, self.args.rounding_place)
                    )
                    chunks_queue.put((index, chunk))
                    logging.info(
                        "%s  Producer:read %s chunks and send it into input queue.",
                        elapsed(self.args.starting_time),
                        index + 1,
                    )
                    start_time = time()
        # Signal end of input
        chunks_queue.put(None)  # type: ignore
        logging.info(
            "%s  Producer: finish reading all chunks.", elapsed(self.args.starting_time)
        )
