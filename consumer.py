import multiprocessing.process
import multiprocessing.queues
from queue import Queue
import threading
from time import time
import logging
from typing import Any
import pandas as pd
from arguments import Args
from filter import TextFilter
from time_helper import ChunkFilteringInfo, ChunkInfo, elapsed

logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


class Consumer:
    def __init__(
        self,
        input_queue: Queue[tuple[int, pd.DataFrame]] | Any,
        filtering_info_queue: Queue[tuple[int, ChunkFilteringInfo]] | Any,
        text_filter: TextFilter,
        args: Args,
    ):
        self.text_filter = text_filter
        self.input_queue = input_queue
        self.filtering_info_queue = filtering_info_queue
        self.args = args

    def start_filtering(self):
        self.text_filter.prepare()
        while True:
            if self.input_queue.empty():
                continue
            if (tuple := self.input_queue.get()) is None:
                self.input_queue.put(None)  # type: ignore # to handle multiple consumers case
                break
            (index, chunk) = tuple
            # filtering
            start_time = time()
            healthy_chunk, unhealthy_chunk = self.text_filter.filter(chunk)
            end_time = time()

            # add the time taken to filter the chunk
            self.filtering_info_queue.put(
                (
                    index,
                    ChunkFilteringInfo(
                        filtering_time=round(end_time - start_time, 4),
                        number_of_healthy=healthy_chunk.shape[0],
                        number_of_unhealthy=unhealthy_chunk.shape[0],
                    ),
                )
            )
            logging.info(
                "%s  Consumer: finish filtering chunk number %s",
                elapsed(self.args.starting_time),
                index + 1,
            )

    def run(self):
        logging.info(
            "%s  consumer:start filtering chunks using %s.",
            elapsed(self.args.starting_time),
            self.text_filter,
        )
        self.start_filtering()
        logging.info(
            "%s  Consumer:finish of filtering chunks using %s.",
            elapsed(self.args.starting_time),
            self.text_filter,
        )
