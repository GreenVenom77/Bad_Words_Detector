import multiprocessing.process
import multiprocessing.queues
from queue import Queue
import threading
import time
import logging
import pandas as pd
from filter import TextFilter
from time_helper import ChunkInfo, elapsed

logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


class DummyLock:
    def acquire(self):
        pass

    def release(self):
        pass


class Consumer:
    def __init__(
        self,
        text_filter: TextFilter,
        input_queue: Queue[tuple[int, pd.DataFrame]],
        statistics_dict,
        use_statistics_dict_lock: bool,
    ):
        self.text_filter = text_filter
        self.input_queue = input_queue
        self.statistics_dict = statistics_dict
        if use_statistics_dict_lock:
            self.statistics_dict_lock = multiprocessing.Lock()
        else:
            self.statistics_dict_lock = DummyLock()

    def start_filtering(self):
        self.text_filter.prepare()
        while True:
            if self.input_queue.empty():
                continue
            if (tuple := self.input_queue.get()) is None:
                break
            (index, chunk) = tuple
            # filtering
            start_time = time.time()
            healthy_chunk, unhealthy_chunk = self.text_filter.filter(chunk)
            end_time = time.time()

            # add the time taken to filter the chunk
            self.statistics_dict_lock.acquire()
            chunks_info: list[float | ChunkInfo] = self.statistics_dict["chunks_info"]
            chunks_info[index] = ChunkInfo(
                reading_time=self.statistics_dict["chunk_info"][index],
                filtering_time=round(end_time - start_time, 4),
                number_of_healthy=healthy_chunk.shape[0],
                number_of_unhealthy=unhealthy_chunk.shape[0],
            )
            self.statistics_dict_lock.release()

            elapsed_time = elapsed(self.statistics_dict["start_time"])
            logging.info(
                "%s  Consumer: finish filtering chunk number %s",
                elapsed_time,
                index + 1,
            )

    def run(self):
        elapsed_time = elapsed(self.statistics_dict["start_time"])
        logging.info(
            "%s  consumer:start filtering chunks using %s.",
            elapsed_time,
            self.text_filter,
        )
        self.start_filtering()
        elapsed_time = elapsed(self.statistics_dict["start_time"])
        logging.info(
            "%s  Consumer:finish of filtering chunks using %s.",
            elapsed_time,
            self.text_filter,
        )
