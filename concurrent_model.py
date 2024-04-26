from abc import ABC, abstractmethod
import multiprocessing
import multiprocessing.process
from queue import Queue

from pandas import DataFrame
from consumer import Consumer
from threading import Thread
from producer import Producer
from chunks_processing_info import ChunkFilteringInfo, ChunkInfo, merge_chunks_info


# interface (abstract class)
class ConcurrentModel(ABC):

    @abstractmethod
    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        """start the concurrent work"""
        pass


class ProcessesPoolModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        with multiprocessing.Manager() as manager:
            chunks_queue = manager.Queue(maxsize=1000)
            reading_info_queue = manager.Queue()
            filtering_info_queue = manager.Queue()
            pool = multiprocessing.Pool(4)
            pool.apply_async(
                producer.run,
                args=(
                    chunks_queue,
                    reading_info_queue,
                ),
            )
            for _ in range(3):
                pool.apply_async(
                    consumer.run,
                    args=(
                        chunks_queue,
                        filtering_info_queue,
                    ),
                )

            pool.close()
            pool.join()

            return merge_chunks_info(reading_info_queue, filtering_info_queue)


class MultiProcessingModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        with multiprocessing.Manager() as manager:
            chunks_queue = manager.Queue(maxsize=1000)
            reading_info_queue = manager.Queue()
            filtering_info_queue = manager.Queue()
            producer_process = multiprocessing.Process(
                target=producer.run,
                args=(
                    chunks_queue,
                    reading_info_queue,
                ),
            )
            consumer_process = multiprocessing.Process(
                target=consumer.run,
                args=(
                    chunks_queue,
                    filtering_info_queue,
                ),
            )
            producer_process.start()
            consumer_process.start()
            producer_process.join()
            consumer_process.join()
            return merge_chunks_info(reading_info_queue, filtering_info_queue)


class MultiThreadingModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        chunks_queue = Queue[tuple[int, DataFrame]](maxsize=1000)
        reading_info_queue = Queue[float]()
        filtering_info_queue = Queue[tuple[int, ChunkFilteringInfo]]()

        producer_thread = Thread(
            target=producer.run,
            args=(
                chunks_queue,
                reading_info_queue,
            ),
        )
        consumer_thread = Thread(
            target=consumer.run,
            args=(
                chunks_queue,
                filtering_info_queue,
            ),
        )
        producer_thread.start()
        consumer_thread.start()
        producer_thread.join()
        consumer_thread.join()
        return merge_chunks_info(reading_info_queue, filtering_info_queue)
