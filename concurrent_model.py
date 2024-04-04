from abc import ABC, abstractmethod
import multiprocessing

from consumer_copy import Consumer
from threading import Thread
from producer_copy import Producer


# interface (abstract class)
class ConcurrentModel(ABC):

    @abstractmethod
    def start(self, producer: Producer, consumer: Consumer) -> None:
        """start the concurrent work"""
        pass


class ProcessesPoolModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> None:
        # Create a pool with 3 processes
        pool = multiprocessing.Pool(processes=4)
        # Start the producer process asynchronously
        pool.apply_async(producer.run)
        # Start the 4 consumer processes asynchronously
        pool.apply_async(consumer.run)
        pool.apply_async(consumer.run)
        pool.apply_async(consumer.run)
        # Close the pool and wait for all processes to finish
        pool.close()
        pool.join()


class MultiProcessingModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> None:
        producer_process = multiprocessing.Process(target=producer.run)
        consumer_process = multiprocessing.Process(target=consumer.run)
        producer_process.start()
        consumer_process.start()
        producer_process.join()
        consumer_process.join()


class MultiThreadingModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> None:
        producer_thread = Thread(target=producer.run)
        consumer_thread = Thread(target=consumer.run)
        producer_thread.start()
        consumer_thread.start()
        producer_thread.join()
        consumer_thread.join()
