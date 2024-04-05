from abc import ABC, abstractmethod
import multiprocessing
import multiprocessing.process

from consumer import Consumer
from threading import Thread
from producer import Producer


# interface (abstract class)
class ConcurrentModel(ABC):

    @abstractmethod
    def start(self, producer: Producer, consumer: Consumer) -> None:
        """start the concurrent work"""
        pass


class ProcessesPoolModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> None:
        producer_process = multiprocessing.Process(target=producer.run)
        consumer_process1 = multiprocessing.Process(target=consumer.run)
        consumer_process2 = multiprocessing.Process(target=consumer.run)
        consumer_process3 = multiprocessing.Process(target=consumer.run)
        producer_process.start()
        consumer_process1.start()
        consumer_process2.start()
        consumer_process3.start()
        producer_process.join()
        consumer_process1.join()
        consumer_process2.join()
        consumer_process3.join()


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
