from typing import Callable
import pandas as pd
from Enums import FilterMode, ProcessingMode
from concurrent_model import *
from producer import Producer
from consumer import Consumer
from queue import Queue
import threading
import multiprocessing
import logging
from time import time
from arguments import Args, parse_args
from filter import *
from statistics_writer import StatisticsWriter
from chunks_processing_info import ChunkFilteringInfo


logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


def main(args: Args):
    producer, consumer = setup_producer_consumer(args)
    writer = StatisticsWriter(args)
    concurrent_model = setup_concurrent_model(args.processing_mode)
    try:
        chunks_info = concurrent_model.start(producer, consumer)
        writer.start(chunks_info)
    except Exception as e:
        logging.exception("Exception occurred while running program: {}".format(str(e)))


def setup_concurrent_model(processing_mode: ProcessingMode) -> ConcurrentModel:
    match processing_mode:
        case ProcessingMode.MultiThreading:
            logging.info("Main: run in threads")
            return MultiThreadingModel()
        case ProcessingMode.MultiProcessing:
            logging.info("Main: run in processes")
            return MultiProcessingModel()
        case ProcessingMode.ProcessesPool:
            logging.info("Main: run in pool of processes")
            return ProcessesPoolModel()


def setup_producer_consumer(args: Args) -> tuple[Producer, Consumer]:
    bad_words: list[str] = (
        pd.read_csv(args.bad_words_file, header=None).iloc[:, 0].tolist()
    )
    manager = multiprocessing.Manager()
    if args.filter_mode == FilterMode.AhoCorasick:
        text_filter: TextFilter = AhoCorasickFilter(bad_words)
    else:
        text_filter: TextFilter = RegexFilter(bad_words)
    if args.processing_mode == ProcessingMode.MultiThreading:
        input_queue = Queue[tuple[int, DataFrame]](maxsize=1000)
        reading_info_queue = Queue[float]()
        filtering_info_queue = Queue[tuple[int, ChunkFilteringInfo]]()
    else:
        input_queue = multiprocessing.Queue(maxsize=1000)
        reading_info_queue = manager.Queue()
        filtering_info_queue = manager.Queue()
    producer = Producer(input_queue, reading_info_queue, args)
    consumer = Consumer(input_queue, filtering_info_queue, text_filter, args)

    return producer, consumer


if __name__ == "__main__":
    args = parse_args()
    main(args)
