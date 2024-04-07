from typing import Callable
import pandas as pd
from Enums import FilterMode, ProcessingMode
from concurrent_model import *
from merge_info import merge_chunks_info, merge_chunks_info_multiple_filters2
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
from time_helper import ChunkFilteringInfo


logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


def main(args: Args):
    producer, consumer, merger = setup_producer_consumer(args)
    writer = StatisticsWriter(args)
    concurrent_model = setup_concurrent_model(args.processing_mode)
    try:
        concurrent_model.start(producer, consumer)
        print("start merging")
        writer.start(merger(producer.reading_info_queue, consumer.filtering_info_queue))
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


def setup_producer_consumer(args: Args) -> tuple[Producer, Consumer, Callable]:
    bad_words: list[str] = (
        pd.read_csv(args.bad_words_file, header=None).iloc[:, 0].tolist()
    )
    if args.filter_mode == FilterMode.AhoCorasick:
        text_filter: TextFilter = AhoCorasickFilter(bad_words)
    else:
        text_filter: TextFilter = RegexFilter(bad_words)
    if args.processing_mode == ProcessingMode.MultiThreading:
        input_queue = Queue[tuple[int, DataFrame]](maxsize=1000)
        reading_info_queue = Queue[float](maxsize=1000)
        filtering_info_queue = Queue[tuple[int, ChunkFilteringInfo]](maxsize=1000)
    else:
        input_queue = multiprocessing.Queue(maxsize=1000)
        reading_info_queue = multiprocessing.Queue(maxsize=1000)
        filtering_info_queue = multiprocessing.Queue(maxsize=1000)
    if args.processing_mode == ProcessingMode.ProcessesPool:
        merger = merge_chunks_info_multiple_filters2
    else:
        merger = merge_chunks_info
    producer = Producer(input_queue, reading_info_queue, args)
    consumer = Consumer(input_queue, filtering_info_queue, text_filter, args)

    return producer, consumer, merger


if __name__ == "__main__":
    args = parse_args()
    main(args)
