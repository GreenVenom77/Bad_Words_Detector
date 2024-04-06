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
from time_helper import generate_time_dict

logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


def main(args: Args):
    producer, consumer = setup_producer_consumer(args)
    concurrent_model = setup_concurrent_model(args.processing_mode)
    try:
        concurrent_model.start(producer, consumer)
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
    # aho
    if args.filter_mode == FilterMode.AhoCorasick:
        text_filter: TextFilter = AhoCorasickFilter(bad_words)
    else:
        text_filter: TextFilter = RegexFilter(bad_words)

    if args.processing_mode == ProcessingMode.MultiThreading:
        time_dict = generate_time_dict()
        input_queue = Queue(maxsize=1000)
    else:
        manager = multiprocessing.Manager()
        time_dict = manager.dict(generate_time_dict())
        input_queue = multiprocessing.Queue(maxsize=1000)

    if args.processing_mode == ProcessingMode.ProcessesPool:
        use_time_dict_lock = True
    else:
        use_time_dict_lock = False
    time_dict["chunk_size"] = args.chunk_size
    time_dict["start_time"] = time()

    producer = Producer(
        args.data_file, args.specify_columns, args.chunk_size, input_queue, time_dict
    )
    consumer = Consumer(text_filter, input_queue, time_dict, use_time_dict_lock)

    return producer, consumer


if __name__ == "__main__":
    args = parse_args()
    main(args)
