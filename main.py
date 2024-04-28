import pandas as pd
from Enums import FilterMode, ProcessingMode
from concurrent_model import *
from producer import Producer
from consumer import Consumer
import logging
from arguments import Args, parse_args
from filter import *
from statistics_writer import StatisticsWriter


logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    force=True,
)


def main(args: Args):
    text_filter = setup_text_filter(args.filter_mode, args.bad_words_file)
    concurrent_model = setup_concurrent_model(args.processing_mode)
    producer, consumer = Producer(args), Consumer(text_filter, args)
    writer = StatisticsWriter(args)
    try:
        chunks_info = concurrent_model.start(producer, consumer)
        writer.start(chunks_info)
    except Exception as e:
        logging.exception("Exception occurred while running program: {}".format(str(e)))


def setup_text_filter(filter_mode: FilterMode, bad_words_path: str) -> TextFilter:
    bad_words: list[str] = pd.read_csv(bad_words_path, header=None).iloc[:, 0].tolist()
    if filter_mode == FilterMode.AhoCorasick:
        filter = AhoCorasickFilter()
    else:
        filter = RegexFilter()
    filter.prepare(bad_words)
    return filter


def setup_concurrent_model(processing_mode: ProcessingMode) -> ConcurrentModel:
    if processing_mode == ProcessingMode.MultiThreading:
        return MultiThreadingModel()
    elif processing_mode == ProcessingMode.MultiProcessing:
        return MultiProcessingModel()
    else:
        return ProcessesPoolModel()


if __name__ == "__main__":

    args = parse_args()
    # region for normal usage
    main(args)
    # endregion

    # region for benchmark usage
    #while args.chunk_size <= 150_000:
    #     main(args)
    #     args.chunk_size += 10_000
    #     args.starting_time = time()
    # endregion
