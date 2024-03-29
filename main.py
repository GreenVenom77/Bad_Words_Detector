from producer import ProducerThread
from consumer import ConsumerThread
from queue import Queue
import argparse
import threading
import multiprocessing
import json
import os
import logging
import time




logging.basicConfig(filename='logfile.log', format=' %(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)

def main(args):
    # Set up the producer-consumer model
    
        
    dataFile=args.dataFile
    badWordsFileName=args.badWords_file
    rows_at_chunk=args.chunk_size
    mode=args.mode
    head=args.head
    specify_cols=args.specify_cols
    filter_mode=args.filter_mode

    start_time=time.time()
    
    if mode=="multiprocessing":
        logging.info("Main: run in process")
        input_queue =multiprocessing.Queue()
        success_queue = multiprocessing.Queue()
         
        fail_queue = multiprocessing.Queue()
        badWordsQueue=multiprocessing.Queue()
        #Create a shared time dictionary
        manager = multiprocessing.Manager()
        timedict = manager.dict({
            'chunksize': int(),
            'number of chunks': int(),
            'reading': [],
            'filtering': [],
            'writing': []
        })
        lock=multiprocessing.Lock()

    
        producer = ProducerThread(dataFile,rows_at_chunk, input_queue,timedict,badWordsFileName,badWordsQueue,specify_cols,start_time)
        consumer = ConsumerThread(input_queue,success_queue,fail_queue,badWordsQueue,timedict,head,filter_mode,start_time,lock)
       
        producer_process=multiprocessing.Process(target=producer.run)
        consumer_process=multiprocessing.Process(target=consumer.run)
      
        try:
            producer_process.start()
            consumer_process.start()

            producer_process.join()
            consumer_process.join()

        except Exception as e:
            logging.error("Error occurred while running program: {}".format(str(e)))
        
        

    elif mode=="threading":
        logging.info("Main: run in threads")
        input_queue = Queue(maxsize=10000)
        success_queue = Queue()
        fail_queue = Queue()
        badWordsQueue=Queue()
        timedict={
            'chunksize':0, 
            'number of chunks':0,
            'reading':[],
            'filtering':[],
            'writing':[]
            }
        
        lock=threading.Lock()
        producer = ProducerThread(dataFile,rows_at_chunk, input_queue,timedict,badWordsFileName,badWordsQueue,specify_cols,start_time)
        consumer = ConsumerThread(input_queue,success_queue,fail_queue,badWordsQueue,timedict,head,filter_mode,start_time,lock)
        
        producer_thread=threading.Thread(target=producer.run)
        consumer_thread=threading.Thread(target=consumer.run)

        producer_thread.start()
        consumer_thread.start()
        
        producer_thread.join()
        consumer_thread.join()

        



    elif mode=="processes_pool":
        logging.info("Main: run in pool of processes")
        input_queue = multiprocessing.Queue()
        success_queue = multiprocessing.Queue()
        fail_queue = multiprocessing.Queue()
        badWordsQueue=multiprocessing.Queue()
        lock=multiprocessing.Lock()
        manager=multiprocessing.Manager()
        timedict = manager.dict({
            'chunksize': 0,
            'number of chunks': 0,
            'reading': [],
            'filtering': [],
            'writing': []
        })
        producer = ProducerThread(dataFile,rows_at_chunk, input_queue,timedict,badWordsFileName,badWordsQueue,specify_cols,start_time)
        consumer = ConsumerThread(input_queue,success_queue,fail_queue,badWordsQueue,timedict,head,filter_mode,start_time,lock)

        # Create a pool with 3 processes
        pool = multiprocessing.Pool(processes=4)

        # Start the producer process asynchronously
        producer_process = pool.apply_async(producer.run)

        # Start the 4 consumer processes asynchronously
        consumer_process1=pool.apply_async(consumer.run) 
        consumer_process2=pool.apply_async(consumer.run) 
        consumer_process3=pool.apply_async(consumer.run) 

        # Close the pool and wait for all processes to finish
        pool.close()
        pool.join()







if __name__=="__main__":
    parser = argparse.ArgumentParser(description='filter specify cols from very big csv RARED file against bad words file using RE')
    
    # Check if args.json exists and load arguments from it if it does
    if os.path.exists('args.json'):
        with open('args.json', 'r') as f:
            args_dict = json.load(f)
            args = argparse.Namespace(**args_dict)
    else:
        parser.add_argument('-d', '--data_file', type=str, help='The csv file that we will filter it')
        parser.add_argument('-b', '--bad_words_file', type=str,  help='The name of bad words file name')
        parser.add_argument('-c', '--chunk-size', type=int, default='1000', help='The chunk size will be processed (default: "1000")')
        parser.add_argument('-m', '--mode', type=str, default="multiprocessing", help='the concurrency model that will work')
        parser.add_argument('-s', '--specify_cols', type=int, default=[0,2,4], help='specified cols that will be filtered')
        parser.add_argument('-h', '--head_cols', type=int, default=[0,1,2], help='specified index cols that will be chosen to filter')
        args = parser.parse_args()

    while(args.chunk_size<=150000):
        main(args)
        args.chunk_size+=10000
        