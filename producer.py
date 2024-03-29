import pandas as pd
import time
import re
import rarfile
import logging
#i/o bounds
logging.basicConfig(filename='logfile.log', format=' %(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)

class ProducerThread():
    def __init__(self, filename, chunksize, input_queue,timedict,badwords_filename ,badWordsQueue,specify_cols,start_time):
        
        logging.info("Initializing RarFile object...")
        self.filename = rarfile.RarFile(filename)
        logging.info("RarFile object initialized successfully.")
        self.chunksize = chunksize
        self.input_queue = input_queue
        self.timedict=timedict
        self.badWordsQueue=badWordsQueue
        self.badWordsQueue.put(badwords_filename)
        self.specify_cols=specify_cols
        self.start_time=start_time
        self.num_chunks =0
        #assign the chunksize value to timedict 
        self.timedict['chunksize']=self.chunksize
    
        
    def elapsed_time(self,starting_time):
        curr_time=time.time()
        elapsed_time = curr_time - starting_time
        # Format the elapsed time as hh:mm:ss
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        elapsed_time_str = f"{hours:02.0f}:{minutes:02.0f}:{seconds:05.2f}"

        # Log a message with the elapsed time and timestamp
        return elapsed_time_str
    
    
    
        #using yiled

    
    def read_csv_chunks(self,filename, chunksize):
        chunks = pd.read_csv(filename.open(filename.namelist()[0]), usecols=self.specify_cols , chunksize=chunksize, iterator=True)
        
        start_time = time.time()
        for chunk in chunks:
            end_time=time.time()
            yield chunk
            self.timedict['reading'].append(end_time-start_time)
           
            start_time = time.time()

    

    
    def run(self):
        elapsed_time_str = self.elapsed_time(self.start_time)
        logging.info("%s  Producer: start reading chunks.",elapsed_time_str)
        
        
        count=1
        # Process chunks of data until there are no more chunks
        for chunk in self.read_csv_chunks(self.filename,self.chunksize):
            self.input_queue.put(chunk)
            self.num_chunks += 1
            
            elapsed_time=self.elapsed_time(self.start_time)
            logging.info("%s  Producer:read %s chunks and send it into queue.",elapsed_time,count)
            
            count+=1

        #send number of chunks in dict that has time statistics as well
        self.timedict['number of chunks']=self.num_chunks
        
        # Signal end of input and compute read time statistics
        self.input_queue.put(None)
        elapsed_time_str = self.elapsed_time(self.start_time)
        logging.info("%s  Producer: finish reading all chunks.",elapsed_time_str)














