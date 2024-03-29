
import re
import threading
import time
import openpyxl
from functools import reduce
import os
import logging
import ahocorasick
import pandas as pd
import csv
logging.basicConfig(filename='logfile.log', format=' %(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)


#cpu bounds
class ConsumerThread():
    def __init__(self, input_queue, success_queue,fail_queue,badwordsQueue,timedict,head,filter_mode,start_time,lock):
        self.input_queue = input_queue
        self.success_queue = success_queue
        self.fail_queue=fail_queue
        self.badWordsQueue=badwordsQueue
        self.badwords_filename=self.badWordsQueue.get()
        self.patterns = pd.read_csv(self.badwords_filename).values.tolist()
        self.badWords =('|'.join(re.escape(x[0]) for x in self.patterns))
        self.timedict=timedict
        self.Head=head
        self.lock=lock
        self.filter_mode=filter_mode
        self.start_time=start_time
        self.automaton = ahocorasick.Automaton()


    def elapsed_time(self,starting_time):
        curr_time=time.time()
        elapsed_time = curr_time - starting_time
        # Format the elapsed time as hh:mm:ss
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        elapsed_time_str = f"{hours:02.0f}:{minutes:02.0f}:{seconds:05.2f}"

        # Log a message with the elapsed time and timestamp
        return elapsed_time_str
    
    

    def create_Trie_UsingPyAcho(self,patterns):
        
        for word in  patterns:
            self.automaton.add_word(word[0].lower(), word[0].lower())
        return self.automaton 

    #pythoinc for loop instead of 
    #x = BoolList[0]
    #   for i in BoolList:
    #      x = x & i
    def check_bool(self, BoolList):
       return reduce(lambda x, y: x & y, BoolList)
    





    def write_csv(self, queue, fileName):
        if not os.path.exists("output"):
            os.makedirs("output")
        
     
        #create folder store healty and unhealty files

        if not os.path.exists("output/FrameSize"+str(self.timedict['chunksize'])):
            os.makedirs("output/FrameSize"+str(self.timedict['chunksize']))

        fileName="output/FrameSize"+str(self.timedict['chunksize'])+"/"+fileName
        
        
        while True:
            if queue.empty():
                break

            record = queue.get()  
            
            # Check if the file already exists
            if not os.path.exists(fileName):
                # write the first chunk with header
                record.to_csv(fileName, mode='w', header=True, index=False) 

            else:    
                # append chunk without header
                record.to_csv(fileName, mode='a', header=False, index=False) 
                        

    def write_csv_statistics(self, filename):
        with open("output/"+filename, 'a', newline='') as csvfile:
            fieldnames = ['chunk_size','chunk_number', 'reading_time', 'filtering_time', 'writing_time']
            writer = csv.DictWriter(csvfile, fieldnames)
            writer.writeheader()

            for i in range(len(self.timedict['reading'])):
                row = {
                    'chunk_size':self.timedict['chunksize'],
                    'chunk_number': i+1,
                    'reading_time': self.timedict['reading'][i],
                    'filtering_time': self.timedict['filtering'][i],
                    'writing_time': self.timedict['writing'][i]
                }
                writer.writerow(row)



        
    def ExceLwriter(self,sheet_name):

        #measure reading total time and avg time
        total_time_of_read=sum(self.timedict['reading'])
        avg_time_of_read=total_time_of_read/self.timedict['number of chunks']

        #measure filtering total time and avg time
        total_time_of_filter=sum(self.timedict['filtering'])
        avg_time_of_filter=total_time_of_filter/self.timedict['number of chunks']
        #measure writing total time and avg time
        total_time_of_writing=sum(self.timedict['writing'])
        avg_time_of_writing=total_time_of_writing/self.timedict['number of chunks']

        #measure processing total time and avg time
        total_time_of_processing=sum(self.timedict['reading'])+sum(self.timedict['filtering'])+sum(self.timedict['writing'])
        avg_time_of_processing=total_time_of_processing/self.timedict['number of chunks']
        
        
        data = {
            "D.frame size":self.timedict['chunksize'] ,
            'avg_Reading Time': avg_time_of_read,
            'avg_filtering Time': avg_time_of_filter,
            'Total_processing_Time': total_time_of_processing,
            'avg_processing_Time': avg_time_of_processing

            }
        
        headers = list(data.keys())
           # check if the file exists
        try:
            workbook = openpyxl.load_workbook("./output/output.xlsx")
            if sheet_name in workbook.sheetnames:
                worksheet = workbook[sheet_name]
            else:
                worksheet = workbook.create_sheet(sheet_name)
                worksheet.append(headers)
        except FileNotFoundError:
        #create a new workbook and worksheet if the file doesn't exist
            workbook = openpyxl.Workbook()
            worksheet = workbook.create_sheet(sheet_name)
            worksheet.append(headers)
        
         # delete the default 'Sheet' worksheet
        if 'Sheet' in workbook.sheetnames:
            del workbook['Sheet']
        
        # append data to the worksheet
        values = [data[header] for header in headers]
        worksheet.append(values)
            # save the workbook
        workbook.save('./output/output.xlsx')






    def filter_AHO(self):
        elapsed_time=self.elapsed_time(self.start_time)
        logging.info("%s  consumer:start filtering and writing chunks using AHO.",elapsed_time)
        count=1
        while True:
            if not self.input_queue.empty():
                
                chunk = self.input_queue.get()

                if chunk is None:
                    self.success_queue.put(None)
                    self.fail_queue.put(None)
                    break
                
                self.create_Trie_UsingPyAcho(self.patterns)
                self.automaton.make_automaton()
                
                #filtering
                start_time=time.time()
            
                
                boolList = [ ~chunk.iloc[:,head].apply(lambda x : len(list(self.automaton.iter(x.lower()))) != 0) for head in self.Head]
                bool_checker = self.check_bool(boolList)

                healthy_df = chunk[bool_checker]
                unhealthy_df = chunk[~bool_checker]
               
                end_time=time.time()
        
                with self.lock:
                    self.success_queue.put(healthy_df)
                    self.fail_queue.put(unhealthy_df)
                    
                self.timedict['filtering'].append(end_time-start_time)
                boolList.clear()
                
                with self.lock:
                    start_time=time.time()
                    
                    healthy_thread=threading.Thread(target=self.write_csv,args=(self.success_queue,"Healty Record"))
                    healthy_thread.start()
                   
                    unhealthy_thread=threading.Thread(target=self.write_csv,args=(self.fail_queue,"UnHealty Record"))                    
                    unhealthy_thread.start()
                    
                    healthy_thread.join()
                    unhealthy_thread.join()
                    
                    end_time=time.time()

                self.timedict['writing'].append(end_time-start_time)
                elapsed_time=self.elapsed_time(self.start_time)
                logging.info("%s  Consumer: finish filtering and writing the chunk number %s",elapsed_time,count)
                count+=1   











    def filter_RE(self):
            elapsed_time=self.elapsed_time(self.start_time)
            logging.info("%s  consumer:start filtering and writing chunks using RE.",elapsed_time)
            count=1
            while True:
                if not self.input_queue.empty():
                    
                    chunk = self.input_queue.get()

                    if chunk is None:
                        self.success_queue.put(None)
                        self.fail_queue.put(None)
                        break
                
                    #filtering
                    start_time=time.time()
                
                    
                    boolList = [ ~chunk.iloc[:,head].str.contains(self.badWords, regex = True, flags = re.I,na=False) for head in self.Head ]
                    bool_checker = self.check_bool(boolList)

                    healthy_df = chunk[bool_checker]
                    unhealthy_df = chunk[~bool_checker]
                
                    end_time=time.time()
            
                    with self.lock:
                        self.success_queue.put(healthy_df)
                        self.fail_queue.put(unhealthy_df)
                        
                    self.timedict['filtering'].append(end_time-start_time)
                    boolList.clear()
                    
                    with self.lock:
                        start_time=time.time()
                        
                        healthy_thread=threading.Thread(target=self.write_csv,args=(self.success_queue,"Healty Record"))
                        healthy_thread.start()
                        unhealthy_thread=threading.Thread(target=self.write_csv,args=(self.fail_queue,"UnHealty Record"))
                        unhealthy_thread.start()       
                        
                        healthy_thread.join()
                        unhealthy_thread.join()

                        end_time=time.time()

                    self.timedict['writing'].append(end_time-start_time)
                    elapsed_time=self.elapsed_time(self.start_time)
                    logging.info("%s  Consumer: finish filtering and writing the chunk number %s",elapsed_time,count)
                    count+=1

        

    def run(self):
        
        if self.filter_mode=="RE":
            self.filter_RE()
        elif self.filter_mode=="AHO":
            self.filter_AHO()   
        
        elapsed_time=self.elapsed_time(self.start_time)
        logging.info("%s  Consumer:starting write excel and csv files statistics ....",elapsed_time)
        excel_thread=threading.Thread(target=self.ExceLwriter,args=(self.filter_mode,))
        csv_thread=threading.Thread(target=self.write_csv_statistics,args=("indiviual_statistics.csv",))

        excel_thread.start()
        csv_thread.start()
        excel_thread.join()
        csv_thread.join()       

        elapsed_time=self.elapsed_time(self.start_time)
        logging.info("%s  Consumer:finish of write excel and csv files statistics .",elapsed_time)
