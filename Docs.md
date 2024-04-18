
# Bad Words Detector

## Objective
Detect bad words from a .csv file compressed in a .rar format using the provided Bad_Words.csv file and output both excel and .csv files that have analytics about the process.
## Key Outcomes
a .csv file containing detailed info about each chunk's size, order, # of healthy records, # of unhealthy records, reading time, filtering time, and total time it took in processor. Also a .xlsx file containing the summary of the statistics of time written from the outputed .csv file.
## Hypothesis
A very big .csv file is compressed into .rar format, this .csv file contains bad words like Ho, etc. We want to filter out these bad words choosing between two filtering techniques and find out which one is better in terms of accuracy and time taken.
## Stake Holders
- Project Manager: Mohamed Saeed
- Dev: Mohamed Saeed, Khaled Mohamed, Hossam Walid
- Design: Kareem Essam, Abd El Rahman Mohamed Mohamed
- Support: Ahmed Gharieb
## Scope
-Must Have: Python, pandas, pyahocorasick, rarfile, openpyxl, faker
## Timeline
### Initial commit

    Author: Hossam Walid 

    Date:  Sat Mar 30 00:34:47 2024

### Intialize 2

    Author: Hossam Walid 

    Date: Sat Mar 30 00:43:00 2024 

### Update ignores 1

    Author: Hossam Walid 

    Date: Sat Mar 30 00:51:27 2024 

### Test

    Author: Hossam Walid 

    Date:   Sat Mar 30 01:35:17 2024 

### Feat: initial refactoring (filter classes)

    Author: Mohamed Saeed 

    Date:  Sat Mar 30 01:38:43 2024 

### Refactoring add filter classes and Merge pull request 1 from GreenVenom77/refactoring 

    Author: Hossam Walid 

    Date: Sat Mar 30 01:42:41 2024 

### Update git ignore

    Author: Mohamed Saeed 

    Date: Sat Mar 30 01:48:01 2024
### Test

    Author: Hossam Walid 

    Date:   Sat Mar 30 01:35:17 2024

### Feat: initial refactoring (filter classes)

    Author: Mohamed Saeed 

    Date:   Sat Mar 30 01:38:43 2024 
### Merge branch 'development' of https://github.com/GreenVenom77/Automata_Project into development
    Author: Mohamed Saeed 
    Date:   Sat Mar 30 01:53:00 2024 
### Delete .idea directory
    Author: Hossam Walid 
    Date:   Sat Mar 30 02:03:36 2024 
### Feat:Update filter file adding the needed classes functions
    Author: Mohamed Saeed 
    Date:   Wed Apr 3 18:58:42 2024 
### Merge branch 'development' of https://github.com/GreenVenom77/Automata_Project into development
    Author: Mohamed Saeed 
    Date:   Wed Apr 3 18:59:11 2024 
### Feat: add arguments.py to wrap argument parsing into Args object.
    Author: Mohamed Saeed 
    Date:   Thu Apr 4 18:48:47 2024 
### Fix: Commit filter mode and processing Mode Enums
    Author: Mohamed Saeed 
    Date:   Thu Apr 4 18:50:47 2024 
### Fix: remove test from filter file
    Author: Mohamed Saeed 
    Date:   Thu Apr 4 18:54:30 2024 
### Update enum argument choices in argument parser
    Author: Mohamed Saeed 
    Date:   Thu Apr 4 21:28:53 2024 
### Feat: add concurrent model module to manage the concurrent work
    Author: Mohamed Saeed 
    Date:   Thu Apr 4 21:30:25 2024 
### Update args.json
    Author: Mohamed Saeed 
    Date:   Thu Apr 4 21:30:58 2024 
### Refactor producer and main
    Author: Mohamed Saeed 
    Date:   Thu Apr 4 22:14:23 2024    
### Update processing mode Enum
    Author: Mohamed Saeed 
    Date:   Thu Apr 4 22:59:23 2024 
### Update consumer
    Author: Mohamed Saeed 
    Date:   Fri Apr 5 04:05:26 2024 
### Update: update Process Pool Model
    Author: Mohamed Saeed 
    Date:   Fri Apr 5 14:06:17 2024 
### Update args
    Author: Mohamed Saeed 
    Date:   Fri Apr 5 14:07:48 2024
### Update default value of chunk size
    Author: Mohamed Saeed 
    Date:   Sat Apr 6 02:29:08 2024 
### Add number of (healthy,unhealthy rows) to generate_time_dict()
    Author: Mohamed Saeed 
    Date:   Sat Apr 6 02:30:06 2024 
### Remove write_csv from consumer , update input queue structure and in case of pool of process sync time dict in consumer
    Author: Mohamed Saeed 
    Date:   Sat Apr 6 02:32:09 2024
### Update: add accumulation the number of healthy & unhealthy rows.
    Author: Mohamed Saeed 
    Date:   Sat Apr 6 02:44:41 2024 
### Update gitignore
    Author: Hossam Walid
    Date:   Sat Apr 6 20:45:18 2024 
### Solve multiple consumer problems on editing statistics dictionary using create a ChunkInfo data structure to hold chunk processing info and using lock
    Author: Mohamed Saeed 
    Date:   Sun Apr 7 00:04:37 2024
### Feat: add statistics writer class.
    Author: Mohamed Saeed 
    Date:   Sun Apr 7 00:05:53 2024 
### Merge branch 'development' of https://github.com/GreenVenom77/Automata_Project into development
    Author: Mohamed Saeed 
    Date:   Sun Apr 7 00:06:05 2024
### Fix: multiple processing statistics data access problem
    Author: Mohamed Saeed 
    Date:   Mon Apr 8 02:13:38 2024 
### gitignore update
    Author: Hossam Walid
    Date:   Mon Apr 8 02:52:41 2024 
### Round the frame_timeTotal
    Author: Khaled Mohamed
    Date:   Mon Apr 8 14:37:53 2024 
### Feat: update statistics writer first output
    Author: Mohamed Saeed 
    Date:   Mon Apr 8 19:46:50 2024 
### Merge branch 'development' of https://github.com/GreenVenom77/Automata_Project into development
    Author: Mohamed Saeed 
    Date:   Mon Apr 8 19:51:21 2024 
### Replace statistics csv columns
    Author: Mohamed Saeed 
    Date:   Mon Apr 8 21:03:51 2024 
### Feat: add write_excel() to StaticsWriter class and change naming som objects and remove head columns user parameter.
    Author: Mohamed Saeed 
    Date:   Tue Apr 9 02:23:51 2024 
### First Fixed and working version from the app
    Author: Mohamed Saeed 
    Date:   Tue Apr 9 16:30:19 2024 
### Fix: check arguments type
    Author: Mohamed Saeed 
    Date:   Sun Apr 14 18:06:38 2024 
### Update and rename args.json to _args.json
    Author: Hossam Walid
    Date:   Sun Apr 14 18:19:50 2024 
### Readme update
    Author: Hossam Walid
    Date:   Mon Apr 15 04:03:26 2024 
![](https://myoctocat.com/assets/images/base-octocat.svg)
