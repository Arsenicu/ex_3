#!/usr/bin/env python
""" Andrey S

    The script reads a zip archive with csv files,
    filters records with a company by OKVD (61)
    and passes them to the database
    
    Education project
"""
import argparse

from tqdm import tqdm
from sqlalchemy import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import exc
from sqlalchemy import  Column, Integer, String, BigInteger
import time

from zipfile import ZipFile
import concurrent.futures
import multiprocessing
import itertools

import orjson
import logging.config
import traceback

LOG_CONFIG = {
    "version": 1,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "ERROR"
        },
        "file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "level": "DEBUG",
            "when": "D",
            "backupCount": 0,
            "filename": "./zip_reader.log"
        }
    },
    "loggers": {
        "__main__": {
            "handlers": ["console", "file"]
        },
        "": {
            "handlers": ["file"],
            "propagate": True
        }
    }
}

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

"""
    Structures for database
"""


 
"""
    first init database, drop table, if exist
    read all company records from Queue and write it's to databes every 1 second
"""
def read_pool(records_queue, counter_db, read_done, lock, hook):
    engine = hook.get_sqlalchemy_engine()
    DeclarativeBase = declarative_base()
    #class hw1(DeclarativeBase): pass
    class company(DeclarativeBase):
        __tablename__ = "telecom_companies"
        ogrn = Column(BigInteger, primary_key=True, index=True)
        inn = Column(String)
        kpp = Column(String)
        name = Column(String) 
        full_name = Column(String)
        okved_code= Column(String)
    try:
        #engine=create_engine("sqlite:///hw1.db")
        #DeclarativeBase.metadata.create_all(engine)
        company.metadata.drop_all(bind=engine)
        company.metadata.create_all(bind=engine)
        Session = sessionmaker(bind=engine)

        with Session(autoflush=False) as db:            
            while True: 
                #while process for reading from zip is working and queue is not empty
                time.sleep(1)
                records = []
                while not records_queue.empty():
                    records.append(records_queue.get())
                try:
                    db.execute(insert(company), records)
                    db.commit()
                    with lock:
                        counter_db.value+=len(records)
                except exc.IntegrityError as err:
                    logger.error("Index OKVD already exists", exc_info=True)
                    raise
                except exc.SQLAlchemyError:
                    logger.error("Exception SQLAlchemy occurred", exc_info=True)
                    raise
                
                if (read_done.value):
                    if records_queue.empty():
                        time.sleep(1)
                        break;
    except Exception as error:
        print(error)
        logger.error("Exception with database occurred", exc_info=True)
        
        #Raise
     
    return True

"""
    read single csv file from zip, extract entry with filter and add record in Queue
"""    
def read_single_file(contained_file, archive, records_queue, counter, lock):
    try:
        with ZipFile(archive) as csv_zip:
            for line in csv_zip.open(contained_file).readlines():
                orr=orjson.loads(line)
                for ora in orr:
                    if ("СвОКВЭД" in ora["data"] and "СвОКВЭДОсн" in ora["data"]["СвОКВЭД"]):
                        if (ora["data"]["СвОКВЭД"]["СвОКВЭДОсн"]["КодОКВЭД"][:2]=="61"):
                            with lock:
                                counter.value += 1
                            record_put = {
                                "ogrn": ora.get("ogrn"),
                                "inn": ora.get("inn"),
                                "kpp": ora.get("kpp"),
                                "name": ora.get("name"),
                                "full_name": ora.get("full_name"), 
                                "okved_code" : ora["data"]["СвОКВЭД"]["СвОКВЭДОсн"]["КодОКВЭД"]
                            }
                            records_queue.put(record_put)
    except Exception as error:
        logger.error(f"Exception when read from {contained_file} file", exc_info=True)

    return True 
     
"""
    start here
    read zip file
    create pool with concurrent process, pass single file to process
    
"""    
def main(archive, hook):
    with ZipFile(archive) as csv_zip:
        try:
            with multiprocessing.Manager() as manager:
                counter = manager.Value('i', 0) #records found by filter
                counter_db = manager.Value('i', 0) #records inserted to DB
                records_queue = manager.Queue() #Queue with records, for write to DB
                lock = manager.Lock()
                read_done = manager.Value('i', False) #True - when the whole zip file has been read
                
                
                
                with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()*2) as executor:  
                    file_list=csv_zip.infolist()
                    ##
                    #file_list=file_list[1:100]
                    ##
                    #start process for working with database
                    
                    #file_list=file_list[0:100] #TEST
                    files_cnt=len(file_list)
                    
                    #progress bar
                    with tqdm(total=files_cnt, unit='files') as pbar:
                        inres=executor.submit(read_pool, records_queue, counter_db, read_done, lock, hook)
                        #repeats arguments for passing it to function map
                        for _ in executor.map(read_single_file, file_list, itertools.repeat(archive, files_cnt), itertools.repeat(records_queue,files_cnt), itertools.repeat(counter,files_cnt), itertools.repeat(lock,files_cnt)):
                            total=len(file_list)
                            pbar.set_description(f"Records found/inserted: {counter.value}/{counter_db.value}")    
                            pbar.update()
                        #ok, we have read the whole zip file    
                        with lock:
                            read_done.value=True   
                        res = inres.result()
                        #well done! we have inserted all the records from the Queue into the database
                        pbar.set_description(f"Completed! records found: {counter.value} inserted to db: {counter_db.value}")    
                        pbar.update()
                return counter_db.value
        except Exception as error:
            #print(error)
            print(traceback.format_exception(error))
            logger.critical("Unhandled exception", exc_info=True)
                   
        #TODO: block double taped KeyboardInterrupt          
        #except KeyboardInterrupt:
        #    executor.shutdown(wait=False, cancel_futures=True)        
        #    print('Canceled by user')
        finally:
            executor.shutdown(wait=False, cancel_futures=True)           
          