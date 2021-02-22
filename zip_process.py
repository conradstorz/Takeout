#!/usr/bin/env python
# -*- coding: utf-8 -*-

# importing required modules 
from loguru import logger
from zipfile import ZipFile, error 
from time import sleep
from tqdm import tqdm
from pathlib import Path

# Logging Setup
logger.remove()  # removes the default console logger provided by Loguru.
# I find it to be too noisy with details more appropriate for file logging.
# INFO and messages of higher priority only shown on the console.
logger.add(lambda msg: tqdm.write(msg, end=""), format="{message}", level="INFO")
# This creates a logging sink and handler that puts all messages at or above the TRACE level into a logfile for each run.
logger.add("file_{time}.log", level="TRACE", encoding="utf8")  # Unicode instructions needed to avoid file write errors.

# specifying the zip file name to process
file_name = "./SAMPLE_ZIPS/takeout-20200903T200301Z-119.zip"

from cfsiv_utils.filehandling import get_files

import datetime
import os

@logger.catch()
def zip_data_explorer(flnm):
    s_extracted_files = []
    s_failed_files = []
    # opening the zip file in READ mode 
    with ZipFile(flnm, 'r') as zip: 
        logger.info(f'Zip contains {len(zip.namelist())} files.')
        for zipinfo in tqdm(zip.infolist()): 
                # logger.info(os.path.basename(zipinfo.filename)) 
                # logger.info('\tModified:\t' + str(datetime.datetime(*zipinfo.date_time))) 
                # logger.info('\tSystem:\t\t' + str(zipinfo.create_system) + '(0 = Windows, 3 = Unix)') 
                # logger.info('\tZIP version:\t' + str(zipinfo.create_version)) 
                # logger.info('\tCompressed:\t' + str(zipinfo.compress_size) + ' bytes') 
                # logger.info('\tUncompressed:\t' + str(zipinfo.file_size) + ' bytes') 
                try:
                    # TODO make this safe by checking for filename collision at destination 
                    zip.extract(zipinfo.filename, path='safer_direc')
                except FileNotFoundError as e:
                    # logger.info(e)
                    # This error seems to occur when the zip contains a bad path for the file.
                    # logger.info('Could not extract!!!')    
                    # TODO find a way to extract this file to a safe path.
                    s_failed_files.append(zipinfo.filename)            
                s_extracted_files.append(zipinfo.filename)
                sleep(.05)
    return (s_extracted_files, s_failed_files)

from pathlib import Path
BASE_DIR = Path.cwd()
RECOVERY_DIR = BASE_DIR.joinpath('safer_direc')

@logger.catch()
def extract_bad_path_files(flnm, flst):
    s_extracted_files = []
    s_failed_files = []
    for file in tqdm(flst):
        filename = Path(os.path.basename(file))
        output = RECOVERY_DIR.joinpath(filename)
        with ZipFile(flnm, 'r') as zip:
            try:
                filebytes = zip.read(file)
            except FileNotFoundError as e:
                logger.info(e)
                # This error seems to occur when the zip contains a bad path for the file.
                logger.info('Could not extract!!!')    
                # TODO find a way to extract this file to a safe path.
                s_failed_files.append(file)   
            with open(output, 'wb') as w:
                w.write(filebytes)         
            s_extracted_files.append(file)              
            sleep(.05)  
    return (s_extracted_files, s_failed_files)



@logger.catch()
def Main():
    e,f = zip_data_explorer(file_name)
    logger.info(f'Number of Extracted files: {len(e)}')
    logger.info(f'Failed files: {len(f)}')
    logger.info('Retrying bad files...')
    e2,f2 = extract_bad_path_files(file_name, f)
    logger.info(f'Number of Extracted files: {len(e2)}')
    if len(f2) < 1:
        logger.info('Success! All files extracted.')
    else:
        logger.info(f'Continued Failed files: {f2}')
    print(Path.cwd())
    files = get_files('S:')
    print(len(files))
    print(Path.cwd())
    return True


if __name__ == "__main__":
    Main()

"""
notes:
outline:
Initialize Script
Recover Data from previous runs
Check Source Directory
Check Output Directory
Ask if this is a new job / Proceed with unfinished jobs
Compare stored Data with Source directory
Compare status with output and stored Data
Move all files from zips to output until interrupted.




# Python program to change the 
# current working directory 
  
  
# importing all necessary libraries  
import sys, os  
    
# initial directory  
cwd = os.getcwd()  
    
# some non existing directory  
fd = 'false_dir/temp'
    
# trying to insert to flase directory  
try:  
    print("Inserting inside-", os.getcwd()) 
    os.chdir(fd)  
        
# Caching the exception      
except:  
    print("Something wrong with specified directory. Exception- ") 
    print(sys.exc_info())  
              
# handling with finally            
finally:  
    print() 
    print("Restoring the path")  
    os.chdir(cwd)  
    print("Current directory is-", os.getcwd())  






zipfile.is_zipfile(filename)

    Returns True if filename is a valid ZIP file based on its magic number, otherwise returns False. filename may be a file or file-like object too.

    Changed in version 3.1: Support for file and file-like objects.


The module defines the following items:

exception zipfile.BadZipFile

    The error raised for bad ZIP files.

    New in version 3.2.

exception zipfile.BadZipfile

    Alias of BadZipFile, for compatibility with older Python versions.

    Deprecated since version 3.2.

exception zipfile.LargeZipFile

    The error raised when a ZIP file would require ZIP64 functionality but that has not been enabled.







ZipFile.extract(member, path=None, pwd=None)

    Extract a member from the archive to the current working directory; member must be its full name or a ZipInfo object. Its file information is extracted as accurately as possible. path specifies a different directory to extract to. member can be a filename or a ZipInfo object. pwd is the password used for encrypted files.

    Returns the normalized path created (a directory or new file).

    Note

    If a member filename is an absolute path, a drive/UNC sharepoint and leading (back)slashes will be stripped, e.g.: ///foo/bar becomes foo/bar on Unix, and C:\foo\bar becomes foo\bar on Windows. And all ".." components in a member filename will be removed, e.g.: ../../foo../../ba..r becomes foo../ba..r. On Windows illegal characters (:, <, >, |, ", ?, and *) replaced by underscore (_).

    Changed in version 3.6: Calling extract() on a closed ZipFile will raise a ValueError. Previously, a RuntimeError was raised.

    Changed in version 3.6.2: The path parameter accepts a path-like object.







ZipFile.read(name, pwd=None)

    Return the bytes of the file name in the archive. name is the name of the file in the archive, or a ZipInfo object. The archive must be open for read or append. pwd is the password used for encrypted files and, if specified, it will override the default password set with setpassword(). Calling read() on a ZipFile that uses a compression method other than ZIP_STORED, ZIP_DEFLATED, ZIP_BZIP2 or ZIP_LZMA will raise a NotImplementedError. An error will also be raised if the corresponding compression module is not available.

    Changed in version 3.6: Calling read() on a closed ZipFile will raise a ValueError. Previously, a RuntimeError was raised.





 ZipFile.testzip()

    Read all the files in the archive and check their CRC’s and file headers. Return the name of the first bad file, or else return None.

    Changed in version 3.6: Calling testfile() on a closed ZipFile will raise a ValueError. Previously, a RuntimeError was raised







ZipFile.comment

    The comment text associated with the ZIP file. If assigning a comment to a ZipFile instance created with mode 'w', 'x' or 'a', this should be a string no longer than 65535 bytes. Comments longer than this will be truncated in the written archive when close() is called.




"""