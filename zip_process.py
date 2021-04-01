#!/usr/bin/env python
# -*- coding: utf-8 -*-

# importing required modules
from io import FileIO
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
logger.add(
    "./LOGS/file_{time}.log", level="TRACE", encoding="utf8"
)  # Unicode instructions needed to avoid file write errors.

# specifying the zip file name to process
file_name = "./SAMPLE_ZIPS/takeout-20200903T200301Z-119.zip"

from cfsiv_utils.filehandling import get_files

import datetime
import os


@logger.catch()
def zip_data_explorer(flnm: Path, action=None):
    if action == None:
        action = []
    else:
        if type(action) != list:
            logger.error(f"action option must be a list.")
    s_extracted_files = []
    s_failed_files = []
    # opening the zip file in READ mode
    with ZipFile(flnm, "r") as zip:
        found_files = len(zip.namelist())
        # logger.info(f'Zip contains {found_files} files.')
        if "view" in action:
            for zipinfo in zip.infolist():
                logger.info(os.path.basename(zipinfo.filename))
                logger.info(
                    "\tModified:\t" + str(datetime.datetime(*zipinfo.date_time))
                )
                logger.info(
                    "\tSystem:\t\t"
                    + str(zipinfo.create_system)
                    + "(0 = Windows, 3 = Unix)"
                )
                logger.info("\tZIP version:\t" + str(zipinfo.create_version))
                logger.info("\tCompressed:\t" + str(zipinfo.compress_size) + " bytes")
                logger.info("\tUncompressed:\t" + str(zipinfo.file_size) + " bytes")
        if "compile" in action:
            file_names = []
            compressed_size = 0
            expanded_size = 0
            for zipinfo in zip.infolist():
                file_names.append(os.path.basename(zipinfo.filename))
                compressed_size += int(zipinfo.compress_size)
                expanded_size += int(zipinfo.file_size)
        if "extract" in action:
            try:
                # TODO make this safe by checking for filename collision at destination
                zip.extract(zipinfo.filename, path="safer_direc")
            except FileNotFoundError as e:
                # logger.info(e)
                # This error seems to occur when the zip contains a bad path for the file.
                # logger.info('Could not extract!!!')
                # TODO find a way to extract this file to a safe path.
                s_failed_files.append(zipinfo.filename)
            s_extracted_files.append(zipinfo.filename)
            sleep(0.05)
    if "compile" in action:
        return (file_names, compressed_size, expanded_size)
    return (s_extracted_files, s_failed_files, found_files)


from pathlib import Path

BASE_DIR = Path.cwd()
RECOVERY_DIR = BASE_DIR.joinpath("safer_direc")


@logger.catch()
def extract_file(zipfile, target_file, target_directory):
    filename = Path(os.path.basename(target_file))
    output = target_directory.joinpath(filename)
    with ZipFile(zipfile, "r") as zip:
        try:
            filebytes = zip.read(target_file)
        except FileNotFoundError as e:
            logger.error(f"Error reading {target_file}\n{e}")
            # This error seems to occur when the zip contains a bad path for the file.
            logger.error("Could not extract!!!")
            # TODO find a way to extract this file to a safe path.
        except KeyError as e:  # occurs when trying to extract files that don't exist in zip.
            logger.error(f"File, {filename}, was not found in zip: {zipfile}")
        else:
            with open(output, "wb") as w:
                try:
                    w.write(filebytes)
                except error as e:
                    logger.error(f"Couldn't write file: {output}\n{e}")
                    raise (e)
    return True


@logger.catch()
def extract_bad_path_files(flnm, flst):
    s_extracted_files = []
    s_failed_files = []
    for file in flst:
        filename = Path(os.path.basename(file))
        output = RECOVERY_DIR.joinpath(filename)
        try:
            extract_file(flnm, file, output)
            s_extracted_files.append(file)
        except error as e:
            s_failed_files.append(file)
            logger.error(e)
    return (s_extracted_files, s_failed_files)


@logger.catch()
def Main1():
    total_files_found = 0
    total_files_extracted = 0
    files = get_files("S:", "*takeout*.zip")
    for file in files:
        logger.info(f"Extracting zip: {file}")
        e, f, found = zip_data_explorer(file, action=["view"])
        total_files_found += found
        total_files_extracted += len(e)
        logger.info(f"Number of Extracted files: {len(e)}")
        logger.info(f"Failed files: {len(f)}")
        logger.info("Retrying bad files...")
        e2, f2 = extract_bad_path_files(file, f)
        logger.info(f"Number of Extracted files: {len(e2)}")
        total_files_extracted += len(e2)
        if len(f2) < 1:
            logger.info("Success! All files extracted.")
        else:
            logger.info(f"Continued Failed files: {f2}")
        logger.info()
    return (total_files_extracted, total_files_found)


@logger.catch()
def Main2():
    all_files_found = []
    total_extracted_size = 0
    total_compressed_size = 0
    files = get_files("S:", "*takeout*.zip")
    for file in tqdm(files):
        logger.info(f"Examining zip: {file}")
        fns, compr, expnd = zip_data_explorer(file, action=["compile"])
        all_files_found += fns
        total_extracted_size += expnd
        total_compressed_size += compr
    return (all_files_found, total_extracted_size, total_compressed_size)


if __name__ == "__main__":
    fls, ext, cmp = Main2()
    logger.info("")
    logger.info(f"Found {len(fls)} files total with {len(set(fls))} unique names.")
    logger.info(f"Total size uncomressed size: {ext/1000000000} Gigs")
    logger.info(f"Compressed size is: {cmp/1000000000} Gigs")


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
    logger.info("Inserting inside-", os.getcwd()) 
    os.chdir(fd)  
        
# Caching the exception      
except:  
    logger.info("Something wrong with specified directory. Exception- ") 
    logger.info(sys.exc_info())  
              
# handling with finally            
finally:  
    logger.info() 
    logger.info("Restoring the path")  
    os.chdir(cwd)  
    logger.info("Current directory is-", os.getcwd())  






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
