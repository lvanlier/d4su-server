
##
#
#   This module is used to convert a CityJSON file into an IFC file
#
##

import urllib.request
import os
import sys
from time import perf_counter
import json


# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()
# Access environment variables as if they came from the actual environment
TMP_PATH = os.getenv('TMP_PATH')

# Set up the logging
import logging
log = logging.getLogger(__name__)

# from local_modules import ifccityjson as cityjson
from cjio import cityjson
from local_modules.ifccityjson.cityjson2ifc import Cityjson2ifc as Cityjson2ifc

# Get the CityJSON file from the URL source
def getAndWriteCityJsonFile(sourceFileURL : str, TMP_PATH : str):
    try:
        response = urllib.request.urlopen(sourceFileURL)
        cityJsonContent = response.read().decode('utf-8')
        fileName = sourceFileURL.split("/")[-1].split(".")[0]
        cityJsonTempPath = TMP_PATH + fileName + ".json"
        with open(cityJsonTempPath, 'w') as f:
            f.write(cityJsonContent)
        return cityJsonTempPath, fileName
    except Exception as e:
        log.error(f'Error in getAndWriteCityJsonFile: {e}')

def convertToIFC(cityJSON_FilePath, data, PRINT=False):
    start = perf_counter()
    if PRINT: log.info("Converting CityJSON to IFC")
    city_model = cityjson.load(cityJSON_FilePath, transform=True)
    converter = Cityjson2ifc()
    converter.configuration(**data)
    converter.convert(city_model)
    end = perf_counter()
    if PRINT: log.info(f"Conversion took {end-start} seconds")
    return data["file_destination"]

######################
# 
#   Convert CityJSON to IFC 
#
######################

def main_proc(task_dict:dict):
    try:
        sourceFileURL = task_dict['instruction_dict']['sourceFileURL']
        ifcOutFileDir = task_dict['ifcOutFileDir']     
        cityJsonTempPath, fileName = getAndWriteCityJsonFile(sourceFileURL, TMP_PATH)
        data = {}
        data["file_destination"] = ifcOutFileDir+fileName+".ifc"
        outFilePath = convertToIFC(cityJsonTempPath, data, PRINT=False)
        task_dict['ifcOutFilePath'] = outFilePath
        os.remove(cityJsonTempPath)
    except Exception as e:
        log.error(f'Error in main_proc of cityjson_to_ifc: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict