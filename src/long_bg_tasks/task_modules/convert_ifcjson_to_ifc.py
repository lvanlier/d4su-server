import ifcopenshell

from local_modules.ifcjson.to_ifcopenshell import JSON2IFC 

import os
from time import perf_counter
from data import files as file_store

# Set up the logging
import logging
log = logging.getLogger(__name__)

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()

# Access environment variables as if they came from the actual environment
TMP_PATH = os.getenv('TMP_PATH')


def transform_ifcJSON_to_ifc(ifcJsonFilePath,ifcFilePath):
    try:
        # convert IfcJSON to Ifc
        t1_start = perf_counter()
        if PRINT:
            message = f"Transforming Model < {ifcJsonFilePath} > to IFC < {ifcFilePath} >"
            log.info(message)
        jsonData = file_store.read_file(ifcJsonFilePath)
        if PRINT:
            message = f'jsonDATA: {type(jsonData)} length: {len(jsonData)} first 1000 chars: {jsonData[:1000]}'
            log.info(message)
        json2ifc = JSON2IFC(jsonData)
        ifcModel = json2ifc.ifcModel()
        t1_stop = perf_counter()
        t_transform = round(t1_stop - t1_start, 2)
        # write the Ifc model to a file
        t1_start = perf_counter()
        #
        # not clean at all but ifcopenshell does not allow to write to a file-like object
        # therefore we need to write to a file and then read it back
        nameTemp = ifcFilePath.split("/")[-1]
        ifcTempPath = TMP_PATH + nameTemp + ".ifc"
        ifcModel.write(ifcTempPath)
        # read the file back
        with open(ifcTempPath, "r") as f:
            ifcModelData = f.read() 
        # write the model to the file store    
        file_store.write_file(ifcFilePath, ifcModelData)
        # remove the temporary file
        os.remove(ifcTempPath)
        
        t1_stop = perf_counter()
        
        t_write = round(t1_stop - t1_start, 2)   
        t_total = round(t_transform + t_write,2)
        json_fileName = ifcJsonFilePath.split("/")[-1]
        ifc_fileName = ifcFilePath.split("/")[-1]
        if PRINT:
            message = f"Transformation of Model < {json_fileName} > to IFC < {ifc_fileName} > took (in seconds) total: {t_total} [transform: {t_transform}, write: {t_write}]"
            log.info(message)
    except Exception as e:
        message = f"Error in transform_jsonIFC_to_ifc: {e}"
        log.error(message)
        raise ValueError(message)  

##
#
#   MAIN PROCEDURE
#
##
    
def main_proc(task_dict):
    try:
        ifcJsonFilePath = task_dict['jsonFilePath']
        ifcFilePath  = task_dict['ifcOutFilePath']
        global PRINT
        PRINT = task_dict['debug']
        transform_ifcJSON_to_ifc(ifcJsonFilePath,ifcFilePath)
        # os.remove(ifcJsonFilePath)
        # to be uncommented when all has been tested for spatialunit extraction
        # file_store.delete_file(ifcJsonFilePath)
    except Exception as e:
        log.error(f'Error in main_proc of convert_ifcjson_to_ifc: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict