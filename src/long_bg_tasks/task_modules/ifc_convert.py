##
#   This module is used to convert an IFC file into another format
#   typically used to represent the model on a map, on a city model, ...
#   The conversion is carried out by IfcConvert from the IfcOpenShell library
#
#   The CityJSON Version is outdated 
#   Upgrade necessary after pip install [cjio](https://github.com/cityjson/cjio)
#   cjio is a command-line tool to process CityJSON files
#   cjio Duplex_A_20110907_optimized.json upgrade save Duplex_A_20110907_optimized_CityJson2.json
#
##

import subprocess
import os

import os
# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()
# Access environment variables as if they came from the actual environment
IFCCONVERT_WD = os.getenv('IFCCONVERT_WD')
TMP_PATH = os.getenv('TMP_PATH')

# Set up the logging
import logging
import time
log = logging.getLogger(__name__)

import long_bg_tasks.task_modules.common_module as common

def convert_with_IfcConvert(ifcSourceFilePath, targetFormat, outfileDir, outFileName, timeout, PRINT=False): 
    if targetFormat == 'glTF':
        conversion = 'glTF'
        conversionExt = '.glb'
    elif targetFormat == 'COLLADA':
        conversion = 'COLLADA'
        conversionExt = '.dae'
    elif targetFormat == 'CityJSON':  
        conversion = 'CityJSON'
        conversionExt = '.cityjson'
    else:
        raise ValueError(f"Unknown targetFormat: {targetFormat}")
    
    part_1 = "./IfcConvert"
    part_2 = ifcSourceFilePath
    part_3 = outfileDir+conversion+"/"+outFileName+conversionExt
    conversionCommand = [
        part_1,
        part_2,
        part_3
    ]
    if PRINT:
        message = f'conversionCommand: {conversionCommand}'
        log.info(message)
    # IfcConvert does not overwrite existing files, without asking for confirmation
    # so we need to remove the output file if it exists    
    if os.path.exists(part_3):
        os.remove(part_3)  
    process = subprocess.Popen(conversionCommand, cwd=IFCCONVERT_WD, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        message = f'Process with PID {process.pid} was killed after timeout of 60 seconds'
        log.error(message)
    if process.returncode != 0:
        message = f'Process with PID {process.pid} finished with return code {process.returncode} and stderr: {process.stderr}'
        log.error(message)
        raise Exception(f'Error in convert_with_IfcConvert: {process.stderr}')
        
    # Change the extension from .cityjson to .json
    if conversion == 'CityJSON':
        os.rename(part_3, part_3[:-9]+'.json')
    outFilePath = part_3
    return outFilePath

##
# 
#   Convert IFC to another format
#
##

def main_proc(task_dict:dict):
    try:
        sourceFileURL = task_dict['instruction_dict']['sourceFileURL']
        outFileDir = task_dict['outFileDir']
        timeout = task_dict['instruction_dict']['timeout']
        # Get the Ifc file from the source
        ifc_file = common.getIfcModel(sourceFileURL)
        fileName = sourceFileURL.split("/")[-1].split(".")[0]
        ifcTempPath = TMP_PATH + fileName + ".ifc"
        ifc_file.write(ifcTempPath)
        ifc_filePath = ifcTempPath
        targetFormat = task_dict['instruction_dict']['targetFormat']
        if task_dict['debug'] == True:
            PRINT = True
        outFilePath = convert_with_IfcConvert(ifc_filePath, targetFormat, outFileDir, fileName, timeout, PRINT)
        task_dict['outFilePath'] = outFilePath
        os.remove(ifc_filePath)
    except Exception as e:
        log.error(f'Error in main_proc of ifc_convert: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict
