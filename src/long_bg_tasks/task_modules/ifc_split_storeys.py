##
#   This module is used to split an IFC file by building storeys
#   using the ifcpatch library
##

from time import perf_counter

import os
# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()
# Access environment variables as if they came from the actual environment
TMP_PATH = os.getenv('TMP_PATH')

# Set up the logging
import logging
log = logging.getLogger(__name__)

import long_bg_tasks.task_modules.common_module as common

import ifcpatch

def apply_patch(ifc_filePath, ifc_file, patch_recipe, patch_arguments, outFilePath, PRINT=False):
    t1_start = perf_counter()
    outifc = ifcpatch.execute({
        "input": ifc_filePath,
        "file": ifc_file,
        "recipe": patch_recipe,
        "arguments": patch_arguments,
    })
    ifcpatch.write(outifc, outFilePath) 
    t1_stop = perf_counter()
    t_patch = round(t1_stop - t1_start,3)
    ifc_fileName = ifc_filePath.split("/")[-1]
    if PRINT:
        message = f"Patch time: {t_patch} seconds for {ifc_fileName}"
        log.info(message)

######################
# 
#   Split the Building in Storeys
#
######################

def main_proc(task_dict:dict):
    try:
        sourceFileURL = task_dict['instruction_dict']['sourceFileURL']
        ifcOutFileDir = task_dict['ifcOutFileDir']
        patch_arguments = [ifcOutFileDir]
        # Get the Ifc file from the source
        ifc_file = common.getIfcModel(sourceFileURL)
        # write the Ifc file to the disk (tempfile)
        # not really nice to do this, but the ifcpatch library requires a file path
        # with the model in addition to the model itself
        nameTemp = sourceFileURL.split("/")[-1].split(".")[0]
        ifcTempPath = TMP_PATH + nameTemp + ".ifc"
        ifc_file.write(ifcTempPath)
        ifc_filePath = ifcTempPath
        patch_recipe = "SplitByBuildingStorey"
        outFilePath = ifcOutFileDir
        apply_patch(ifc_filePath, ifc_file, patch_recipe, patch_arguments, outFilePath, PRINT=True)
        # remove the temporary file requied by ifcpatch
        os.remove(ifcTempPath)
    except Exception as e:
        log.error(f'Error in main_proc of ifc_split_storeys: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict