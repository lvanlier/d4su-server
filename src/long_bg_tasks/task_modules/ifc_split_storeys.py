##
#   This module is used to split an IFC file by building storeys
#   using the ifcpatch library
##


import long_bg_tasks.task_modules.common_module as common
from model.transform import IfcSplitStoreys_Instruction, IfcSplitStoreys_Result
from data.files import zip_directory

import ifcpatch
import zipfile

import os
import uuid
import shutil


# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter



class IfcSplitStoreys():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction =IfcSplitStoreys_Instruction(**self.task_dict['IfcSplitStoreys_Instruction'])
            self.sourceFileURL = instruction.sourceFileURL
            self.TEMP_PATH = task_dict['TEMP_PATH']
            self.BASE_PATH = task_dict['BASE_PATH']
            self.IFCEXTRACT_PATH = task_dict['IFCEXTRACT_PATH']
            self.PRINT = task_dict['debug']
            self.start = perf_counter()
            if self.PRINT:
                log.info(f'>>>>> In IfcSplitStoreys.init with instruction: {instruction}')
        except Exception as e:
            log.error(f'Error in IfcSplitStoreys.init: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in IfcSplitStoreys.init: {e}'

    def splitStoreys(self):        
        try:
            ifcFilePath = common.setFilePath(self.sourceFileURL, self.BASE_PATH)            
            ifcModel = common.getIfcModel(ifcFilePath)
            result_rel_dir = f'{self.IFCEXTRACT_PATH}{uuid.uuid4()}' 
            result_dir = f'{self.BASE_PATH}{result_rel_dir}' 
            os.makedirs(result_dir, exist_ok=False)                               
            patch_arguments = [result_dir]
            # Get the Ifc file from the source
            ifc_file = ifcModel
            # write the Ifc file to the disk (tempfile)
            # not really nice to do this, but the ifcpatch library requires a file path
            # with the model in addition to the model itself
            ifcTempPath = f'{self.TEMP_PATH}{uuid.uuid4()}.ifc'
            ifc_file.write(ifcTempPath)
            ifc_filePath = ifcTempPath
            patch_recipe = "SplitByBuildingStorey"
            outFilePath = result_dir
            self.apply_patch(ifc_filePath, ifc_file, patch_recipe, patch_arguments, outFilePath)
            # remove the temporary file required by ifcpatch
            os.remove(ifcTempPath)
            # create a zip file for the result directory
            zipFilePath = f'{result_dir}.zip'
            zip_file = zipfile.ZipFile(zipFilePath, 'w')
            # Zip the directory
            zip_directory(result_dir, zip_file)
            # Close the zip file
            zip_file.close()
            # remove the directory
            shutil.rmtree(result_dir, ignore_errors=False)
            result = IfcSplitStoreys_Result(
                resultPath=f'{result_rel_dir}.zip',
                runtime = round(perf_counter() - self.start, 2)
            )
            self.task_dict['result']['IfcSplitStoreys_Result'] = result.dict()               
        except Exception as e:
            log.error(f'Error in IfcSplitStoreys.splitStoreys: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in IfcSplitStoreys.splitStoreys: {e}'
        return self.task_dict

    def apply_patch(self, ifc_filePath, ifc_file, patch_recipe, patch_arguments, outFilePath):
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
        if self.PRINT:
            message = f"Patch time: {t_patch} seconds"
            log.info(message)

