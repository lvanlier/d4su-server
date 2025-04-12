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

from model.transform import IfcConvert_Instruction, IfcConvert_Result
from data import transform as data

import time
import uuid

# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter


import long_bg_tasks.task_modules.common_module as common


class IfcConvert:
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = IfcConvert_Instruction(**self.task_dict['IfcConvert_Instruction'])
            self.sourceFileURL = instruction.sourceFileURL
            self.timeout = instruction.timeout
            self.targetFormat = instruction.targetFormat
            self.bundleId = instruction.bundleId
            self.unitId = instruction.unitId
            self.BASE_PATH = task_dict['BASE_PATH']
            self.TEMP_PATH = task_dict['TEMP_PATH']
            self.CONVERSION_OUTFILES = task_dict['CONVERSION_OUTFILES']
            self.IFCCONVERT_WD = task_dict['IFCCONVERT_WD']
            self.PRINT = task_dict['debug']
            self.start = perf_counter()
            if self.PRINT:
                print(f'>>>>> In IfcConvert: {self.sourceFileURL}') 
        except Exception as e:
            log.error(f'Error in IfcConvert.init: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in IfcConvert.init: {e}' 

        
    def convert(self):
        try:
            ## outFileDir = task_dict['outFileDir']
            # Get the Ifc file from the source
            ifc_file = common.getIfcModel(self.sourceFileURL)
            ifcTempPath = f'{self.TEMP_PATH}{str(uuid.uuid4())}.ifc'
            ifc_file.write(ifcTempPath)
            result_fileName = f'{str(uuid.uuid4())}'
            result_rel_dir = f'{self.CONVERSION_OUTFILES}'
            result_dir = f'{self.BASE_PATH}{result_rel_dir}'       
            outFileConversionDirAndFileName = self.convert_with_IfcConvert(ifcTempPath, result_dir, result_fileName)
            result_rel_path = f'{result_rel_dir}{outFileConversionDirAndFileName}'
            os.remove(ifcTempPath)
            result = IfcConvert_Result(
                resultPath = result_rel_path,
                runtime = round(perf_counter() - self.start, 2) 
            )
            self.task_dict['result']['IfcConvert_Result'] = result.dict()
            if self.bundleId is not None and self.unitId is not None and self.bundleId !='' and self.unitId != '':
                data.updateBundleUnitJson(self.bundleId, self.unitId, self.targetFormat, result_rel_path)
        except Exception as e:
            log.error(f'Error in main_proc of ifc_convert: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = str(e)
        return self.task_dict

    def convert_with_IfcConvert(self, ifcSourceFilePath, outfileDir, outFileName): 
        if self.targetFormat == 'glTF':
            conversion = 'glTF'
            conversionExt = '.glb'
        elif self.targetFormat == 'COLLADA':
            conversion = 'COLLADA'
            conversionExt = '.dae'
        elif self.targetFormat == 'CityJSON':  
            conversion = 'CityJSON'
            conversionExt = '.cityjson'
        else:
            raise ValueError(f"Unknown targetFormat: {self.targetFormat}")
        
        part_1 = "./IfcConvert"
        part_2 = ifcSourceFilePath
        part_3 = outfileDir+conversion+"/"+outFileName+conversionExt
        conversionCommand = [
            part_1,
            part_2,
            part_3
        ]
        if self.PRINT:
            message = f'conversionCommand: {conversionCommand}'
            log.info(message)
        # IfcConvert does not overwrite existing files, without asking for confirmation
        # so we need to remove the output file if it exists    
        if os.path.exists(part_3):
            os.remove(part_3)  
        process = subprocess.Popen(conversionCommand, cwd=self.IFCCONVERT_WD, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            process.wait(timeout=self.timeout)
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
        outfileName = outFilePath.split('/')[-1]
        return f'{conversion}/{outfileName}'
