
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
import uuid


# Set up the logging
import logging
log = logging.getLogger(__name__)

# from local_modules import ifccityjson as cityjson
from cjio import cityjson
from local_modules.ifccityjson.cityjson2ifc import Cityjson2ifc as Cityjson2ifc
from model.transform import CityJsonToIfc_Instruction, CityJsonToIfc_Result
from long_bg_tasks.task_modules import common_module as common


class CityJsonToIfc:
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = CityJsonToIfc_Instruction(**self.task_dict['CityJsonToIfc_Instruction'])
            self.sourceFileURL = instruction.sourceFileURL
            self.BASE_PATH = task_dict['BASE_PATH']
            self.TEMP_PATH = task_dict['TEMP_PATH']
            self.CONVERSION_OUTFILES = task_dict['CONVERSION_OUTFILES']
            self.PRINT = task_dict['debug']
            if self.PRINT:
                print(f'>>>>> In CityJsonToIfc.init: {self.sourceFileURL}') 
        except Exception as e:
            log.error(f'Error in CityJsonToIfc.init: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in CityJsonToIfc.init: {e}'         
    
    def convert(self):
        try:
            cityJsonTempPath = self.getAndWriteCityJsonFile()
            result_rel_path = f'{self.CONVERSION_OUTFILES}IFC/{str(uuid.uuid4())}.ifc'
            result_path = f'{self.BASE_PATH}{result_rel_path}'       
            data = {}
            data["file_destination"] = result_path
            self.convertToIFC(cityJsonTempPath, data)
            os.remove(cityJsonTempPath)
            result = CityJsonToIfc_Result(
                resultPath = result_rel_path
            )
            self.task_dict['result']['CityJsonToIfc_Result'] = result.dict()  
        except Exception as e:
            log.error(f'Error in CityJsonToIfc.convert: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in CityJsonToIfc.convert: {e}'
        return self.task_dict


    # Get the CityJSON file from the URL source
    def getAndWriteCityJsonFile(self):
        try:
            cityJsonContent = common.getFileBytesContent(self.sourceFileURL).decode('utf-8')
            cityJsonTempPath = f'{self.TEMP_PATH}{str(uuid.uuid4())}.json'
            with open(cityJsonTempPath, 'w') as f:
                f.write(cityJsonContent)
            return cityJsonTempPath
        except Exception as e:
            log.error(f'Error in getAndWriteCityJsonFile: {e}')

    def convertToIFC(self, cityJSON_FilePath, data):
        start = perf_counter()
        if self.PRINT: log.info("Converting CityJSON to IFC")
        city_model = cityjson.load(cityJSON_FilePath, transform=True)
        converter = Cityjson2ifc()
        converter.configuration(**data)
        converter.convert(city_model)
        end = perf_counter()
        if self.PRINT: log.info(f"Conversion took {end-start} seconds")
        return
