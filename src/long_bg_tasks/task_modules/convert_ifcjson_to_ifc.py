import ifcopenshell

from urllib.parse import urlparse
import uuid
import json

import long_bg_tasks.task_modules.common_module as common
from local_modules.ifcjson.to_ifcopenshell import JSON2IFC 

import os
from time import perf_counter
from data import files as file_store
from data import transform as data

from model.transform import ConvertIfcJsonToIfc_Instruction, ConvertIfcJsonToIfc_Result

# Set up the logging
import logging
log = logging.getLogger(__name__)

    
class ConvertIfcJsonToIfc():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = ConvertIfcJsonToIfc_Instruction(**self.task_dict['ConvertIfcJsonToIfc_Instruction'])
            self.sourceFileURL = instruction.sourceFileURL
            self.bundleId = self.task_dict['bundleId']
            self.unitId = self.task_dict['unitId']
            self.TEMP_PATH = self.task_dict['TEMP_PATH']
            self.BASE_PATH = self.task_dict['BASE_PATH']
            self.JSON2IFC_PATH = self.task_dict['JSON2IFC_PATH']
            self.PRINT = self.task_dict['debug']
            if self.PRINT:
                print(f'>>>>> In ConvertIfcJsonToIfc.init: {self.sourceFileURL}') 
        except Exception as e:
            log.error(f'Error in ConvertIfcJsonToIfc.init : {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in ConvertIfcJsonToIfc.init : {e}'

        
    def convert(self):
        try:
            parsed_url = urlparse(self.sourceFileURL)
            if parsed_url.scheme == 'http' or parsed_url.scheme == 'https':
                ifcJsonFilePath = self.sourceFileURL
            else:
                ifcJsonFilePath = f'{self.BASE_PATH}{self.sourceFileURL}'            
            result_rel_path = f'{self.JSON2IFC_PATH}{uuid.uuid4()}_OUT.ifc' 
            result_path = f'{self.BASE_PATH}{result_rel_path}'       
            self.transform_ifcJSON_to_ifc(ifcJsonFilePath,result_path)
            result = ConvertIfcJsonToIfc_Result(
                resultPath = result_rel_path
            )
            self.task_dict['result']['ConvertIfcJsonToIfc_Result'] = result.dict()
            if self.unitId is not None and self.unitId != '':
                # update the unit in the database
                if self.PRINT:
                    message = f'>>> before data.updateBundleUnitJson; bundleId:{self.bundleId}, unitId: {self.unitId}'
                    log.info(message)
                    print(message)
                data.updateBundleUnitJson(self.bundleId, self.unitId, 'IFC', result_rel_path)
            else:
                if self.PRINT:
                    message = f'>>> unitId: {self.unitId}'
                    log.info(message)
                    print(message)
        except Exception as e:
            log.error(f'Error in ConvertIfcJsonToIfc.convert: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in ConvertIfcJsonToIfc.convert: {e}'
        return self.task_dict
    
    
    def transform_ifcJSON_to_ifc(self, ifcJsonFilePath, ifcFilePath):
        try:
            # convert IfcJSON to Ifc
            t1_start = perf_counter()
            if self.PRINT:
                message = f"Transforming Model < {ifcJsonFilePath} > to IFC < {ifcFilePath} >"
                log.info(message)
            jsonData_JSON, header = common.get_ifcJson(ifcJsonFilePath)
            jsonData = json.dumps(jsonData_JSON)
            if self.PRINT:
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
            ifcTempPath = f'{self.TEMP_PATH}{uuid.uuid4()}_OUT.ifc' 
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
            if self.PRINT:
                message = f"Transformation of IfcJson to IFC took (in seconds) total: {t_total} [transform: {t_transform}, write: {t_write}]"
                log.info(message)
        except Exception as e:
            message = f"Error in transform_jsonIFC_to_ifc: {e}"
            log.error(message)
            raise ValueError(message)  

