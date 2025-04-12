import uuid

from local_modules.ifcjson.ifc2json4 import IFC2JSON4 

from data import files as file_store 
import long_bg_tasks.task_modules.common_module as common

from model.transform import ConvertIfcToIfcJson_Instruction, ConvertIfcToIfcJson_Result

import json
   
import os

# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter


class ConvertIfcToIfcJson():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = ConvertIfcToIfcJson_Instruction(**self.task_dict['ConvertIfcToIfcJson_Instruction'])
            self.sourceFileURL = instruction.sourceFileURL
            self.BASE_PATH = task_dict['BASE_PATH']
            self.IFCJSON_PATH = task_dict['IFCJSON_PATH']
            self.PRINT = task_dict['debug']
            self.start = perf_counter()
            if self.PRINT:
                print(f'>>>>> In ConvertIfcToJson: {self.sourceFileURL}') 
        except Exception as e:
            log.error(f'Error in init of ConvertIfcToJson: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in init of ConvertIfcToJson: {e}'
    
    def convert(self):
        try:
            ifcFilePath = common.setFilePath(self.sourceFileURL, self.BASE_PATH)
            ifcModel = common.getIfcModel(ifcFilePath)
            GEOM = True
            ifcJSON = self.getIfc2JSON(ifcModel, GEOM)
            header = dict()
            header["type"] = ifcJSON["type"]
            header["version"] = ifcJSON["version"]
            header["schemaIdentifier"] = ifcJSON["schemaIdentifier"]
            header["originatingSystem"]= ifcJSON["originatingSystem"]
            header["preprocessorVersion"] = ifcJSON["preprocessorVersion"]
            header["timeStamp"] = ifcJSON["timeStamp"]
            rootObjectId = ""
            for item in ifcJSON["data"]:
                if item["type"] == "IfcProject":    
                    rootObjectId = item["globalId"]
                    rootObjectType = item["type"]
                    rootObjectName = item["name"]
                    break
            if not rootObjectId:
                raise Exception("No root object found")
            # Write ifcJSON to a file
            result_rel_path = f'{self.IFCJSON_PATH}{uuid.uuid4()}_NI.json' 
            result_path = f'{self.BASE_PATH}{result_rel_path}'
            self.writeIfcJSON(ifcJSON, result_path)
            # return what is needed for the next step
            result = ConvertIfcToIfcJson_Result(
                rootObjectId = rootObjectId,
                rootObjectType = rootObjectType,
                rootObjectName = rootObjectName,
                resultPath = result_rel_path,
                runtime = round(perf_counter() - self.start, 2)
            )
            self.task_dict['result']['ConvertIfcToIfcJson_Result'] = result.dict()    
        except Exception as e:
            log.error(f'Error ConvertIfcToJson.convert: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error ConvertIfcToJson.convert: {e}'
        finally:
            pass
        return self.task_dict
    
    def getIfc2JSON(self, ifcModel, GEOM):
        try:
            ifcJSON = IFC2JSON4(ifcModel, NO_INVERSE=True, GEOMETRY=GEOM).spf2Json()
            return ifcJSON
        except Exception as e:
            log.error(f'Error in getIfc2JSON: {e}')

    def writeIfcJSON(self, ifcJSON, ifcJsonFilePath):
        file_store.write_file(ifcJsonFilePath, json.dumps(ifcJSON, indent=2))
