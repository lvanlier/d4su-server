import uuid


import tempfile

from data import files as file_store 
import long_bg_tasks.task_modules.common_module as common

from pydantic import BaseModel
from model.transform import MigrateIfcSchema_Instruction, MigrateIfcSchema_Result

import ifcpatch
   
import os

# Set up the logging
import logging
log = logging.getLogger(__name__)


class MigrateIfcSchema():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = MigrateIfcSchema_Instruction(**self.task_dict['MigrateIfcSchema_Instruction'])
            self.sourceFileURL = instruction.sourceFileURL
            self.targetSchema = instruction.targetSchema
            self.PRINT = task_dict['debug']
            self.TEMP_PATH = task_dict['TEMP_PATH']
            self.BASE_PATH = task_dict['BASE_PATH']
            self.MIGRATED_PATH = task_dict['MIGRATED_PATH']
            if self.PRINT:
                log.info(f'>>>>> In MigrateIfcSchema.init with instruction: {instruction}')
        except Exception as e:
            log.error(f'Error in init of MigrateIfcSchema: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in init of MigrateIfcSchema: {e}'
    
    def migrate(self):
        try:
            ifcModel = common.getIfcModel(self.sourceFileURL)
             
            input_schema = ifcModel.schema 
            # also possible:
            # input_schema = ifcModel.wrapped_data.header.file_schema.schema_identifiers[0]
            if input_schema == 'IFC2X3':
                ifcModel = self.migrateIFC(ifcModel)
                result_rel_path = f'{self.MIGRATED_PATH}{uuid.uuid4()}.ifc' 
                result_path = f'{self.BASE_PATH}{result_rel_path}'
                ifcModel.write(result_path)
                result = MigrateIfcSchema_Result(
                    resultPath = result_rel_path
                )
                self.task_dict['result']['MigrateIfcSchema_Result'] = result.dict()               
            else:
                message = f'IFC file is already in {input_schema} schema'
                log.info(message)
                self.task_dict['status'] = 'failed'
                self.task_dict['error'] = message   
        except Exception as e:
            log.error(f'Error in main_proc of MigrateIfcSchema: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = str(e)
        finally:
            pass
        return self.task_dict
    
    def migrateIFC(self, ifcModel):
        try:
            ifcTempPath = self.TEMP_PATH + str(uuid.uuid4()) +".ifc"
            ifcModel.write(ifcTempPath)
            ifc_filePath = ifcTempPath
            patch_recipe = "Migrate"
            schema = self.targetSchema
            patch_arguments = [schema]
            ifcModel = ifcpatch.execute({
                "input": ifc_filePath,
                "file": ifcModel,
                "recipe": patch_recipe,
                "arguments": patch_arguments,
            })
            os.remove(ifc_filePath)
            return ifcModel
        except Exception as e:
            log.error(f'Error in migrateIFC: {e}')
