##
# Tesselalte the geometry of selected elemnts of the IFC file using IfcPatch
##

import uuid

import long_bg_tasks.task_modules.common_module as common

from pydantic import BaseModel
from model.transform import TessellateIfcElements_Instruction, TessellateIfcElements_Result
import ifcopenshell
import ifcpatch
   
import os

# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter

class TessellateIfcElements():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = TessellateIfcElements_Instruction(**self.task_dict['TessellateIfcElements_Instruction'])        
            self.sourceFileURL = instruction.sourceFileURL
            self.tessellateIfcElements = instruction.tessellateIfcElements
            self.TEMP_PATH = task_dict['TEMP_PATH']
            self.BASE_PATH = task_dict['BASE_PATH']
            self.TESSELLATED_PATH = task_dict['TESSELLATED_PATH']
            self.PRINT = task_dict['debug']
            self.start = perf_counter()
            if self.PRINT:
                log.info(f'>>>>> In TessellateIfcElements.init with instruction: {instruction}')
        except Exception as e:
            log.error(f'Error in init of TessellateIfcElements: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in init of TessellateIfcElements: {e}'
    
    def tessellate(self):
        try:
            ifcFilePath = common.setFilePath(self.sourceFileURL, self.BASE_PATH)
            ifcModel = common.getIfcModel(ifcFilePath)   
            input_schema = ifcModel.schema
            # also possible:
            # input_schema = ifcModel.wrapped_data.header.file_schema.schema_identifiers[0]
            if input_schema == 'IFC2X3':
                message = f'IFC file in {input_schema} schema and should be migrated to IFC4 before tessellation'
                log.info(message)
                self.task_dict['status'] = 'failed'
                self.task_dict['error'] = message
            else:
                ifcModel = self.tessellateIFC(ifcModel)
                result_rel_path = f'{self.TESSELLATED_PATH}{uuid.uuid4()}.ifc' 
                result_path = f'{self.BASE_PATH}{result_rel_path}'
                ifcModel.write(result_path)
                result = TessellateIfcElements_Result(
                    resultPath = result_rel_path,
                    runtime = round(perf_counter() - self.start, 2)
                )
                self.task_dict['result']['TessellateIfcElements_Result'] = result.dict()            
        except Exception as e:
            log.error(f'Error in main_proc of TessellateIfcElements: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = str(e)
        finally:
            pass
        return self.task_dict
    
    def tessellateIFC(self, ifcModel):
        try:
            ifcTempPath = self.TEMP_PATH + str(uuid.uuid4()) +".ifc"
            ifcModel.write(ifcTempPath)
            ifc_filePath = ifcTempPath
            patch_recipe = "TessellateElements"
            schema = "IFC4"
            elementTypes = self.tessellateIfcElements.elementTypes[0]
            forcedFacetedBREP = self.tessellateIfcElements.forcedFacetedBREP
            patch_arguments = [elementTypes, forcedFacetedBREP]
            ifcModel = ifcpatch.execute({
                "input": ifc_filePath,
                "file": ifcModel,
                "recipe": patch_recipe,
                "arguments": patch_arguments,
            })
            os.remove(ifc_filePath)
            return ifcModel
        except Exception as e:
            log.error(f'Error in tessellateIFC: {e}')
