
from time import perf_counter
import uuid

# Set up the logging
import logging
log = logging.getLogger(__name__)

import long_bg_tasks.task_modules.common_module as common
from model.transform import IfcExtractElements_Instruction, IfcExtractElements_Result
import ifcpatch

class IfcExtractElements():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = IfcExtractElements_Instruction(**self.task_dict['IfcExtractElements_Instruction'])        
            self.sourceFileURL = instruction.sourceFileURL
            self.elementTypes = instruction.elementTypes
            self.BASE_PATH = task_dict['BASE_PATH']
            self.IFCEXTRACT_PATH = task_dict['IFCEXTRACT_PATH']
            self.PRINT = task_dict['debug']
            if self.PRINT:
                log.info(f'>>>>> In ExtractIfcElements.init with instruction: {instruction}')
        except Exception as e:
            log.error(f'Error in ExtractIfcElements.init: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in ExtractIfcElements.init: {e}'

    def extract(self):
        try:
            ifcFilePath = common.setFilePath(self.sourceFileURL, self.BASE_PATH)
            ifc_file = common.getIfcModel(ifcFilePath)
            # Patch the Ifc file
            ifc_filePath = "-not needed-"
            patch_recipe = "ExtractElements"
            patch_arguments = self.elementTypes
            result_rel_path = f'{self.IFCEXTRACT_PATH}{uuid.uuid4()}.ifc' 
            result_path = f'{self.BASE_PATH}{result_rel_path}'               
            outFilePath = result_path
            self.apply_patch(ifc_filePath, ifc_file, patch_recipe, patch_arguments, outFilePath)
            result = IfcExtractElements_Result(
                resultPath = result_rel_path
            )
            self.task_dict['result']['IfcExtractElements_Result'] = result.dict()
        except Exception as e:
            log.error(f'Error in ExtractIfcElements.extract: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in ExtractIfcElements.extract: {e}'
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
