##
#   Validates an IFC files against a list of IDS files
##

import uuid
import ifctester
import ifctester.reporter
import ifcopenshell
import tempfile
from urllib.request import urlopen
 

import long_bg_tasks.task_modules.common_module as common

from pydantic import BaseModel
from model.transform import ValidateIfcAgainstIds_Instruction, ValidateIfcAgainstIds_ResultItem, ValidateIfcAgainstIds_Result

import os

# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter

#
#  Validate the IFC model against the ids               
#
class ValidateIfcAgainstIds():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = ValidateIfcAgainstIds_Instruction(**self.task_dict['ValidateIfcAgainstIds_Instruction'])
            self.sourceFileURL = instruction.sourceFileURL
            self.idsFilesURLs = instruction.idsFilesURLs
            self.resultType = instruction.resultType
            self.BASE_PATH = task_dict['BASE_PATH']
            self.CHECK_RESULTS_PATH = task_dict['CHECK_RESULTS_PATH']
            self.PRINT = task_dict['debug']
            self.start = perf_counter()
            if self.PRINT:
                log.info(f'>>>>> In ValidateIfcAgainstIds.init with instruction: : {instruction}')
        except Exception as e:
            log.error(f'Error in init of ValidateIfcAgainstIds: {e}')
            self.task_dict['st atus'] = 'failed'
            self.task_dict['error'] = f'Error in init of ValidateIfcAgainstIds: {e}'
        
    def validate(self):
        try:
            ifcFilePath = common.setFilePath(self.sourceFileURL, self.BASE_PATH)            
            self.ifcModel = common.getIfcModel(ifcFilePath)
            results = ValidateIfcAgainstIds_Result(
                result = []
            )
            for idsUrl in self.idsFilesURLs:
                idsFilePath = common.setFilePath(idsUrl, self.BASE_PATH)
                ids = common.getFileBytesContent(idsFilePath)
                res = self.validateOneIds(self.ifcModel, ids, self.resultType)
                results.result.append(res)
            self.task_dict['result']['ValidateIfcAgainstIds_Result']=results.dict()
        except Exception as e:
            log.error(f'Error in main_proc of validate_ifc_against_ids: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = str(e)
        finally:
            pass
        return self.task_dict
        
    def validateOneIds(self, model, ids, resultType='bcfzip'):
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".ids") as tmp_file:
                tmp_file.write(ids)
                tmp_file.flush()
                tmp_file_path = tmp_file.name
            ids_data = ifctester.ids.open(tmp_file_path)
            ids_data.validate(model)     
            if resultType == 'json':
                engine = ifctester.reporter.Json(ids_data)
                engine.report()
                result_rel_path = f'{self.CHECK_RESULTS_PATH}{uuid.uuid4()}.json' 
            elif resultType == 'html':
                engine = ifctester.reporter.Html(ids_data)
                engine.report()
                result_rel_path = f'{self.CHECK_RESULTS_PATH}{uuid.uuid4()}.html' 
            else:
                engine = ifctester.reporter.Bcf(ids_data)
                engine.report()
                result_rel_path = f'{self.CHECK_RESULTS_PATH}{uuid.uuid4()}.bcfzip'  
            result_path = f'{self.BASE_PATH}{result_rel_path}'
            result = ValidateIfcAgainstIds_ResultItem(
                isSuccess = engine.results['status'],
                percentChecksPass = engine.results['percent_checks_pass'],
                percentRequirementsPass= engine.results['percent_requirements_pass'],
                percentSpecificationsPass = engine.results['percent_specifications_pass'],
                resultPath = result_rel_path
            )
            engine.to_file(result_path)        
            return result
        except Exception as e:
            log.error(f'Error in validate: {e}')
        finally:
            os.remove(tmp_file_path)

    