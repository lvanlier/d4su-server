#
#   Validates an IFC files against a list of IDS files
#

import uuid
import ifctester
import ifctester.reporter
import ifcopenshell
import tempfile
import urllib.request

import long_bg_tasks.task_modules.common_module as common
   
import os

# Access environment variables as if they came from the actual environment

# Set up the logging
import logging
log = logging.getLogger(__name__)

#
#  Validate the IFC model aginst the ids               
#     
def validate(model, ids, resultType='bcfzip'):
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
            result_rel_path = f'{CHECK_RESULTS_PATH}{uuid.uuid4()}.json' 
        elif resultType == 'html':
            engine = ifctester.reporter.Html(ids_data)
            engine.report()
            result_rel_path = f'{CHECK_RESULTS_PATH}{uuid.uuid4()}.html' 
        else:
            engine = ifctester.reporter.Bcf(ids_data)
            engine.report()
            result_rel_path = f'{CHECK_RESULTS_PATH}{uuid.uuid4()}.bcfzip'  
        result_path = f'{BASE_PATH}{result_rel_path}'
        result = {}
        result['is_success'] = engine.results['status']
        result['percent_checks_pass'] = engine.results['percent_checks_pass']
        result['percent_requirements_pass']= engine.results['percent_requirements_pass']
        result['percent_specifications_pass'] = engine.results['percent_specifications_pass']
        result['result_path'] = result_rel_path
        engine.to_file(result_path)        
        return result
    except Exception as e:
        log.error(f'Error in validate: {e}')
    finally:
        os.remove(tmp_file_path)
    
        
######################
# 
#   Import and process
#
######################

def main_proc(task_dict:dict):
    try:
        sourceFileURL = task_dict['instruction_dict']['sourceFileURL']
        idsFilesURLs = task_dict['instruction_dict']['idsFilesURLs']
        resultType = task_dict['instruction_dict']['resultType']
        global BASE_PATH
        BASE_PATH = task_dict['BASE_PATH']
        global CHECK_RESULTS_PATH
        CHECK_RESULTS_PATH = task_dict['CHECK_RESULTS_PATH']
        global PRINT
        PRINT = task_dict['debug']
        ifcModel = common.getIfcModel(sourceFileURL)
        results = []
        for idsUrl in idsFilesURLs:
            ids = urllib.request.urlopen(idsUrl).read()
            result = validate(ifcModel, ids, resultType)
            results.append(result)
        task_dict['result'] = results
    except Exception as e:
        log.error(f'Error in main_proc of validate_ifc_against_ids: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    finally:
        pass
    return task_dict
