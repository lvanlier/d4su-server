
from time import perf_counter

# Set up the logging
import logging
log = logging.getLogger(__name__)

import long_bg_tasks.task_modules.common_module as common
import ifcpatch


def apply_patch(ifc_filePath, ifc_file, patch_recipe, patch_arguments, outFilePath, PRINT=False):
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
    ifc_fileName = ifc_filePath.split("/")[-1]
    if PRINT:
        message = f"Patch time: {t_patch} seconds for {ifc_fileName}"
        log.info(message)


######################
# 
#   Import and process
#
######################

def main_proc(task_dict:dict):
    try:
        sourceFileURL = task_dict['instruction_dict']['sourceFileURL']
        patch_arguments = task_dict['instruction_dict']['elementTypes']
        # Get the Ifc file from the source
        ifc_file = common.getIfcModel(sourceFileURL)
        # Patch the Ifc file
        ifc_filePath = "-not needed-"
        patch_recipe = "ExtractElements"
        outFilePath = task_dict['ifcOutFilePath']
        apply_patch(ifc_filePath, ifc_file, patch_recipe, patch_arguments, outFilePath, PRINT=True)
    except Exception as e:
        log.error(f'Error in main_proc of ifc_extract_elements: {e}')
        task_dict['status'] = 'failed'
    return task_dict