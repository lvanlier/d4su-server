from .celery import app
import long_bg_tasks.task_modules.import_ifc_and_transform_to_json as import_ifc
# DEL import long_bg_tasks.task_modules.filter_IfcJson_old as filter_IfcJson_old
# DL import long_bg_tasks.task_modules.store_ifcjson_in_DB_copy as store_ifcjson_in_DB
import long_bg_tasks.task_modules.notify_result as notify_result
import long_bg_tasks.task_modules.read_model_from_DB_and_write_json as read_model_from_DB_and_write_json
import long_bg_tasks.task_modules.convert_ifcjson_to_ifc as convert_ifcjson_to_ifc
import long_bg_tasks.task_modules.ifc_extract_elements as ifc_extract_elements
import long_bg_tasks.task_modules.ifc_split_storeys as ifc_split_storeys
import long_bg_tasks.task_modules.ifc_convert as ifc_convert
import long_bg_tasks.task_modules.cityjson_to_ifc as cityjson_to_ifc
import long_bg_tasks.task_modules.export_spaces_from_bundle as export_spaces_from_bundle
import long_bg_tasks.task_modules.create_spatialzones_in_bundle as create_spatialzones_in_bundle
import long_bg_tasks.task_modules.extract_spatial_unit as extract_spatial_unit
import long_bg_tasks.task_modules.create_or_update_bundleUnits_copy as create_or_update_bundleUnits
import long_bg_tasks.task_modules.extract_envelope as extract_envelope

from long_bg_tasks.task_modules.validate_ifc_against_ids import ValidateIfcAgainstIds
from long_bg_tasks.task_modules.migrate_ifc_schema import MigrateIfcSchema
from long_bg_tasks.task_modules.tessellate_ifc_elements import TessellateIfcElements
from long_bg_tasks.task_modules.convert_ifc_to_ifcjson import ConvertIfcToIfcJson
from long_bg_tasks.task_modules.filter_ifcjson import FilterIfcJson
from long_bg_tasks.task_modules.store_ifcjson_in_db import StoreIfcJsonInDb
from long_bg_tasks.task_modules.create_or_update_bundleunits import CreateOrUpdateBundleUnits
# from long_bg_tasks.task_modules.import_and_process_ifc import ImportAndProcessIfc MUST NOT EXIST
from long_bg_tasks.task_modules.get_ifcjson_from_db import GetIfcJsonFromDb
from long_bg_tasks.task_modules.convert_ifcjson_to_ifc import ConvertIfcJsonToIfc

import json

@app.task
def getIFCAndCreateIfcJson(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = import_ifc.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def filterIfcJsonOld(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = filter_IfcJson_old.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict)
    return task_dict_dump

@app.task
def storeIfcJsonInDB(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = store_ifcjson_in_DB.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict)
    return task_dict_dump


@app.task
def createOrUpdateBundleUnits(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = create_or_update_bundleUnits.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict)
    return task_dict_dump


@app.task
def readModelFromDBAndWriteJson(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = read_model_from_DB_and_write_json.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict)
    return task_dict_dump


@app.task
def ifcExtractElements(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = ifc_extract_elements.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def ifcSplitStoreys(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = ifc_split_storeys.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def ifcConvert(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['debug'] == True:
        print('inside ifcConvert')
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = ifc_convert.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def cityJson2Ifc(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = cityjson_to_ifc.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def exportSpacesFromBundle(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = export_spaces_from_bundle.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump


@app.task
def createSpatialZonesInBundle(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = create_spatialzones_in_bundle.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump


@app.task
def extractSpatialUnit(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = extract_spatial_unit.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def extractEnvelope(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = extract_envelope.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

#=========== Below are the new tasks ===========

@app.task
def notifyResult(task_dict_dump:str):
    # Get root_id, i.e., the id of the root task;
    # if needed, there are other infos available on task's self.resquest 
    task_id = notifyResult.request.root_id
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] != 'failed':
        task_dict['status'] = 'completed'
        task_dict_dump = json.dumps(task_dict)
    notify_result.main_proc(task_id, task_dict)
    return task_dict_dump

@app.task
def validateIfcAgainstIds(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = ValidateIfcAgainstIds(task_dict)
            task_dict = task.validate()
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def migrateIfcSchema(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = MigrateIfcSchema(task_dict)
            task_dict = task.migrate()
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def tessellateIfcElements(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = TessellateIfcElements(task_dict)
            task_dict = task.tessellate() 
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def convertIfcToIfcJson(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = ConvertIfcToIfcJson(task_dict)
            task_dict = task.convert() 
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def filterIfcJson(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = FilterIfcJson(task_dict)
            task_dict = task.filterJson() 
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def storeIfcJsonInDb(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = StoreIfcJsonInDb(task_dict)
            task_dict = task.store() 
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def createOrUpdateBundleUnits(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = CreateOrUpdateBundleUnits(task_dict)
            task_dict = task.setBundleUnits() 
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def importAndProcessIfc(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = ConvertIfcToIfcJson(task_dict)
            task_dict = task.convert()
            sourceFileURL = task_dict['result']['ConvertIfcToIfcJson_Result']['resultPath']
            if 'FilterIfcJson_Instruction' in task_dict:
                task_dict['FilterIfcJson_Instruction']['sourceFileURL'] = sourceFileURL
                task = FilterIfcJson(task_dict)
                task_dict = task.filterJson()
                sourceFileURL = task_dict['result']['FilterIfcJson_Result']['resultPath']
            task_dict['StoreIfcJsonInDb_Instruction']['sourceFileURL'] = sourceFileURL
            task = StoreIfcJsonInDb(task_dict)
            task_dict = task.store()
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def getIfcJsonFromDb(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = GetIfcJsonFromDb(task_dict)
            task_dict = task.getIfcJson() 
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def convertIfcJsonToIfc(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = ConvertIfcJsonToIfc(task_dict)
            task_dict = task.convert() 
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump