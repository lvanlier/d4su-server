from .celery import app
import long_bg_tasks.task_modules.import_ifc_and_transform_to_json as import_ifc
import long_bg_tasks.task_modules.filter_IfcJson as filter_ifcJson
import long_bg_tasks.task_modules.store_ifcjson_in_DB as store_ifcjson_in_DB
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
import long_bg_tasks.task_modules.create_or_update_bundleUnits as create_or_update_bundleUnits
import long_bg_tasks.task_modules.extract_envelope as extract_envelope

from long_bg_tasks.task_modules.validate_ifc_against_ids import ValidateIfcAgainstIds
from long_bg_tasks.task_modules.migrate_ifc_schema import MigrateIfcSchema

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
def filterIfcJson(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = filter_ifcJson.main_proc(task_dict)
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
def convertIfcJsonToIfc(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task_dict = convert_ifcjson_to_ifc.main_proc(task_dict)
        except Exception as e:
            task_dict['status'] = 'failed'
    task_dict_dump = json.dumps(task_dict)
    return task_dict_dump

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