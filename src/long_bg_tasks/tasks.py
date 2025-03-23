from .celery import app

from long_bg_tasks.task_modules.notify_result import NotifyResult
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
from long_bg_tasks.task_modules.ifc_extract_elements import IfcExtractElements
from long_bg_tasks.task_modules.ifc_split_storeys import IfcSplitStoreys
from long_bg_tasks.task_modules.extract_spatial_unit import ExtractSpatialUnit  
from long_bg_tasks.task_modules.export_spaces_from_bundle import ExportSpacesFromBundle
from long_bg_tasks.task_modules.create_spatialzones_in_bundle import CreateSpatialZonesInBundle
from long_bg_tasks.task_modules.extract_envelope import ExtractEnvelope
from long_bg_tasks.task_modules.ifc_convert import IfcConvert
from long_bg_tasks.task_modules.cityjson_to_ifc import CityJsonToIfc
from long_bg_tasks.task_modules.populate_bundleunitproperties import PopulateBundleUnitProperties


import json


@app.task
def notifyResult(task_dict_dump:str):
    # Get root_id, i.e., the id of the root task;
    # if needed, there are other infos available on task's self.resquest 
    task_id = notifyResult.request.root_id
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] != 'failed':
        task_dict['status'] = 'completed'
        task_dict_dump = json.dumps(task_dict)
    task = NotifyResult(task_id, task_dict)
    task.notify()
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
            task_dict['error'] += f'Error in validateIfcAgainstIds: {e}'
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
            task_dict['error'] += f'Error in migrateIfcSchema: {e}'
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
            task_dict['error'] += f'Error in tessellateIfcElements: {e}'
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
            task_dict['error'] += f'Error in convertIfcToIfcJson: {e}'
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
            task_dict['error'] += f'Error in filterIfcJson: {e}'
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
            task_dict['error'] += f'Error in storeIfcJsonInDb: {e}'
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
            task_dict['error'] += f'Error in createOrUpdateBundleUnits: {e}'
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
            task_dict['error'] += f'Error in importAndProcessIfc: {e}'
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
            task_dict['error'] += f'Error in getIfcJsonFromDb: {e}'
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
            task_dict['error'] += f'Error in convertIfcJsonToIfc: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def ifcExtractElements(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = IfcExtractElements(task_dict)
            task_dict = task.extract()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in ifcExtractElements: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def ifcSplitStoreys(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = IfcSplitStoreys(task_dict)
            task_dict = task.splitStoreys()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in ifcSplitStoreys: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump


@app.task
def extractSpatialUnit(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = ExtractSpatialUnit(task_dict)
            task_dict = task.extract()
            withIFC = task_dict['ExtractSpatialUnit_Instruction']['withIFC']
            if withIFC == True:
                sourceFileURL = task_dict['result']['ExtractSpatialUnit_Result']['resultPath']
                task_dict['ConvertIfcJsonToIfc_Instruction']['sourceFileURL'] = sourceFileURL
                task = ConvertIfcJsonToIfc(task_dict)
                task_dict = task.convert()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in extractSpatialUnit: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def exportSpacesFromBundle(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = ExportSpacesFromBundle(task_dict)
            task_dict = task.export()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in exportSpacesFromBundle: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def createSpatialZonesInBundle(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = CreateSpatialZonesInBundle(task_dict)
            task_dict = task.createSpatialZones()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in createSpatialZonesInBundle: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def extractEnvelope(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = ExtractEnvelope(task_dict)
            task_dict = task.extract()
            withIFC = task_dict['ExtractEnvelope_Instruction']['withIFC']
            if withIFC == True:
                sourceFileURL = task_dict['result']['ExtractEnvelope_Result']['resultPath']
                task_dict['ConvertIfcJsonToIfc_Instruction']['sourceFileURL'] = sourceFileURL
                task = ConvertIfcJsonToIfc(task_dict)
                task_dict = task.convert()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in extractEnvelope: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def ifcConvert(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = IfcConvert(task_dict)
            task_dict = task.convert()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in ifcConvert: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def cityJsonToIfc(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = CityJsonToIfc(task_dict)
            task_dict = task.convert()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in cityJson2Ifc: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump

@app.task
def populateBundleUnitProperties(task_dict_dump:str):
    task_dict = json.loads(task_dict_dump)
    if task_dict['status'] == 'failed':
        return task_dict_dump
    else:
        try:
            task = PopulateBundleUnitProperties(task_dict)
            task_dict = task.populate()
        except Exception as e:
            task_dict['status'] = 'failed'
            task_dict['error'] += f'Error in populateBundleUnitProperties: {e}'
    task_dict_dump = json.dumps(task_dict) 
    return task_dict_dump
