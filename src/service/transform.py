from pydantic import UUID4
from celery import chain
import json

from model import transform as model
from data.admin import delete_all_p1, delete_all_p2 # only for testing
from data import common as data

from long_bg_tasks.tasks import getIFCAndCreateIfcJson, filterIfcJson, storeIfcJsonInDB, notifyResult
from long_bg_tasks.tasks import readModelFromDBAndWriteJson, convertIfcJsonToIfc, ifcExtractElements, ifcSplitStoreys
from long_bg_tasks.tasks import ifcConvert, cityJson2Ifc, exportSpacesFromBundle, createSpatialZonesInBundle, extractSpatialUnit
from long_bg_tasks.tasks import createOrUpdateBundleUnits

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
import os
load_dotenv()
# Access environment variables as if they came from the actual environment
IFC_JSON_FILES = os.getenv('IFC_JSON_FILES')
IFC_OUT_FILES = os.getenv('IFC_OUT_FILES')
CONVERSION_OUTFILES = os.getenv('CONVERSION_OUTFILES')
TMP_PATH = os.getenv('TMP_PATH')

# Set up the logging
import logging
log = logging.getLogger(__name__)

   
######################
# 
#   Import and process (extract and transform) an IFC file
#
######################

async def import_and_transform_ifc(instruction:model.ImportInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "import_and_transform_ifc"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    withFilter = task_dict['instruction_dict']['withFilter']
    sourceFileURL = task_dict['instruction_dict']['sourceFileURL']
    fileName = sourceFileURL.split("/")[-1].split(".")[0]
    task_dict['ifcJsonFilePath'] = IFC_JSON_FILES+fileName+"_NI.json"
    task_dict['filteredIfcJsonFilePath'] = IFC_JSON_FILES+fileName+"_FIL.json"
    task_dict_dump = json.dumps(task_dict)
    #
    # delete_all_p1() # delete all DB data relating to the bundle (only for testing)
    # delete_all_p2() # delete all DB data relating to the ifcJSON from second pass (only for testing)
    #
    print(f"withFilter: {withFilter}")
    if withFilter == True:
        task_chain = chain(
            getIFCAndCreateIfcJson.s(task_dict_dump),
            filterIfcJson.s(),
            storeIfcJsonInDB.s(),
            createOrUpdateBundleUnits.s(),
            notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
        )
    else:
        task_chain = chain(
            getIFCAndCreateIfcJson.s(task_dict_dump),
            storeIfcJsonInDB.s(),
            createOrUpdateBundleUnits.s(),
            notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
        )
    result = task_chain.delay()
    return

async def get_model_from_db_and_provide_ifc(instruction:model.IfcFromDBInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "get_model_from_db_and_provide_ifc"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    if instruction.byBundleId:
        bundle = data.getBundleById(instruction.byBundleId)
    else:
        bundle = data.getBundleByName(instruction.byBundleName)
        
    task_dict['jsonFilePath'] = TMP_PATH + bundle.name +'.json'
    task_dict['ifcOutFilePath'] = IFC_OUT_FILES+bundle.name+"_OUT.ifc"
    task_dict['bundleId'] = str(bundle.bundle_id)
    #
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        readModelFromDBAndWriteJson.s(task_dict_dump),
        convertIfcJsonToIfc.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

async def ifc_extract_elements(instruction:model.IfcExtractElementsInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "ifc_extract_elements"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    sourceFileURL = task_dict['instruction_dict']['sourceFileURL']
    fileName = sourceFileURL.split("/")[-1].split(".")[0]
    task_dict['ifcOutFilePath'] = CONVERSION_OUTFILES+'IFC/'+fileName+"_EXTRACT.ifc"
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcExtractElements.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

async def ifc_split_storeys(instruction:model.IfcSplitStoreysInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "ifc_split_storeys"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['ifcOutFileDir'] = CONVERSION_OUTFILES+'IFC/'
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcSplitStoreys.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()


async def ifc_convert(instruction:model.IfcConvertInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "ifc_convert"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['outFileDir'] = CONVERSION_OUTFILES
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcConvert.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
    
async def cityjson_to_ifc(instruction:model.CityJson2IfcInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "cityjson_to_ifc"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['ifcOutFileDir'] = CONVERSION_OUTFILES+'IFC/'
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        cityJson2Ifc.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
    
async def export_spaces_from_bundle(instruction:model.ExportSpacesFromBundleInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "export_spaces_from_bundle"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    bundleId = task_dict['instruction_dict']['bundleId']
    bundle = data.getBundleById(bundleId) 
    task_dict['csvFilePath'] = TMP_PATH+'CSV/'+bundle.name+'_spaces.csv'    
    task_dict['jsonFilePath'] = TMP_PATH+'JSON/'+ bundle.name+'_spaces+.json'
    task_dict['debug'] = False
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        exportSpacesFromBundle.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

async def create_spatialzones_in_bundle(instruction:model.CreateSpatialZonesInBundleInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "create_spatialzones_in_bundle"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    bundleId = task_dict['instruction_dict']['bundleId']
    task_dict['bundleId'] = bundleId # for the createOrUpdateBundleUnits task
    bundle = data.getBundleById(bundleId) 
    task_dict['csvFilePath'] = TMP_PATH+'CSV/'+bundle.name+'_spaces.csv'    
    task_dict['debug'] = False
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        createSpatialZonesInBundle.s(task_dict_dump),
        createOrUpdateBundleUnits.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()


async def extract_spatial_unit(instruction:model.ExtractSpatialUnitInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "extract_spatial_unit"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    bundleId = task_dict['instruction_dict']['bundleId']
    elementId = task_dict['instruction_dict']['elementId']
    jsonFilePath = TMP_PATH+bundleId+"_"+elementId+"_EXTRACT.json"
    task_dict['jsonFilePath'] = jsonFilePath
    task_dict['ifcOutFilePath'] = IFC_OUT_FILES+bundleId+"_"+elementId+"_EXTRACT.ifc"
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        extractSpatialUnit.s(task_dict_dump),
        convertIfcJsonToIfc.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()