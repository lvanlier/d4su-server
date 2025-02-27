from pydantic import UUID4
from celery import chain
import json

from model import transform as model
from data.admin import delete_all_p1, delete_all_p2 # only for testing
from data import common as data

from long_bg_tasks.tasks import (
    notifyResult, 
    validateIfcAgainstIds, 
    migrateIfcSchema, 
    tessellateIfcElements, 
    convertIfcToIfcJson, 
    filterIfcJson, 
    storeIfcJsonInDb, 
    createOrUpdateBundleUnits, 
    importAndProcessIfc,
    getIfcJsonFromDb, 
    convertIfcJsonToIfc,
    ifcExtractElements,
    ifcSplitStoreys,
    extractSpatialUnit,

    ifcConvert, 
    cityJson2Ifc, 
    exportSpacesFromBundle, 
    createSpatialZonesInBundle,
    extractEnvelope 
)

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
import os
load_dotenv()
# Access environment variables as if they came from the actual environment
IFC_JSON_FILES = os.getenv('IFC_JSON_FILES')
IFC_OUT_FILES = os.getenv('IFC_OUT_FILES')
CONVERSION_OUTFILES = os.getenv('CONVERSION_OUTFILES')
TMP_PATH = os.getenv('TMP_PATH')
BASE_PATH = os.getenv('BASE_PATH')
CHECK_RESULTS_PATH = os.getenv('CHECK_RESULTS_PATH')
MIGRATED_PATH = os.getenv('MIGRATED_PATH')
TESSELLATED_PATH = os.getenv('TESSELLATED_PATH')
IFCJSON_PATH = os.getenv('IFCJSON_PATH')
JSON2IFC_PATH = os.getenv('JSON2IFC_PATH')
IFCEXTRACT_PATH = os.getenv('IFCEXTRACT_PATH')

# Set up the logging
import logging
log = logging.getLogger(__name__)



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
    task_dict['debug'] = False
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


    
async def extract_envelope(instruction:model.ExtractEnvelopeInstruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "extract_envelope"
    task_dict['instruction_dict'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    bundleId = task_dict['instruction_dict']['bundleId']
    task_dict['csvFilePath'] = TMP_PATH+'CSV/'+bundleId+'_external_elements_in_bundle.csv'
    task_dict['jsonFilePath'] = TMP_PATH+'JSON/'+bundleId+'_external_elements_in_bundle.json'
    task_dict['ifcOutFilePath'] = IFC_OUT_FILES+bundleId+'_external_elements_in_bundle.ifc'
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        extractEnvelope.s(task_dict_dump),
        convertIfcJsonToIfc.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

# ============ down here ===============================================
  
#
# Validate the IFC model against the Information Delivery Specification (IDS)
#
async def validate_ifc_against_ids(instruction:model.ValidateIfcAgainstIds_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "validate_ifc_against_ids"
    task_dict['ValidateIfcAgainstIds_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['CHECK_RESULTS_PATH'] = CHECK_RESULTS_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        validateIfcAgainstIds.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Migrate an IFC file to a different schema (IFC2X3 -> IFC4)
#  
async def migrate_ifc_schema(instruction:model.MigrateIfcSchema_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "migrate_ifc_schema"
    task_dict['MigrateIfcSchema_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['MIGRATED_PATH'] = MIGRATED_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        migrateIfcSchema.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Tessellate selected product elements in an IFC 
#  
async def tessellate_ifc_elements(instruction:model.TessellateIfcElements_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "tessellate_ifc_elements"
    task_dict['TessellateIfcElements_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['TESSELLATED_PATH'] = TESSELLATED_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        tessellateIfcElements.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Convert an IFC to IFCJSON with IFC2JSON
#
async def convert_ifc_to_ifcjson(instruction:model.ConvertIfcToIfcJson_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "convert_ifc_to_ifcjson"
    task_dict['ConvertIfcToIfcJson_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        convertIfcToIfcJson.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Filter an IFCJSON 
#
async def filter_ifcjson(instruction:model.FilterIfcJson_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "filter_ifcjson"
    task_dict['FilterIfcJson_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        filterIfcJson.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Store an IFCJSON in the database
#
async def store_ifcjson_in_db(instruction:model.StoreIfcJsonInDb_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "store_ifcjson_in_db"
    task_dict['StoreIfcJsonInDb_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        storeIfcJsonInDb.s(task_dict_dump),
        createOrUpdateBundleUnits.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
    
#
#   Import and IFC file, convert it to an IfcJSON and store it in the Database 
#
async def import_and_process_ifc(instruction:model.ImportAndProcessIfc_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "import_and_process_ifc"
    task_dict['ConvertIfcToIfcJson_Instruction'] = instruction.source.dict()
    if instruction.filter and instruction.filter not in [None, '']:
        task_dict['FilterIfcJson_Instruction'] = instruction.filter.dict()
    task_dict['Store_Instruction'] = instruction.store.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    task_chain = chain(
        importAndProcessIfc.s(task_dict_dump),
        createOrUpdateBundleUnits.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
    return
#
#   Get the IfcJSON from the database
#
async def get_ifcjson_from_db(instruction:model.GetIfcJsonFromDb_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "get_ifcjson_from_db"
    task_dict['GetIfcJsonFromDb_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        getIfcJsonFromDb.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Convert the IFCJSON to IFC
#
async def convert_ifcjson_to_ifc(instruction:model.ConvertIfcJsonToIfc_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "convert_ifcjson_to_ifc"
    task_dict['ConvertIfcJsonToIfc_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['JSON2IFC_PATH'] = JSON2IFC_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        convertIfcJsonToIfc.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Extract elements from an IFC using IfcPatch
#
async def ifc_extract_elements(instruction:model.IfcExtractElements_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "ifc_extract_elements"
    task_dict['IfcExtractElements_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCEXTRACT_PATH'] = IFCEXTRACT_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcExtractElements.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Split an IFC file by building storeys using the ifcpatch library
#
async def ifc_split_storeys(instruction:model.IfcSplitStoreys_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "ifc_split_storeys"
    task_dict['IfcSplitStoreys_Instruction'] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCEXTRACT_PATH'] = IFCEXTRACT_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcSplitStoreys.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Extract a Spatial Unit from the database and produce an IfcJSON; for simplicity an IFC file
#   is also produced
#
async def extract_spatial_unit(instruction:model.ExtractSpatialUnit_Instruction, procToken:UUID4):
    task_dict = model.task_dict
    task_dict['taskName'] = "extract_spatial_unit"
    task_dict['ExtractSpatialUnit_Instruction'] = instruction.dict()
    task_dict['ConvertIfcJsonToIfc_Instruction'] = model.ConvertIfcJsonToIfc_Instruction(
        sourceFileURL = ''
    ).dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['JSON2IFC_PATH'] = JSON2IFC_PATH
    task_dict['debug'] = True
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        extractSpatialUnit.s(task_dict_dump),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

