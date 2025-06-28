from pydantic import UUID4
from celery import chain, chord
import json

from model import transform as model
from model import task as task
from data.admin import delete_all_p1, delete_all_p2 # only for testing
from data import common as data

from long_bg_tasks.tasks import (
    journalize,
    notifyResult, 
    journalizeAndNotifyAllResults,
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
    exportSpacesFromBundle, 
    createSpatialZonesInBundle,
    extractEnvelope, 
    ifcConvert, 
    cityJsonToIfc,
    populateBundleUnitProperties 
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
SPACES_PATH = os.getenv('SPACES_PATH')
IFCCONVERT_WD = os.getenv('IFCCONVERT_WD')
EXPORT_PATH = os.getenv('EXPORT_PATH')
UPLOAD_PATH = os.getenv('UPLOAD_PATH')


# Set up the logging
import logging
log = logging.getLogger(__name__)

def isDebug(name:str):
    if name in (
        import_and_process_ifc.__name__,
        convert_ifc_to_ifcjson.__name__,
        filter_ifcjson.__name__,
        store_ifcjson_in_db.__name__,
        extract_spatial_unit.__name__,
        extract_all_spatial_units.__name__, 
        export_spaces_from_bundle.__name__,
        create_spatialzones_in_bundle.__name__,
        import_and_process_ifc.__name__,
        extract_envelope.__name__,
        ifc_convert.__name__,
        cityjson_to_ifc.__name__,
        populate_bundleunitproperties.__name__,
        tessellate_ifc_elements.__name__
    ):
        return True 
    return False
  
#
# Validate the IFC model against the Information Delivery Specification (IDS)
#
async def validate_ifc_against_ids(instruction:model.ValidateIfcAgainstIds_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "validate_ifc_against_ids"
    task_dict[model.ValidateIfcAgainstIds_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['CHECK_RESULTS_PATH'] = CHECK_RESULTS_PATH
    task_dict['debug'] = isDebug(validate_ifc_against_ids.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        validateIfcAgainstIds.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Migrate an IFC file to a different schema (IFC2X3 -> IFC4)
#  
async def migrate_ifc_schema(instruction:model.MigrateIfcSchema_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "migrate_ifc_schema"
    task_dict[model.MigrateIfcSchema_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['MIGRATED_PATH'] = MIGRATED_PATH
    task_dict['debug'] = isDebug(migrate_ifc_schema.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        migrateIfcSchema.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Tessellate selected product elements in an IFC 
#  
async def tessellate_ifc_elements(instruction:model.TessellateIfcElements_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "tessellate_ifc_elements"
    task_dict[model.TessellateIfcElements_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['TESSELLATED_PATH'] = TESSELLATED_PATH
    task_dict['debug'] = isDebug(tessellate_ifc_elements.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        tessellateIfcElements.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Convert an IFC to IFCJSON with IFC2JSON
#
async def convert_ifc_to_ifcjson(instruction:model.ConvertIfcToIfcJson_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "convert_ifc_to_ifcjson"
    task_dict[model.ConvertIfcToIfcJson_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = isDebug(convert_ifc_to_ifcjson.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        convertIfcToIfcJson.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Filter an IFCJSON 
#
async def filter_ifcjson(instruction:model.FilterIfcJson_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "filter_ifcjson"
    task_dict[model.FilterIfcJson_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = isDebug(filter_ifcjson.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        filterIfcJson.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
# Store an IFCJSON in the database
#
async def store_ifcjson_in_db(instruction:model.StoreIfcJsonInDb_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "store_ifcjson_in_db"
    task_dict[model.StoreIfcJsonInDb_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = isDebug(store_ifcjson_in_db.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        storeIfcJsonInDb.s(task_dict_dump),
        createOrUpdateBundleUnits.s(),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
    
#
#   Import and IFC file, convert it to an IfcJSON and store it in the Database 
#
async def import_and_process_ifc(instruction:model.ImportAndProcessIfc_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "import_and_process_ifc"
    task_dict[model.ConvertIfcToIfcJson_Instruction.__name__] = instruction.source.dict()
    if instruction.filter and instruction.filter not in [None, '']:
        task_dict[model.FilterIfcJson_Instruction.__name__] = instruction.filter.dict()
    task_dict[model.StoreIfcJsonInDb_Instruction.__name__] = instruction.store.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = isDebug(import_and_process_ifc.__name__)
    task_dict_dump = json.dumps(task_dict)
    task_chain = chain(
        importAndProcessIfc.s(task_dict_dump),
        journalize.s(),
        createOrUpdateBundleUnits.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
    return
#
#   Get the IfcJSON from the database
#
async def get_ifcjson_from_db(instruction:model.GetIfcJsonFromDb_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "get_ifcjson_from_db"
    task_dict[model.GetIfcJsonFromDb_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['debug'] = isDebug(get_ifcjson_from_db.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        getIfcJsonFromDb.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Convert the IFCJSON to IFC
#
async def convert_ifcjson_to_ifc(instruction:model.ConvertIfcJsonToIfc_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "convert_ifcjson_to_ifc"
    task_dict[model.ConvertIfcJsonToIfc_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['JSON2IFC_PATH'] = JSON2IFC_PATH
    task_dict['debug'] = isDebug(convert_ifcjson_to_ifc.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        convertIfcJsonToIfc.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Extract elements from an IFC using IfcPatch
#
async def ifc_extract_elements(instruction:model.IfcExtractElements_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "ifc_extract_elements"
    task_dict[model.IfcExtractElements_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCEXTRACT_PATH'] = IFCEXTRACT_PATH
    task_dict['debug'] = isDebug(ifc_extract_elements.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcExtractElements.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Split an IFC file by building storeys using the ifcpatch library
#
async def ifc_split_storeys(instruction:model.IfcSplitStoreys_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "ifc_split_storeys"
    task_dict[model.IfcSplitStoreys_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCEXTRACT_PATH'] = IFCEXTRACT_PATH
    task_dict['debug'] = isDebug(ifc_split_storeys.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcSplitStoreys.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Extract a Spatial Unit (actually, a 'bundle unit' that can be linked to a Spatial Unit) 
#   from the database and produce an IfcJSON; for simplicity an IFC file
#   is also produced if requested in the instruction
#
async def extract_spatial_unit(instruction:model.ExtractSpatialUnit_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "extract_spatial_unit"
    task_dict[model.ExtractSpatialUnit_Instruction.__name__] = instruction.dict()
    # added for the conversion to IFC when requested
    task_dict[model.ConvertIfcJsonToIfc_Instruction.__name__] = model.ConvertIfcJsonToIfc_Instruction(
        sourceFileURL = ''
    ).dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['JSON2IFC_PATH'] = JSON2IFC_PATH
    task_dict['debug'] = isDebug(extract_spatial_unit.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        extractSpatialUnit.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Extract all Spatial Units (i.e. all 'bundle units' from a bundle and with a given type) 
#   from the database and produce an IfcJSON; for simplicity an IFC files
#   are also produced if requested in the instruction
#
async def extract_all_spatial_units(instruction:model.ExtractAllSpatialUnits_Instruction, procToken:UUID4):
    bundle_id = instruction.bundleId
    useRepresentationsCache = instruction.useRepresentationsCache
    unit_type = instruction.elementType
    includeRelationshipTypes = instruction.includeRelationshipTypes
    withIFC = instruction.withIFC
    bundleUnitList = await data.readBundleUnitList(bundle_id, unit_type)
    #
    # the chord will be used to run the tasks in parallel
    # the header will contain the list of the signatures of all tasks to be run in parallell
    # and the callback will be used to journalize the results and notify the results
    # 
    chord_header = []
    for i in range(len(bundleUnitList)):
        # create a new task_dict for each task
        task_dict = task.task_dict().dict()
        # add the general (all) info to the task_dict
        task_dict['taskAllName'] = "extract_all_spatial_units"
        task_dict['taskAll_instruction_className'] = model.ExtractAllSpatialUnits_Instruction.__name__
        task_dict['taskAllInstruction'] = instruction.dict()
        task_dict['allDebug']=isDebug(extract_all_spatial_units.__name__)
        # create an Instruction for the task
        task_dict['taskName'] = "extract_spatial_unit"
        ExtractSpatialUnit_Instruction = model.ExtractSpatialUnit_Instruction(
            bundleId = str(bundleUnitList[i]['bundle_id']),
            useRepresentationsCache = useRepresentationsCache,
            elementType = bundleUnitList[i]['unit_type'],
            elementId = str(bundleUnitList[i]['unit_id']),
            withIFC = withIFC,
            includeRelationshipTypes = includeRelationshipTypes
        )
        task_dict[model.ExtractSpatialUnit_Instruction.__name__] = ExtractSpatialUnit_Instruction.dict()
        # added for the conversion to IFC when requested
        task_dict[model.ConvertIfcJsonToIfc_Instruction.__name__] = model.ConvertIfcJsonToIfc_Instruction(
            sourceFileURL = ''
        ).dict()
        task_dict['procToken_str'] = str(procToken)
        task_dict['BASE_PATH'] = BASE_PATH
        task_dict['IFCJSON_PATH'] = IFCJSON_PATH
        task_dict['TEMP_PATH'] = TMP_PATH
        task_dict['JSON2IFC_PATH'] = JSON2IFC_PATH
        task_dict['debug'] = task_dict['allDebug']
        task_dict_dump = json.dumps(task_dict)
        log.info(f"task_dict_dump: {task_dict_dump}")
        chord_header.append(extractSpatialUnit.s(task_dict_dump))
    chord_callback = journalizeAndNotifyAllResults.s()
    task_chord = chord(chord_header)(chord_callback)
     
#
#   Export spaces from a bundle
#
async def export_spaces_from_bundle(instruction:model.ExportSpacesFromBundle_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "export_spaces_from_bundle"
    task_dict[model.ExportSpacesFromBundle_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['SPACES_PATH'] = SPACES_PATH
    task_dict['debug'] = isDebug(export_spaces_from_bundle.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        exportSpacesFromBundle.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
#
#   Create spatial zones in a bundle
#
async def create_spatialzones_in_bundle(instruction:model.CreateSpatialZonesInBundle_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "create_spatialzones_in_bundle"
    task_dict[model.CreateSpatialZonesInBundle_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['SPACES_PATH'] = SPACES_PATH
    task_dict['debug'] = isDebug(create_spatialzones_in_bundle.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        createSpatialZonesInBundle.s(task_dict_dump),
        journalize.s(),
        createOrUpdateBundleUnits.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Extract the envelope
#
async def extract_envelope(instruction:model.ExtractEnvelope_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "extract_envelope"
    task_dict[model.ExtractEnvelope_Instruction.__name__] = instruction.dict()
    # added for the conversion to IFC when requested
    task_dict[model.ConvertIfcJsonToIfc_Instruction.__name__] = model.ConvertIfcJsonToIfc_Instruction(
        sourceFileURL = ''
    ).dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['IFCJSON_PATH'] = IFCJSON_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['JSON2IFC_PATH'] = JSON2IFC_PATH
    task_dict['debug'] = isDebug(extract_envelope.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        extractEnvelope.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Convert an IFC file to another format: glTF, COLLADA, CityJSON
#
async def ifc_convert(instruction:model.IfcConvert_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "ifc_convert"
    task_dict[model.IfcConvert_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['IFCCONVERT_WD'] = IFCCONVERT_WD
    task_dict['CONVERSION_OUTFILES'] = CONVERSION_OUTFILES
    task_dict['debug'] = isDebug(ifc_convert.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcConvert.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Convert a CityJSON file to IFC
#    
async def cityjson_to_ifc(instruction:model.CityJsonToIfc_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "cityjson_to_ifc"
    task_dict[model.CityJsonToIfc_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['CONVERSION_OUTFILES'] = CONVERSION_OUTFILES
    task_dict['debug'] = isDebug(cityjson_to_ifc.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        cityJsonToIfc.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()

#
#   Populate the bundleunitproperties table to support the export of spatial unit properties
#    
#   Populate the bundleunitproperties table to support the export of spatial unit properties
async def populate_bundleunitproperties(instruction:model.PopulateBundleUnitProperties_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "populate_bundleunitproperties"
    task_dict[model.PopulateBundleUnitProperties_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['EXPORT_PATH'] = EXPORT_PATH
    task_dict['debug'] = isDebug(populate_bundleunitproperties.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        populateBundleUnitProperties.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
