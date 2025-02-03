
from local_modules.ifcjson.ifc2json4 import IFC2JSON4 
import uuid

import json

from data import transform as data_transform
from data import files as file_store 
import long_bg_tasks.task_modules.common_module as common

import ifcpatch
   
import os
# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()
# Access environment variables as if they came from the actual environment
TMP_PATH = os.getenv('TMP_PATH')

# Set up the logging
import logging
log = logging.getLogger(__name__)

#
#  Migrate the IFC file               
#
def migrateIFC(ifcModel):
    try:
        ifcTempPath = TMP_PATH + str(uuid.uuid4()) +".ifc"
        ifcModel.write(ifcTempPath)
        ifc_filePath = ifcTempPath
        patch_recipe = "Migrate"
        schema = "IFC4"
        patch_arguments = [schema]
        ifcModel = ifcpatch.execute({
            "input": ifc_filePath,
            "file": ifcModel,
            "recipe": patch_recipe,
            "arguments": patch_arguments,
        })
        os.remove(ifc_filePath)
        # this is for test of the migrated file
        ifcTempPath = TMP_PATH + 'mig_'+ str(uuid.uuid4()) +".ifc"
        ifcModel.write(ifcTempPath)
        return ifcModel
    except Exception as e:
        log.error(f'Error in migrateIFC: {e}')
#
#  Tessellate the IFC file               
#
def tesselateIFC(ifcModel):
    try:
        ifcTempPath = TMP_PATH + str(uuid.uuid4()) +".ifc"
        ifcModel.write(ifcTempPath)
        ifc_filePath = ifcTempPath
        patch_recipe = "TessellateElements"
        schema = "IFC4"
        patch_arguments = ["IfcWall,IfcSlab,IfcBeam,IfcColumn,IfcWindow,IfcDoor,IfcSpace", False]
        ifcModel = ifcpatch.execute({
            "input": ifc_filePath,
            "file": ifcModel,
            "recipe": patch_recipe,
            "arguments": patch_arguments,
        })
        os.remove(ifc_filePath)
        # this is for test of the tessellated file
        ifcTempPath = TMP_PATH + 'tes_' + str(uuid.uuid4()) +".ifc"
        ifcModel.write(ifcTempPath)
        return ifcModel
    except Exception as e:
        log.error(f'Error in tessellateIFC: {e}')     
#
#   Transform the Ifc file into an ifcJSON dictionary using IFC2JSON
#
def getIfc2JSON(ifcModel, GEOM):
    try:
        ifcJSON = IFC2JSON4(ifcModel, NO_INVERSE=True, GEOMETRY=GEOM).spf2Json()
        return ifcJSON
    except Exception as e:
        log.error(f'Error in getIfc2JSON: {e}')
    
#
#   Write ifcJSON to a file
#
def writeIfcJSON(ifcJSON, ifcJsonFilePath):
    file_store.write_file(ifcJsonFilePath, json.dumps(ifcJSON, indent=2))


def updateDB(task_dict:dict, header:dict):
    try:
        task_dict = data_transform.logInDB_create_ifcJSON(task_dict, header)
        return task_dict
    except Exception as e:
        log.error(f'Error in updateDB: {e}')
        
######################
# 
#   Import and process
#
######################

def main_proc(task_dict:dict):
    try:
        sourceFileURL = task_dict['instruction_dict']['sourceFileURL']
        migrateSchema = task_dict['instruction_dict']['migrateSchema']
        tessellate = task_dict['instruction_dict']['tessellate']
        # Get the Ifc file from the source
        ifcModel = common.getIfcModel(sourceFileURL)
        input_schema = ifcModel.schema 
        # alos possible:
        # input_schema = ifcModel.wrapped_data.header.file_schema.schema_identifiers[0]
        if input_schema == 'IFC2X3' and migrateSchema == True:
            ifcModel = migrateIFC(ifcModel)
        # Transform the Ifc file into an ifcJSON dictionary using IFC2JSON
        if tessellate == True:
            ifcModel = tesselateIFC(ifcModel)
            # GEOM = 'tessellate'
            GEOM = True
        else:
            GEOM = True
        ifcJSON = getIfc2JSON(ifcModel, GEOM)
        header = dict()
        header["type"] = ifcJSON["type"]
        header["version"] = ifcJSON["version"]
        header["schemaIdentifier"] = ifcJSON["schemaIdentifier"]
        header["originatingSystem"]= ifcJSON["originatingSystem"]
        header["preprocessorVersion"] = ifcJSON["preprocessorVersion"]
        header["timeStamp"] = ifcJSON["timeStamp"]
        rootObjectId = ""
        for item in ifcJSON["data"]:
            if item["type"] == "IfcProject":    
                rootObjectId = item["globalId"]
                rootObjectType = item["type"]
                rootObjectName = item["name"]
                break
        if not rootObjectId:
            raise Exception("No root object found")
        # Write ifcJSON to a file
        ifcJsonFilePath= task_dict['ifcJsonFilePath']
        writeIfcJSON(ifcJSON, ifcJsonFilePath)
        # return what is needed for the next step
        task_dict['rootObjectId'] = rootObjectId
        task_dict['rootObjectType'] = rootObjectType
        task_dict['rootObjectName'] = rootObjectName
        task_dict['ifcJsonFilePath']= ifcJsonFilePath
        task_dict = updateDB(task_dict, header)
    except Exception as e:
        log.error(f'Error in main_proc of import_ifc_and_transform_to_json: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict