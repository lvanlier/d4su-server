from fastapi import APIRouter, HTTPException
import uuid 
import json  

from service import transform as service
from model import transform as model

router = APIRouter(prefix = "/transform")


#
# Validate the IFC model against the Information Delivery Specification (IDS)
#  
@router.post("/validate-ifc-against-ids", tags=["Check"])
async def validate_ifc_against_ids(instruction:model.ValidateIfcAgainstIds_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.validate_ifc_against_ids(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
#
# Migrate the IFC model from IFC2X3 to IFC4
#  
@router.post("/migrate-ifc-schema", tags=["Convert"])
async def migrate_ifc_schema(instruction:model.MigrateIfcSchema_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.migrate_ifc_schema(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Tessellate selected IFC Elements
#  
@router.post("/tessellate-ifc-elements", tags=["Geometry"])
async def tessellate_ifc_elements(instruction:model.TessellateIfcElements_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.tessellate_ifc_elements(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Convert an IFC to IFCJSON with IFC2JSON
#
@router.post("/convert-ifc-to-ifcjson", tags=["Convert"])
async def convert_ifc_to_ifcjson(instruction:model.ConvertIfcToIfcJson_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.convert_ifc_to_ifcjson(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Filter an IfcJSON
#
@router.post("/filter-ifcjson", tags=["Filter"])
async def filter_ifcjson(instruction:model.FilterIfcJson_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.filter_ifcjson(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Store an IfcJSON in  the PostgreSQL database
#
@router.post("/store-ifcjson-in-db", tags=["Store"])
async def store_ifcjson_in_db(instruction:model.StoreIfcJsonInDb_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.store_ifcjson_in_db(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Import an Ifc, convert it to an IfcJSON, filter it (or not) and store the data in the Dabase
#
@router.post("/import-and-process-ifc", tags=["Workflow"])
async def import_and_process_ifc(instruction:model.ImportAndProcessIfc_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.import_and_process_ifc(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Read the IfcJSON from the DB for further processing
#
@router.post("/get-ifcjson-from-db", tags=["Extract"])
async def get_ifcjson_from_db(instruction:model.GetIfcJsonFromDb_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.get_ifcjson_from_db(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Convert the IfcJSON to an IFC File
#
@router.post("/convert-ifcjson-to-ifc", tags=["Convert"])
async def convert_ifcjson_to_ifc(instruction:model.ConvertIfcJsonToIfc_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.convert_ifcjson_to_ifc(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Extract elements from an IFC using IfcPatch
#
@router.post("/ifc-extract-elements", tags=["Extract"])
async def ifc_extract_elements(instruction:model.IfcExtractElements_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.ifc_extract_elements(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Extract storeys from an IFC using IfcPatch Split Storeys
#
@router.post("/ifc-split-storeys", tags=["Extract"])
async def ifc_split_storeys(instruction:model.IfcSplitStoreys_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.ifc_split_storeys(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Extract a Spatial Unit from the database and produce an IfcJSON
#
@router.post("/extract-spatial-unit", tags=["Extract"])
async def extract_spatial_unit(instruction:model.ExtractSpatialUnit_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.extract_spatial_unit(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
#   Export spaces from a bundle as a csv or json file
#  
@router.post("/export-spaces-from-bundle", tags=["Export"])
async def export_spaces_from_bundle(instruction:model.ExportSpacesFromBundle_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.export_spaces_from_bundle(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
#   Create spatial zones in a bundle
#
@router.post("/create-spatialzones-in-bundle", tags=["Store"])
async def create_spatialzones_in_bundle(instruction:model.CreateSpatialZonesInBundle_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.create_spatialzones_in_bundle(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
#   Extract the envelope 
#
@router.post("/extract-envelope", tags=["Extract"])
async def extract_envelope(instruction:model.ExtractEnvelope_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.extract_envelope(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
#   Convert to to glTF, COLLADA or CityJSON
#    
@router.post("/ifc-convert", tags=["Convert", "Host"])
async def ifc_convert(instruction:model.IfcConvert_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.ifc_convert(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
#
#   Convert from CityJSON to IFC
#      
@router.post("/cityjson-to-ifc", tags=["Convert", "Host"])
async def cityjson_to_ifc(instruction:model.CityJsonToIfc_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.cityjson_to_ifc(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
