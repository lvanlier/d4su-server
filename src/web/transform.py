from fastapi import APIRouter, HTTPException
import uuid 
import json  

from service import transform as service
from model import transform as model

router = APIRouter(prefix = "/transform")

#
# Convert an IFC to IFCJSON with IFC2JSON
#
@router.post("/convert-ifc-to-ifcjson")
async def convert_ifc_to_ifcjson(instruction:model.ConvertIfcToIfcJson_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.convert_ifc_to_ifcjson(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Migrate an IFC file to a different schema
#
@router.post("/migrate-ifc-schema")
async def migrate_ifc_schema(instruction:model.MigrateIfcSchema_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.migrate_ifc_schema(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Tessallate an IFC
#
@router.post("/tessellate-ifc")
async def tessellate_ifc(instruction:model.TessellateIfc_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.tesselate_ifc(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Filter an IFCJSON
#
@router.post("/filter-ifcjson")
async def filter_ifcjson(instruction:model.FilterIfcJson_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.filter_ifcjson(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
#
# Store an IFCJSON in the database
#
@router.post("/store-ifcjson-in-db")
async def store_ifcjson_in_db(instruction:model.StoreIfcJsonInDb_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.store_ifcjson_in_db(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.post("/ifc-import")
async def ifc_import(instruction:model.ImportInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.import_and_transform_ifc(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
   
@router.post("/ifc-from-db")
async def ifc_from_db(instruction:model.IfcFromDBInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.get_model_from_db_and_provide_ifc(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
@router.post("/ifc-extract-elements")
async def ifc_extract_elements(instruction:model.IfcExtractElementsInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.ifc_extract_elements(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
@router.post("/ifc-split-storeys")
async def ifc_split_storeys(instruction:model.IfcSplitStoreysInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.ifc_split_storeys(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
@router.post("/ifc-convert")
async def ifc_convert(instruction:model.IfcConvertInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.ifc_convert(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
    
@router.post("/cityjson2ifc")
async def cityjson2ifc(instruction:model.CityJson2IfcInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.cityjson_to_ifc(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.post("/export-spaces-from-bundle")
async def export_spaces_from_bundle(instruction:model.ExportSpacesFromBundleInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.export_spaces_from_bundle(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.post("/create-spatialzones-in-bundle")
async def create_spatialzones_for_bundle(instruction:model.CreateSpatialZonesInBundleInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.create_spatialzones_in_bundle(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.post("/extract-spatial-unit")
async def extract_spatial_unit(instruction:model.ExtractSpatialUnitInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.extract_spatial_unit(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
@router.post("/extract-envelope")
async def extract_envelope(instruction:model.ExtractEnvelopeInstruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.extract_envelope(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

#
# Validate the IFC model against the Information Delivery Specification (IDS)
#  
@router.post("/validate-ifc-against-ids")
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
@router.post("/migrate-ifc-schema")
async def migrate_ifc_schema(instruction:model.MigrateIfcSchema_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.migrate_ifc_schema(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
