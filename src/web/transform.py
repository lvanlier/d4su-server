from fastapi import APIRouter, HTTPException
import uuid 
import json  

from service import transform as service
from model import transform as model

router = APIRouter(prefix = "/transform")

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