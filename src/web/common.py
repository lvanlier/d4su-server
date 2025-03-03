from fastapi import APIRouter, HTTPException
import uuid 
import json  

from model import common as model
from data import common as data
from service import common as service

router = APIRouter(prefix = "/common")

@router.post("/create-spatial-unit", tags=["CRUD"])
async def create(spatialUnit:model.CreateSpatialUnit):
    try:
        spatialUnitId = await service.createSpatialUnit(spatialUnit)
        return {"message": "Spatial unit created", "spatialUnitId":str(spatialUnitId)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
   
    
@router.delete("/delete/bundle/{id}", tags=["CRUD"])
async def delete_bundle(id:str):
    try:
        data.deleteBundleById(id)
        return {"message": "Delete bundle with id: " + id} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))