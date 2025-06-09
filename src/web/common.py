from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
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
        return {"spatialUnitId":str(spatialUnitId)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
   
    
@router.delete("/bundle/{bundle_id}", tags=["CRUD"])
async def delete_bundle(bundle_id:str):
    try:
        data.deleteBundleById(bundle_id)
        return {"message": "Deleted bundle with id: " + bundle_id} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
@router.get("/bundle-list/", tags=["CRUD"])
async def read_bundle_list(from_date: str | None = None, to_date: str | None=None, limit:int = 100):
    try:
        bundleList = await data.readBundleList(from_date, to_date, limit)
        if bundleList is None:
            raise HTTPException(status_code=404, detail="No Bundle found")
        return {"bundleList": bundleList}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.get("/bundle/{bundle_id}/", tags=["CRUD"])
async def read_bundle(bundle_id:str):
    try:
        bundle = await data.readBundle(bundle_id)
        if bundle is None:
            raise HTTPException(status_code=404, detail=f"Bundle not found for bundle_id = {bundle_id}")
        return {"bundle": bundle}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.get("/bundle-journal/{bundle_id}", tags=["CRUD"])
async def read_bundle_journal(bundle_id:str, from_date: str | None = None, to_date: str | None=None, limit:int = 100):
    try:
        bundleJournalItems = await data.readBundleJournal(bundle_id, from_date, to_date, limit)
        if bundleJournalItems is None:
            raise HTTPException(status_code=404, detail=f"No Items in bundlejournal for bundle_id = {bundle_id}")
        return {"bundleJournalItems": bundleJournalItems}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.get("/bundle-journal-by-token/{proctoken}", tags=["CRUD"])
async def read_bundle_journal(proctoken:str):
    try:
        bundleJournalItems = await data.readBundleJournalByToken(proctoken)
        if bundleJournalItems is None:
            raise HTTPException(status_code=404, detail=f"No Items in bundlejournal for proctoken = {proctoken}")
        return {"bundleJournalItems": bundleJournalItems}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

    
@router.get("/spatial-unit-list/", tags=["CRUD"])
async def read_spatial_unit_list(from_date: str | None = None, to_date: str | None=None, limit:int = 100):
    try:
        spatialUnitList = await data.readSpatialUnitList(from_date, to_date, limit)
        if spatialUnitList is None:
            raise HTTPException(status_code=404, detail="No Spatial Unit found")
        return {"spatialUnitList": spatialUnitList}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.get("/spatial-unit/{spatial_unit_id}/", tags=["CRUD"])
async def read_spatial_unit(spatial_unit_id:str):
    try:
        spatialUnit = await data.readSpatialUnit(spatial_unit_id)
        if spatialUnit is None:
            raise HTTPException(status_code=404, detail=f"Spatial Unit not found for spatial_unit_id = {spatial_unit_id}")
        return {"spatialUnit": spatialUnit}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
@router.get("/spatial-unit/{spatial_unit_id}/root-bundle-unit-list", tags=["CRUD"])
async def read_spatial_unit_root_bundle_unit_list(spatial_unit_id:str): 
    try:
        spatialUnitRootBundleUnitList = await data.readSpatialUnitRootBundleUnitList(spatial_unit_id)
        if spatialUnitRootBundleUnitList is None:
            raise HTTPException(status_code=404, detail=f"No Bundle found for spatial_unit_id = {spatial_unit_id}")
        return {"spatialUnitRootBundleUnitList": spatialUnitRootBundleUnitList}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.get("/bundle/{bundle_id}/bundle-unit-list", tags=["CRUD"])
async def read_bundle_unit_list(bundle_id:str, unit_type:str | None = None):  
    try:
        bundleUnitList = await data.readBundleUnitList(bundle_id, unit_type)
        if bundleUnitList is None:
            raise HTTPException(status_code=404, detail=f"No Bundle Units found for bundle_id = {bundle_id}")
        return {"bundleUnitList": bundleUnitList}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.get("/bundle/{bundle_id}/unit/{unit_id}/", tags=["CRUD"])
async def read_bundle_unit(bundle_id:str, unit_id:str):
    try:
        bundleUnit = await data.readBundleUnit(bundle_id, unit_id)
        if bundleUnit is None:
            raise HTTPException(status_code=404, detail=f"Bundle Unit not found for bundle_id = {bundle_id} and unit_id = {unit_id}")
        return {"bundleUnit": bundleUnit}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e)) 
    
@router.get("/bundle/{bundle_id}/spatialzone/{sz_id}/properties", tags=["CRUD"])
async def read_bundle_unit_properties(bundle_id:str, sz_id:str, unit_type:str | None = None, unit_id:str | None = None, object_type:str | None = None, propertyset_name: str | None = None, properties_type: str | None = None, limit:int = 100):
    try:
        bundleUnitProperties = await data.readBundleUnitProperties(bundle_id, sz_id, unit_type, unit_id, object_type, propertyset_name, properties_type, limit)
        if bundleUnitProperties is None:
            raise HTTPException(status_code=404, detail=f"Properties not found for bundle_id = {bundle_id} and unit_type = {unit_type} and sz_id = {sz_id} and  and unit_id = {unit_id}")
        return {"bundleUnitProperties": bundleUnitProperties}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
@router.get("/bundle/{bundle_id}/object/{object_id}/", tags=["CRUD"])
async def read_object(bundle_id:str, object_id:str):
    try:
        ifcobject = await data.readObject(bundle_id, object_id)
        if ifcobject is None:
            raise HTTPException(status_code=404, detail=f"Object not found for bundle_id = {bundle_id} and object_id = {object_id}")
        return {"object": ifcobject}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))
    
@router.patch("/bundle/{bundle_id}/object/{object_id}/", tags=["CRUD"])
async def update_object(bundle_id:str, object_id:str, ifcobject:model.UpdateObject):
    try:
        updatedObject = await data.updateObject(bundle_id, object_id, ifcobject)
        if updatedObject is None:
            raise HTTPException(status_code=404, detail=f"Object not found for bundle_id = {bundle_id} and object_id = {object_id}")
        return {"object": updatedObject}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))

class AssignmentOfSpaceRepresentationToSpatialZone(BaseModel):
    bundle_id: str
    spatialZoneId: str
    spaceId: str
    
@router.post("/assign-space-representation-to-spatialzone", tags=["CRUD"])    
async def assign_space_representation_to_spatialzone(assignment: AssignmentOfSpaceRepresentationToSpatialZone):
    try:
        space = await data.readObject(assignment.bundle_id, assignment.spaceId)
        if space is None:
            raise HTTPException(status_code=404, detail=f"Space not found for bundle_id = {assignment.bundle_id} and object_id = {assignment.spaceId}")
        spatialzone = await data.readObject(assignment.bundle_id, assignment.spatialZoneId)
        if spatialzone is None:
            raise HTTPException(status_code=404, detail=f"Spatial Zone not found for bundle_id = {assignment.bundle_id} and spatialZoneId = {assignment.spatialZoneId}")
        sz_dict = spatialzone.dict()   
        sz_dict["elementjson"]["representation"]["representations"] = [{"type": "IfcShapeRepresentation","ref":x} for x in space.representation_ids]
        sz_dict["elementjson"]["objectPlacement"] = space.elementjson["objectPlacement"]
        updateobject = model.UpdateObject(
            representation_ids = space.representation_ids,
            elementjson = sz_dict["elementjson"]
        )
        updatedSpatialZone = await data.updateObject(assignment.bundle_id, assignment.spatialZoneId, updateobject)
        if updatedSpatialZone is None:
            raise HTTPException(status_code=404, detail=f"Spatial Zone not found for bundle_id = {assignment.bundle_id} and spatialZoneId = {assignment.spatialZoneId}")
        return {"spatialZone": updatedSpatialZone}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))