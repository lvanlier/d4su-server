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
async def read_BundleUnit(bundle_id:str, unit_id:str):
    try:
        bundleUnit = await data.readBundleUnit(bundle_id, unit_id)
        if bundleUnit is None:
            raise HTTPException(status_code=404, detail=f"Bundle Unit not found for bundle_id = {bundle_id} and unit_id = {unit_id}")
        return {"bundleUnit": bundleUnit}
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))