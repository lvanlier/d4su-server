from fastapi import APIRouter, HTTPException

from data import admin as data 

router = APIRouter(prefix = "/admin")


@router.post("/import_ifctypes")
async def importIfcTypes():
    try:
        data.importIfcTypes()
        return {"message": "Import IfcTypes"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e)

@router.delete("/drop/all_p1")
async def drop_all_p1():
    try:
        data.drop_all_p1()
        return {"message": "Drop all p1"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)


@router.delete("/drop/all_p2")
async def drop_all_p2():
    try:
        data.drop_all_p2()
        return {"message": "Delete all p2"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)


@router.delete("/delete/all_p1")
async def delete_all_p1():
    try:
        data.delete_all_p1()
        return {"message": "Delete all p1"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)   

@router.delete("/delete/all_p2")
async def delete_all_p2():
    try:
        data.delete_all_p2()
        return {"message": "Delete all p2"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)  
