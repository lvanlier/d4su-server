from fastapi import APIRouter, HTTPException

from data import admin as data 
from service import admin as service
from model import admin as model

import uuid

router = APIRouter(prefix = "/admin")


@router.post("/import_ifctypes", tags=["Admin"])
async def importIfcTypes():
    try:
        data.importIfcTypes()
        return {"message": "Import IfcTypes"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e)

@router.delete("/drop/all_p1", tags=["Admin"])
async def drop_all_p1():
    try:
        data.drop_all_p1()
        return {"message": "Drop all p1"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)


@router.delete("/drop/all_p2", tags=["Admin"])
async def drop_all_p2():
    try:
        data.drop_all_p2()
        return {"message": "Delete all p2"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)


@router.delete("/delete/all_p1", tags=["Admin"])
async def delete_all_p1():
    try:
        data.delete_all_p1()
        return {"message": "Delete all p1"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)   

@router.delete("/delete/all_p2", tags=["Admin"])
async def delete_all_p2():
    try:
        data.delete_all_p2()
        return {"message": "Delete all p2"} 
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)

@router.post("/ifcfilequery", tags=["Admin"])
async def ifcfilequery(instruction:model.IfcFileQuery_Instruction):
    try:
        procToken = uuid.uuid4() # the token that will be used to track the process and is provided to the client
        await service.ifcfilequery(instruction, procToken)
        return {"message": "Submitted process", "token": str(procToken)}
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.msg)
