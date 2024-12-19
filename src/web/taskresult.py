from fastapi import APIRouter, HTTPException, Body
import uuid 
import json  

from service import transform as service

router = APIRouter(prefix = "/result")

@router.post("/")
async def task_result(taskResult = Body(embed=True)):
    # to be done: notify the result via websockets to the requesting system
    try:
        print ('<<< Task result received:>>>')
        print(taskResult)
        return {"message": "Notified the result"}
    except Exception as exc:
        raise HTTPException(status_code=409, detail=exc.msg)
   
