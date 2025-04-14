from fastapi import APIRouter, HTTPException, Body
import uuid 
import json

import asyncio
from model.transform import resultQueue

from service import transform as service

router = APIRouter(prefix = "/result")

@router.post("/", tags=["Internal"])
async def task_result(taskResult = Body(embed=True)):
    tr = taskResult['taskResult']
    if tr['debug'] == True:
        print ('<<< Task result received:>>>')
    # notify the result via websockets to the requesting system
    try:
        result = {}
        result['taskName'] = tr['taskName']
        result['status'] = tr['status']
        result['error'] = tr['error']
        result['procToken'] = tr['procToken_str']
        result['result'] = tr['result']
        instructions = {}
        for key, value in tr.items():
            if str(key).endswith('_Instruction') :
                instructions[key] = value
        result['taskInstructions'] = instructions
        message = json.dumps(result, indent=4)
        await resultQueue.put(message)   
        return {"message": "Notified the result"}
    except Exception as exc:
        raise HTTPException(status_code=409, detail=exc.msg)
   
