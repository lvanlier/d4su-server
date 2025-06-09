from pydantic import BaseModel

# this  documents the format of the task_dict which is passed to the task modules
class task_dict(BaseModel):
    taskName: str | None = ''
    status: str | None = 'processing'
    error: str | None = ''
    procToken_str: str | None = ''
    result: dict | None = {}
    bundleId: str | None = ''
    unitId: str | None = ''
    spatialUnitId: str | None = ''

class task_result(BaseModel) :
    taskId: str | None = None,
    taskResult : dict | None = None


import asyncio
resultQueue = asyncio.Queue()
