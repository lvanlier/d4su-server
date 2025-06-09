from pydantic import UUID4
from celery import chain, chord
import json

from model import admin as model
from model import task as task


from long_bg_tasks.tasks import (
    notifyResult,
    journalize,
    ifcFileQuery
)

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
import os
load_dotenv()
# Access environment variables as if they came from the actual environment
TMP_PATH = os.getenv('TMP_PATH')
BASE_PATH = os.getenv('BASE_PATH')


# Set up the logging
import logging
log = logging.getLogger(__name__)

def isDebug(name:str):
    if name in (
        ifcfilequery.__name__
    ):
        return True 
    return False

#
# Admin queries on Ifc and IfcJSON files 
#
async def ifcfilequery(instruction:model.IfcFileQuery_Instruction, procToken:UUID4):
    task_dict = task.task_dict().dict()
    task_dict['taskName'] = "ifcfilequery"
    task_dict[model.IfcFileQuery_Instruction.__name__] = instruction.dict()
    task_dict['procToken_str'] = str(procToken)
    task_dict['BASE_PATH'] = BASE_PATH
    task_dict['TEMP_PATH'] = TMP_PATH
    task_dict['debug'] = isDebug(ifcfilequery.__name__)
    task_dict_dump = json.dumps(task_dict)
    log.info(f"task_dict_dump: {task_dict_dump}")
    task_chain = chain(
        ifcFileQuery.s(task_dict_dump),
        journalize.s(),
        notifyResult.s() # use this instead of a result.get() to avoid blocking the main thread
    )
    result = task_chain.delay()
