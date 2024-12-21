import os
import urllib.request

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()



# Set up the logging
import logging
log = logging.getLogger(__name__)

# Access environment variables as if they came from the actual environment
FASTAPI_RESULT_URL = os.getenv('FASTAPI_RESULT_URL')

from urllib import request
import json

def main_proc(task_id, task_dict):
    # notify the result to the FastAPI
    req = request.Request(FASTAPI_RESULT_URL, method="POST")
    req.add_header('Content-Type', 'application/json')
    taskResult = dict()
    taskResult['taskId'] = task_id
    taskResult['taskResult'] = task_dict
    data = dict()
    data['taskResult'] = taskResult
    data = json.dumps(data)
    data = data.encode()
    try:
        r = request.urlopen(req, data=data)
        content = r.read()
    except Exception as e:
        log.error(f"Error in notify_result.main_proc: {e}") 
    return

