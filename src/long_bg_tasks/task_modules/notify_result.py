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

class NotifyResult():
    def __init__(self, task_id, task_dict):
        try:
            self.task_id = task_id
            self.task_dict = task_dict
        except Exception as e:
            log.error(f"Error in NotifyResult.init: {e}")
            self.task_dict['status'] = 'error'
            self.task_dict['error'] = f"Error in NotifyResult.init: {e}"
        
    def notify(self):
        try:
            req = request.Request(FASTAPI_RESULT_URL, method="POST")
            req.add_header('Content-Type', 'application/json')
            taskResult = dict()
            taskResult['taskId'] = self.task_id
            taskResult['taskResult'] = self.task_dict
            data = dict()
            data['taskResult'] = taskResult
            data = json.dumps(data)
            data = data.encode()
            r = request.urlopen(req, data=data)
            content = r.read()
        except Exception as e:
            log.error(f"Error in NotifyResult.notify: {e}") 
        return

