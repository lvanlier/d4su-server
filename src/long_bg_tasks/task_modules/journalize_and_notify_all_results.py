import os

from sqlmodel import delete, select, null

import uuid
import datetime

from model import transform as model_transform
from model import common as model_common       
from data import init2 as init


# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()

# Access environment variables as if they came from the actual environment
FASTAPI_RESULT_URL = os.getenv('FASTAPI_RESULT_URL')

from urllib import request
import json

# Set up the logging
import logging
log = logging.getLogger(__name__)


class JournalizeAndNotifyAllResults():
    def __init__(self, task_id, chord_list:list):
        try:
            self.task_id = task_id
            self.chord_list = [json.loads(x) for x in chord_list]
            self.task_dict = model_transform.task_dict             
            self.task_dict['taskName'] = self.chord_list[0]['taskAllName']  
            self.instruction = self.chord_list[0]['taskAllInstruction']
            taskAll_instruction_className = self.chord_list[0]['taskAll_instruction_className']
            self.task_dict[taskAll_instruction_className] = self.instruction    
            self.task_dict['debug'] = self.chord_list[0]['allDebug']
            self.procToken_str = self.chord_list[0]['procToken_str']           
            for item in self.chord_list:
                if item['status'] == 'failed':
                    self.task_dict['status'] = 'failed'
                    self.task_dict['error'] = item['error']
                    break
            if 'bundleId' in self.instruction:
                self.bundleId = self.instruction['bundleId']
                if self.bundleId == None or self.bundleId == '':
                    self.bundleId = 0
                elif str(self.bundleId).isnumeric():
                    self.bundleId = int(self.bundleId)
                else:
                    self.bundleId = 0
            else:        
                self.bundleId = 0
            self.task_dict['bundleId']= self.bundleId
            self.task_dict['procToken_str'] = self.procToken_str
            task_results = []
            for item in self.chord_list:
                task_result = {}
                if 'unitId' in item:
                    task_result['unitId'] = item['unitId']
                if 'result' in item:
                    task_result['result'] = item['result']
                else:
                    task_result = []
                task_results.append(task_result)
            self.task_dict['result'] = task_results
        except Exception as e:
            log.error(f"Error in JournalizeAndNotify.init: {e}")
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f"Error in JournalizeAndNotify.init: {e}"
        
    def journalize(self):
        try:
            session = init.get_session()
            bundleJournal_i = model_common.bundleJournal(
                id=uuid.uuid4(),
                bundle_id=self.bundleId,
                operation_json = {
                    'operation': self.task_dict['taskName'],
                    'instruction':self.instruction,
                    'result': self.task_dict['result']
                },
                created_at= datetime.datetime.now()
            )
            session.add(bundleJournal_i)
            session.commit()
            session.close() 
        except Exception as e:
            log.error(f"Error in JournalizeAndNotify.journalize: {e}") 
        finally:
            return
    
    def notify(self):
        try:
            if self.task_dict['status'] != 'failed':
                self.task_dict['status'] = 'completed'
            taskResult = dict()
            taskResult['taskId'] = self.task_id
            taskResult['taskResult'] = self.task_dict
            req = request.Request(FASTAPI_RESULT_URL, method="POST")
            req.add_header('Content-Type', 'application/json')            
            data = dict()
            data['taskResult'] = taskResult
            data = json.dumps(data)
            data = data.encode()
            r = request.urlopen(req, data=data)
            content = r.read()
        except Exception as e:
            log.error(f"Error in JournalizeAndNotify.notify: {e}") 
        finally:
            return
