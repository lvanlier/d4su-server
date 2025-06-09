import os

from sqlmodel import delete, select, null

import uuid
import datetime

from model import common as model       
from data import init2 as init

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()

# Set up the logging
import logging
log = logging.getLogger(__name__)

import json

class Journalize():
    def __init__(self, task_dict):
        try:
            self.task_dict = task_dict
            self.task_name = self.task_dict['taskName']
            self.proctoken = self.task_dict['procToken_str']
            if 'bundleId' in self.task_dict:
                self.bundleId = task_dict['bundleId']
                if self.bundleId == None or self.bundleId == '':
                    self.bundleId = 0
                elif str(self.bundleId).isnumeric():
                    self.bundleId = int(self.bundleId)
                else:
                    self.bundleId = 0
            else:        
                self.bundleId = 0
            if 'result' in self.task_dict:
                self.task_result = self.task_dict['result']
            else:
                self.task_result = []
            self.instruction = {}
            for key, value in self.task_dict.items():
                if str(key).endswith('_Instruction') :
                    self.instruction[key] = value
        except Exception as e:
            log.error(f"Error in Journalize.init: {e}")
            self.task_dict['status'] = 'error'
            self.task_dict['error'] = f"Error in Journalize.init: {e}"
        
    def journalize(self):
        try:
            session = init.get_session()
            bundleJournal_i = model.bundleJournal(
                id=uuid.uuid4(),
                bundle_id = self.bundleId,
                proctoken = uuid.UUID(self.proctoken),
                operation_json = {
                    'operation': self.task_name,
                    'instruction':self.instruction,
                    'result': self.task_result
                },
                created_at= datetime.datetime.now()
            )
            session.add(bundleJournal_i)
            session.commit()
            session.close() 
        except Exception as e:
            log.error(f"Error in Journalize.journalize: {e}") 
        finally:
            return self.task_dict

