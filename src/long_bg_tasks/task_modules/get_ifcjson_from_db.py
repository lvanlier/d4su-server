from sqlmodel import create_engine, Session, select, text

import json
import uuid

import sys
import logging
log = logging.getLogger(__name__)

from data import common as data
from data import init2 as init
from data import files as file_store

from model.transform import GetIfcJsonFromDb_Instruction, GetIfcJsonFromDb_Result

import os

class GetIfcJsonFromDb():
    def __init__(self, task_dict:dict):
        self.task_dict = task_dict
        try:
            instruction = GetIfcJsonFromDb_Instruction(**self.task_dict['GetIfcJsonFromDb_Instruction'])
            self.bundleId = instruction.bundleId
            self.task_dict['bundleId']=instruction.bundleId
            self.PRINT = task_dict['debug']
            self.BASE_PATH = task_dict['BASE_PATH']
            self.IFCJSON_PATH = task_dict['IFCJSON_PATH']
            if self.PRINT:
                log.info(f'>>>>> In GetIfcJsonFromDb.init with instruction: {instruction}')
        except Exception as e:
            log.error(f'Error in GetIfcJsonFromDb.init: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in GetIfcJsonFromDb.init: {e}'

    def getIfcJson(self):
        try:
            bundle = data.getBundleById(self.bundleId)
            header = bundle.header
            outJsonModel = dict(header)
            
            session = init.get_session()
            objects = self.get_objects(session, bundle.bundle_id)
            representations = self.get_representations(session, bundle.bundle_id)
            propertySets = self.get_propertySets(session, bundle.bundle_id)
            relationships = self.get_relationships(session, bundle.bundle_id)
            session.close()
            
            outJsonModel['data'] = list()
            if len(objects) != None:
                outJsonModel['data'].extend(objects)
            if len(representations) != None:
                outJsonModel['data'].extend(representations)
            if len(propertySets) != None:
                outJsonModel['data'].extend(propertySets)  
            if len(relationships) != None:
                outJsonModel['data'].extend(relationships)           
            
            # Write ifcJSON to a file
            result_rel_path = f'{self.IFCJSON_PATH}{uuid.uuid4()}_OUT.json' 
            result_path = f'{self.BASE_PATH}{result_rel_path}'
            file_store.write_file(result_path, json.dumps(outJsonModel, indent=2))

            # return what is needed for the next step
            result = GetIfcJsonFromDb_Result(
                resultPath = result_rel_path
            )
            self.task_dict['result']['GetIfcJsonFromDb_Result'] = result.dict()  
               
        except Exception as e:
            log.error(f'Error in GetIfcJsonFromDb.getIfcJson: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in GetIfcJsonFromDb.getIfcJson: {e}'
        return self.task_dict
    
    def getBundleHeader(self, session, bundle_id):
        statement_literal = f"SELECT header FROM bundle WHERE bundle_id = '{bundle_id}'"
        statement = text(statement_literal)
        result = session.execute(statement).one()
        return result[0]

    def get_objects(self, session, bundle_id):
        statement_literal = f"SELECT elementjson FROM object WHERE object.bundle_id = '{bundle_id}'"
        statement = text(statement_literal)
        results = session.execute(statement).all()
        result_list = [row[0] for row in results]
        return result_list

    def get_representations(self, session, bundle_id):
        statement_literal = f"SELECT elementjson FROM representation WHERE representation.bundle_id = '{bundle_id}'"
        statement = text(statement_literal)
        results = session.execute(statement).all()
        result_list = [row[0] for row in results]
        return result_list

    def get_propertySets(self, session, bundle_id):
        statement_literal = f"SELECT elementjson FROM propertyset WHERE propertyset.bundle_id = '{bundle_id}'"
        statement = text(statement_literal)
        results = session.execute(statement).all()
        result_list = [row[0] for row in results]
        return result_list

    def get_relationships(self, session, bundle_id):    
        statement_literal = f"SELECT elementjson FROM relationship WHERE relationship.bundle_id = '{bundle_id}'"
        statement = text(statement_literal)
        results = session.execute(statement).all()
        result_list = [row[0] for row in results]
        return result_list

  
