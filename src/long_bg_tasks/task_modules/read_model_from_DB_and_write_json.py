from sqlmodel import create_engine, Session, select, text

import json

import sys
import logging
log = logging.getLogger(__name__)

from data import common as data
from data import init2 as init
from data import files as file_store

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
import os
load_dotenv()
# Access environment variables as if they came from the actual environment
TMP_PATH  = os.getenv('TMP_PATH')

def getBundleHeader(session, bundle_id):
    statement_literal = f"SELECT header FROM bundle WHERE bundle_id = '{bundle_id}'"
    statement = text(statement_literal)
    result = session.execute(statement).one()
    return result[0]

def get_objects(session, bundle_id):
    statement_literal = f"SELECT elementjson FROM object WHERE object.bundle_id = '{bundle_id}'"
    statement = text(statement_literal)
    results = session.execute(statement).all()
    result_list = [row[0] for row in results]
    return result_list

def get_representations(session, bundle_id):
    statement_literal = f"SELECT elementjson FROM representation WHERE representation.bundle_id = '{bundle_id}'"
    statement = text(statement_literal)
    results = session.execute(statement).all()
    result_list = [row[0] for row in results]
    return result_list

def get_propertySets(session, bundle_id):
    statement_literal = f"SELECT elementjson FROM propertyset WHERE propertyset.bundle_id = '{bundle_id}'"
    statement = text(statement_literal)
    results = session.execute(statement).all()
    result_list = [row[0] for row in results]
    return result_list

def get_relationships(session, bundle_id):    
    statement_literal = f"SELECT elementjson FROM relationship WHERE relationship.bundle_id = '{bundle_id}'"
    statement = text(statement_literal)
    results = session.execute(statement).all()
    result_list = [row[0] for row in results]
    return result_list


def main_proc(task_dict:dict):
    try:
        bundle = data.getBundleById(task_dict['bundleId'])
        header = bundle.header
        outJsonModel = dict(header)
        
        session = init.get_session()
        objects = get_objects(session, bundle.bundle_id)
        representations = get_representations(session, bundle.bundle_id)
        propertySets = get_propertySets(session, bundle.bundle_id)
        relationships = get_relationships(session, bundle.bundle_id)
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
        
        # outFilePath = TMP_PATH + bundle.name +'.json'
        outFilePath = task_dict['jsonFilePath']
        
        file_store.write_file(outFilePath, json.dumps(outJsonModel, indent=2))
        '''
        indent=2
        with open(outFilePath, 'w') as outJsonFile:
            json.dump(outJsonModel, outJsonFile, indent=indent)
        '''
        
        task_dict['jsonFilePath'] = outFilePath
    except Exception as e:
        log.error(f'Error in main_proc of read_model_from_DB_and_write_json: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict
    
    
        
        