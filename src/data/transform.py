from sqlmodel import delete, select

import uuid
import datetime

from model import common as model       
from data import init2 as init
from data import transform as data

# Set up the logging
import logging
log = logging.getLogger(__name__)

##
#
#   Update DB for the Import Step
#
##
def logInDB_create_ifcJSON(task_dict:dict, header:dict):
    try:
        spatialUnitId_str = task_dict['instruction_dict']['spatialUnitId']
        spatialUnitId = uuid.UUID(spatialUnitId_str)
        session = init.get_session()
        sourceFileURL=task_dict['instruction_dict']['sourceFileURL']
        fileName = sourceFileURL.split("/")[-1].split(".")[0]
        # create a new bundle
        bundle_i = model.bundle(  
            name=fileName,
            source_type='IFC',
            files={
                'sourceIfc': task_dict['instruction_dict']['sourceFileURL'],
                'sourceIfcHeader': header,
                'outputJson': task_dict['ifcJsonFilePath']
            },
            header=header,
            description="Ifc File",
            active=True
        )
        session.add(bundle_i)
        session.commit()
        session.refresh(bundle_i) 
        task_dict['bundleId'] = str(bundle_i.bundle_id) 
        #  create a root entry in bundleUnit     
        bundleUnit_i = model.bundleUnit(
            edge_id=uuid.uuid4(),
            bundle_id=bundle_i.bundle_id,
            unit_id=task_dict['rootObjectId'],
            unit_type=task_dict['rootObjectType'],
            unit_name=task_dict['rootObjectName'],
            relationship_id=uuid.UUID('00000000-0000-0000-0000-000000000000'),
            relationship_type='root',
            parent_id=uuid.UUID('00000000-0000-0000-0000-000000000000'),
            parent_type='',
            unitjson={}
        )   
        session.add(bundleUnit_i)
        edge_id = bundleUnit_i.edge_id
        spatialUnitBundleUnit_i = model.spatialUnitBundleUnit(
            id=uuid.uuid4(),
            spatial_unit_id=spatialUnitId,
            bundle_id=bundle_i.bundle_id,
            bundleunit_edge_id=edge_id
        )
        session.add(spatialUnitBundleUnit_i) 
        bundleJournal_i = model.bundleJournal(
            id=uuid.uuid4(),
            bundle_id=bundle_i.bundle_id,
            operation_json = {
                'operation':'import-and-transform',
                'description':'Imported IFC file and converted to ifcJSON',
            }
        )
        session.add(bundleJournal_i)
        session.commit()
        session.close()
    except Exception as e:
        task_dict['status'] = 'failed'
        log.error(f'Error in logInDB_create_ifcJSON: {e}')
    return task_dict
    

##
#
#   Update DB for the Filter Step
#
##
def logInDB_filter_ifcJSON(task_dict:dict, header:dict):
    try:
        # Update bundle
        bundleId_str = task_dict['bundleId']
        bundleId = int(bundleId_str)
        session = init.get_session()  
        statement = select(model.bundle).where(model.bundle.bundle_id == bundleId)
        results = session.exec(statement)
        bundle_i = results.one()
        files=dict(bundle_i.files)
        files['filteredJson'] = task_dict['filteredIfcJsonFilePath']
        files['filteredJsonHeader'] = header
        bundle_i.files=files
        bundle_i.header=header
        bundle_i.updated_at = datetime.datetime.now()
        session.add(bundle_i)
        # Add an entry in bundleJournal to document the operation
        bundleJournal_i = model.bundleJournal(
            id=uuid.uuid4(),
            bundle_id=bundleId,
            operation_json = {
                'operation':'filter',
                'description':'Filtered IFC file',
                'filter': task_dict['instruction_dict']['filter']
            }
        )
        session.add(bundleJournal_i)
        session.commit()
        session.close() 
    except Exception as e:
        task_dict['status'] = 'failed'
        log.error(f'Error in logInDB_filter_ifcJSON: {e}')
    return task_dict

##
#
#   Update DB for the Store in DB Step
#
##
def logInDB_store_ifcJSON_to_db(task_dict:dict, header:dict):
    try:
        # Update bundle
        bundleId_str = task_dict['bundleId']
        bundleId = int(bundleId_str)
        session = init.get_session()  
        statement = select(model.bundle).where(model.bundle.bundle_id == bundleId)
        results = session.exec(statement)
        bundle_i = results.one()
        bundle_i.header=header
        bundle_i.updated_at = datetime.datetime.now()
        session.add(bundle_i)
        # Update bundle to add a new entry in files
        bundleId_str = task_dict['bundleId']
        bundleId = int(bundleId_str)
        session = init.get_session()  
        # Add an entry in bundleJournal to document the operation
        bundleJournal_i = model.bundleJournal(
            id=uuid.uuid4(),
            bundle_id=bundleId,
            operation_json = {
                'operation':'store-in-db',
                'description':'Stored (filtered) IFC file in the database',
            }
        )
        session.add(bundleJournal_i)
        session.commit()
        session.close() 
    except Exception as e:
        task_dict['status'] = 'failed'
        log.error(f'Error in logInDB_store_ifcJSON_to_db: {e}')
    return task_dict