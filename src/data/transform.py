from sqlmodel import delete, select, null


import uuid
import datetime
import json

from model import common as model       
from data import init2 as init
from data import transform as data

# Set up the logging
import logging
log = logging.getLogger(__name__)

##
#
# Create entry in Bundle for the StoreIfcJsonInDb
#
## 

def create_bundle_for_StoreIfcJsonInDb(spatialUnitId_str, bundleName, sourceFileURL, parentBundleId_str, rootObject, header):
    try:
        session = init.get_session()
        spatialUnitId = uuid.UUID(spatialUnitId_str)
        if parentBundleId_str is None:
            parentBundleId = None
        elif parentBundleId_str.isnumeric():
            parentBundleId = int(parentBundleId_str)
        else:
            parentBundleId = None
        # create a new bundle
        bundle_i = model.bundle(
            parent_id=parentBundleId, 
            name=bundleName,
            source_type='IFC',
            files={
                'ifcJsonFileURL': sourceFileURL,
            },
            header=header,
            description="IfcJson File",
            active=True
        )
        session.add(bundle_i)
        session.commit()
        session.refresh(bundle_i) 
        bundleId = bundle_i.bundle_id
        #  create a root entry in bundleUnit     
        bundleUnit_i = model.bundleUnit(
            bundleunit_id=uuid.uuid4(),
            bundle_id=bundleId,
            unit_id=rootObject['rootObjectId'],
            unit_type=rootObject['rootObjectType'],
            unit_name=rootObject['rootObjectName'],
            relationship_id=uuid.UUID('00000000-0000-0000-0000-000000000000'),
            relationship_type='root',
            parent_id=uuid.UUID('00000000-0000-0000-0000-000000000000'),
            parent_type='',
            unitjson={}
        )   
        session.add(bundleUnit_i)
        bundleunit_id = bundleUnit_i.bundleunit_id
        spatialUnitBundleUnit_i = model.spatialUnitBundleUnit(
            id=uuid.uuid4(),
            spatial_unit_id=spatialUnitId,
            bundle_id=bundleId,
            bundleunit_id=bundleunit_id
        )
        session.add(spatialUnitBundleUnit_i) 
        session.commit()
        session.close()
        return str(bundleId)
    except Exception as e:
        log.error(f'Error in create_bundle_for_StoreIfcJsonInDb: {e}')
        return None


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
        session.commit()
        session.close() 
    except Exception as e:
        task_dict['status'] = 'failed'
        log.error(f'Error in logInDB_store_ifcJSON_to_db: {e}')
    return task_dict


##
#
#   Update DB for the adding unit IfcJSON, IFC or glTF file
#
##
def updateBundleUnitJson(bundleId:str, unitId:str, key:str, value:str):
    try:
        session = init.get_session()
        bundleId = int(bundleId)
        unitId = uuid.UUID(unitId)
        statement = select(model.bundleUnit).where(model.bundleUnit.bundle_id == bundleId, model.bundleUnit.unit_id == unitId)
        results = session.exec(statement)
        bundleUnit_i = results.one()
        if bundleUnit_i.unitjson is None or bundleUnit_i.unitjson == '':
            bundleUnit_i.unitjson = {}
        unitjson = bundleUnit_i.unitjson
        unitjson[key] = value
        bundleUnit_i.unitjson = unitjson
        print(f'>>> updateBundleUnitJson: {bundleUnit_i.unitjson}')
        bundleUnit_i.updated_at = datetime.datetime.now()
        session.add(bundleUnit_i)
        session.commit()
        session.close() 
    except Exception as e:
        log.error(f'Error in updateBundleUnitJson: {e}')
    return