##
#
#   Store the ifcJSON Data in the Postgres DB Tables
#
#   input : the filePath of the (filtered) ifcJSON file 
#  output : the Postgres Tables are uploaded with (filtered) ifcJSON
#
##

import pandas as pd
from time import perf_counter
import uuid
import json

from data import transform as data_transform
from data import insert_copy as data
import long_bg_tasks.task_modules.common_module as common


# Set up the logging
import logging
log = logging.getLogger(__name__)


#
# Store propertySet
#
def storePropertySets(pset_df, bundleId):
    # before touching to 'data', we need to extract 'name'
    pset_df.insert(0, 'name', pset_df['data'].apply(pd.Series)['name']) 
    # Convert the 'data' dict to a json string
    pset_df['data'] = pset_df['data'].apply(json.dumps)
    pset_df.insert(0, 'bundle_id', bundleId)
    pset_df.insert(0, 'created_at', pd.Timestamp.now())
    pset_for_db = pset_df[['bundle_id','uuid','name','data','created_at']]
    pset_for_db.columns = ['bundle_id','propertyset_id','name','elementjson','created_at']
    data.bulk_insert_df(pset_for_db, 'propertyset')
#
# Store representation
#
def storeRepresentations(repr_df, bundleId):
    # Convert the 'data' dict to a json string
    # repr_df['data'] = repr_df['data'].apply(json.dumps)
    repr_df['data'] = repr_df['data'].apply(json.dumps)
    repr_df.insert(0, 'bundle_id', bundleId)
    repr_df.insert(0, 'created_at', pd.Timestamp.now())
    repr_for_db = repr_df[['bundle_id','uuid','type','data','created_at']]
    repr_for_db.columns = ['bundle_id','representation_id','type','elementjson','created_at']
    data.bulk_insert_df(repr_for_db, 'representation')
#
# Store objects - representationIds are only present for specitic types
#
def storeObjects(obje_df, bundleId):
    # before touching to 'data', we need to extract 'name'
    obje_df.insert(0, 'name', obje_df['data'].apply(pd.Series)['name'])
    # Convert the list of UUIDs to a PostgreSQL array format
    obje_df['representationIds'] = obje_df['representationIds'].apply(lambda x: '{' + ','.join(x) + '}' if x else None)
    # Convert the 'data' dict to a json string
    obje_df['data'] = obje_df['data'].apply(json.dumps)
    obje_df.insert(0, 'bundle_id', bundleId)
    obje_df.insert(0, 'created_at', pd.Timestamp.now())
    obje_for_db = obje_df[['bundle_id','uuid','type','name','representationIds', 'data','created_at']]
    obje_for_db.columns = ['bundle_id','object_id','type','name','representation_ids','elementjson','created_at']
    data.bulk_insert_df(obje_for_db, 'object')
#
# Store relationships
#
def storeRelationships(rela_df, bundleId):
    # Convert the 'data' dict to a json string
    rela_df['data'] = rela_df['data'].apply(json.dumps)
    rela_df.insert(0, 'bundle_id', bundleId) 
    rela_df.insert(0, 'created_at', pd.Timestamp.now())
    rela_for_db = rela_df[['bundle_id','uuid','type','relating_type','relating_ref','data','created_at']]
    rela_for_db.columns = ['bundle_id','relationship_id','type','relating_type','relating_id','elementjson','created_at']
    data.bulk_insert_df(rela_for_db, 'relationship')
#
# Store relatedMembership
#
def storeRelatedMemberships(rela_df, bundleId):
    # Expand the related_types_and_refs column
    rela_df['related_types_and_refs'] = rela_df['related_types_and_refs'].apply(lambda x: x.split(';') if x else [])
    relm_for_db = rela_df.explode('related_types_and_refs').reset_index(drop=True)
    # Convert the JSON strings back to dictionaries
    relm_for_db['related_types_and_refs'] = relm_for_db['related_types_and_refs'].apply(json.loads)
    # Extract related_type and related_ref from the dictionaries
    relm_for_db['related_type'] = relm_for_db['related_types_and_refs'].apply(lambda x: x['related_type'])
    relm_for_db['related_ref'] = relm_for_db['related_types_and_refs'].apply(lambda x: x['related_ref'])
    # Do not Add the bundle_id and created_at columns. They are already there rela_df >> relm_for_db 
    # relm_for_db.insert(0, 'bundle_id', bundleId) 
    # relm_for_db.insert(0, 'created_at', pd.Timestamp.now())
    # Add a UUID for the relatedmembership
    relm_for_db.insert(0,'id', relm_for_db.apply(lambda _: uuid.uuid4(), axis=1))
    # Select and rename columns for the final DataFrame
    relm_for_db = relm_for_db[['id', 'bundle_id','uuid', 'related_type', 'related_ref', 'created_at']]
    relm_for_db.columns = ['id', 'bundle_id','relationship_id', 'object_type', 'object_id', 'created_at']
    data.bulk_insert_df(relm_for_db, 'relatedmembership')
    return


######################
# 
#   Start the process
#
######################

def main_proc(task_dict:dict):
    try:    
        withFilter = task_dict['instruction_dict']['withFilter']
        if withFilter == True:
            inputIfcJsonFilePath = task_dict['filteredIfcJsonFilePath']
        else:
            inputIfcJsonFilePath = task_dict['ifcJsonFilePath']
        # Get the reference IfcTypes
        ifcTypes_df = common.get_IfcTypes_from_ref_csv()
        # Get the reference Relationships
        rel_dict = common.get_relationhips_from_ref_csv()
        ifcJson, header = common.get_ifcJson(inputIfcJsonFilePath)
        jsonModelData = common.get_jsonModelData(ifcJson)
        modelData_df = common.get_modelData_df(jsonModelData, ifcTypes_df)                      
        pset_df = modelData_df[modelData_df['category'] == 'propertySet']
        repr_df = modelData_df[modelData_df['category'] == 'representation']
        rela_df = modelData_df[modelData_df['category'] == 'relationship']
        # Check for duplicate relationships 
        rela_df = common.process_relationship_duplicate_uuids(rela_df)

        obje_df = modelData_df[~modelData_df['category'].isin(['propertySet','relationship','representation'])]

        #   Add representationIds to  objects. 
        #   E.g., A wall has a representation, with a type and an array of representations      
        obje_df = common.add_representationIds_to_objects(obje_df) 
        #   add relatingType, relatingId and relatedTypeAndIds to relationships
        rela_df = common.add_relating_and_related_to_relationships(rela_df, rel_dict)
        
        bundleId = int(task_dict['bundleId'])
        
        storePropertySets(pset_df, bundleId)
        # storeBundleMembership(pset_df, bundleId, 'pset')
        storeRepresentations(repr_df, bundleId)
        # storeBundleMembership(repr_df, bundleId, 'repr')
        storeObjects(obje_df, bundleId)
        # storeBundleMembership(obje_df, bundleId, 'obje')
        storeRelationships(rela_df, bundleId)
        #toreBundleMembership(rela_df, bundleId, 'rela')
        storeRelatedMemberships(rela_df, bundleId)
        
        task_dict = data_transform.logInDB_store_ifcJSON_to_db(task_dict, header)
    except Exception as e:
        log.error(f'Error in main_proc of store_ifcjson_in_DB: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict
