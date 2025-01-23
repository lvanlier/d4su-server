##
#
#   Precondition: a User has filled the CSV file with the spaces and their related SpatialZones
# 
#   Get all spaces with their related SpatialZone in a bundle from the csv file
#   Create the Spatial Zones in the database and relate them to the spaces
#
##

import pandas as pd
import uuid
import urllib.request
from typing import Literal
import datetime
import sys

from sqlmodel import create_engine, Session, select, text

from data import common as data
from data import init as init

# Set up the logging
import logging
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
log = logging.getLogger('d4su-server')

PRINT = False

spatialZone: Literal['Private Parts - Apartment', 'Private Parts - Multistorey Apartment', 'Common Parts', 'Parking', 'Common Site Parts']

def get_csv(fileURL):
    try:
        response = urllib.request.urlopen(fileURL)
        response_content = response.read().decode('utf-8')
        from io import StringIO
        df = pd.read_csv(StringIO(response_content), sep=';')
        return df
    except Exception as e:
        log.error(f'Error in getIfcModel: {e}')
        raise Exception(f'Error in getIfcModel: {e}')
    
def add_representation_to_df_sz(session, bundleId, df_sz):
    # this is a function that adds the representation to the df_sz
    # we may need the representation to ensure presence in some viewers even if the representations
    # are not meaningfull. 
    # Here we add the representations of the contained spaces to the spatialzones
    # But we don't handle object placement at this stage!
    #
    try:
        space_ids = df_sz['space_id'].tolist()
        list_str = ','.join([str("'"+i+"'") for i in space_ids])
        statement_literal = f""" select object_id::text, elementjson ->> 'representation' as representation 
            from object where bundle_id = {bundleId} and object_id in ({list_str})"""
        statement = text(statement_literal)
        result = session.exec(statement).all()
        result_list = [row for row in result]
        representation_dict = {row[0]: row[1] for row in result_list}
        df_sz['representation'] = df_sz['space_id'].map(representation_dict)
    except Exception as e:
        log.error(f'Error in add_representation_to_df_sz: {e}')
        raise Exception(f'Error in add_representation_to_df_sz: {e}')
    return df_sz




def create_and_relate_SpatialZones(session, bundleId, containerType, spatialZoneGivenType, df_sz, created_at):
    if PRINT: print(f"df_sz type: {type(df_sz)}")
    try:
        if containerType == 'IfcBuilding':
            container_colname = 'building_id'
        elif containerType == 'IfcBuildingStorey':
            container_colname = 'storey_id'
        else:
            raise Exception(f'Unsupported containerType: {containerType}')
        if spatialZoneGivenType == 'IfcSpatialZone': 
            spatialZoneRelationshipGivenType = 'IfcRelReferencedInSpatialStructure'
        elif spatialZoneGivenType == 'IfcGroup': 
            spatialZoneRelationshipGivenType = 'IfcRelAssignsToGroup'
        elif spatialZoneGivenType == 'IfcZone': 
            spatialZoneRelationshipGivenType = 'IfcRelAssignsToGroup'
        else:
            raise Exception(f'Unsupported spatialZoneGivenType: {spatialZoneGivenType}')
        containerId = ''
        spatialZoneName = ''
        spatialZoneId = ''
        object_list_container = []
        object_list_spatialzone = []
        if PRINT: log.info(f"df_sz type: {type(df_sz)}")
        for index, row in df_sz.iterrows():
            if PRINT: log.info(f"row['spatialzone_name']:{row['spatialzone_name']} SpatialZone: {spatialZoneName}")
            if row[container_colname] != containerId:
                if  containerId != '':
                    # end the previous container
                    relationshipType = 'IfcRelAggregates'
                    relatingType = containerType
                    relatingId = containerId
                    objectType = spatialZoneGivenType
                    relationshipId = ''
                    relationship = data.Db_Relationship(session, bundleId, relationshipId, relationshipType, relatingType, relatingId, objectType, object_list_container, created_at)
                    relationshipId = relationship.relationshipId
                    data.Db_RelatedMembership(session, bundleId, relationshipId, objectType, object_list_container, created_at)
                # begin a new container
                object_list_container = []
                containerId = row[container_colname]
            if row['spatialzone_name'] != spatialZoneName:
                if spatialZoneName != '':
                    if PRINT: log.info(f"END - row['spatialzone_name']:{row['spatialzone_name']} SpatialZone: {spatialZoneName}")
                    # end the previous spatialzone
                    object = data.Db_Object(session, bundleId, spatialZoneId, spatialZoneGivenType, sz_row['spatialzone_name'], sz_row['spatialzone_longname'], object_representation, created_at)
                    relationshipType = spatialZoneRelationshipGivenType
                    relationshipId = ''
                    relatingType = spatialZoneGivenType
                    relatingId = spatialZoneId
                    objectType = 'IfcSpace'
                    relationship = data.Db_Relationship(session, bundleId, relationshipId, relationshipType, relatingType, relatingId, objectType, object_list_spatialzone, created_at)
                    relationshipId = relationship.relationshipId
                    data.Db_RelatedMembership(session, bundleId, relationshipId, objectType, object_list_spatialzone, created_at)
                # begin a new spatialzone
                sz_row= row.copy()
                spatialZoneId = str(uuid.uuid4())
                object_list_container.append(spatialZoneId)
                spatialZoneName = row['spatialzone_name']
                if PRINT: log.info(f"BEGIN - row['spatialzone_name']:{row['spatialzone_name']} SpatialZone: {spatialZoneName}")
                object_list_spatialzone = []
                object_representation = []
                object_list_spatialzone.append(row['space_id'])
                if 'representation' in row:
                    object_representation.append(row['representation'])
            else:
                object_list_spatialzone.append(row['space_id'])
                if 'representation' in row:
                    object_representation.append(row['representation'])
        # end with the spatialzone in progress
        if  len(object_list_spatialzone) > 0:
            if PRINT: log.info(f"END - row['spatialzone_name']:{row['spatialzone_name']} SpatialZone: {spatialZoneName}")
            object = data.Db_Object(session, bundleId, spatialZoneId, spatialZoneGivenType, sz_row['spatialzone_name'], sz_row['spatialzone_longname'], object_representation, created_at)
            relationshipType = spatialZoneRelationshipGivenType
            relationshipId = ''
            relatingType = spatialZoneGivenType
            relatingId = spatialZoneId
            objectType = 'IfcSpace'
            relationship = data.Db_Relationship(session, bundleId, relationshipId, relationshipType, relatingType, relatingId, objectType, object_list_spatialzone, created_at)
            relationshipId = relationship.relationshipId
            data.Db_RelatedMembership(session, bundleId, relationshipId, objectType, object_list_spatialzone, created_at)
        # end with the container in progress
        if  len(object_list_container) > 0:
            relationshipType = 'IfcRelAggregates'
            relationshipId = ''
            relatingType = containerType
            relatingId = containerId
            objectType = spatialZoneGivenType
            relationship = data.Db_Relationship(session, bundleId, relationshipId, relationshipType, relatingType, relatingId, objectType, object_list_container, created_at)
            relationshipId = relationship.relationshipId
            data.Db_RelatedMembership(session, bundleId, relationshipId, objectType, object_list_container, created_at)
    except Exception as e:
        log.error(f'Error in create_and_relate_SpatialZones: {e}')
        raise Exception(f'Error in create_and_relate_SpatialZones: {e}')

##
# 
#   Main Procedure
#
##

def main_proc(task_dict:dict):
    try:
        bundleId=task_dict['instruction_dict']['bundleId'] 
        csvFileURL= task_dict['instruction_dict']['csvFileURL']
        hasRepresentation = task_dict['instruction_dict']['hasRepresentation']
        spatialZoneGivenType = task_dict['instruction_dict']['spatialZoneGivenType']         
        global PRINT
        PRINT = task_dict['debug']
        data.setPRINT(PRINT)
        # get the csv file
        df_sz = get_csv(csvFileURL)  
        session = init.get_session()
        created_at = datetime.datetime.now()
        #
        # Add the representation to the df_sz
        #
        if hasRepresentation:
            df_sz = add_representation_to_df_sz(session, bundleId, df_sz)
        #
        # Create the Spatial Zones in the database and relate them to their container and to their member spaces
        #
        # Pass for spatialZonesTypes on building level
        #
        df_sz_building_parts = df_sz[df_sz['spatialzone_type'].isin(['Common Parts', 'Parking', 'Private Parts - Multistorey Apartment'])]
        if df_sz_building_parts.empty != True:
            df_sz_building_parts = df_sz_building_parts.copy()
            df_sz_building_parts.sort_values(['spatialzone_type', 'building_id', 'spatialzone_name', 'storey_id'], ascending=[True, True, True, True], inplace=True)
            if PRINT:
                for index, row in df_sz_building_parts.iterrows():
                    print(row['spatialzone_name'],row['space_name'])
            containerType = 'IfcBuilding'
            create_and_relate_SpatialZones(session, bundleId, containerType, spatialZoneGivenType, df_sz_building_parts, created_at)
        
        df_sz_storey_parts = df_sz[df_sz['spatialzone_type'].isin(['Private Parts - Apartment'])]
        if df_sz_storey_parts.empty != True:
            df_sz_storey_parts = df_sz_storey_parts.copy()
            df_sz_storey_parts.sort_values(['spatialzone_type', 'building_id', 'spatialzone_name', 'storey_id'], ascending=[True, True, True, True], inplace=True) 
            containerType = 'IfcBuildingStorey'
            if PRINT:
                for index, row in df_sz_storey_parts.iterrows():
                    print(row['spatialzone_name'],row['space_name'])  
            create_and_relate_SpatialZones(session, bundleId, containerType, spatialZoneGivenType, df_sz_storey_parts, created_at)   
        session.commit()
        session.close()
    except Exception as e:
        log.error(f'Error in main_proc of create_spatialzones_in_bundle: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict
