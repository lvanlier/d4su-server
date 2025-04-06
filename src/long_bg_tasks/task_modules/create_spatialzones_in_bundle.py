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
import json
from typing import Literal
import datetime
import sys

from sqlmodel import create_engine, Session, select, text

from data import common as data
from data import init as init
from data import files as file_store
import long_bg_tasks.task_modules.common_module as common

from urllib.parse import urlparse
from urllib.request import urlopen

# Set up the logging
import logging
log = logging.getLogger(__name__)


PRINT = False

# Should be moved to a config file
spatialZones_at_building_level = ['Common Parts', 'Parking', 'Private Parts - Multistorey Apartment', 'Private Parts - Multistorey Business', 'Storage', 'Private Parts - Amenities']
spatialZones_at_storey_level = ['Private Parts - Apartment', 'Private Parts - Business']
spatialZones = spatialZones_at_building_level + spatialZones_at_storey_level

def get_csv(fileURL):
    try:
        parsed_url = urlparse(fileURL)
        if parsed_url.scheme == 'http' or parsed_url.scheme == 'https':
            csv_content = urlopen(fileURL).read().decode('utf-8')
        else: # local file
            csv_content = file_store.read_file(fileURL).decode('utf-8')
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content), sep=';')
        return df
    except Exception as e:
        log.error(f'Error get_csv: {e}')
        raise Exception(f'Error get_csv: {e}')
    
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


def create_and_relate_SpatialZones(session, createdSpatialZones, bundleId, containerType, spatialZoneGivenType, df_sz, created_at):
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
                    object = data.Db_Object(session, bundleId, spatialZoneId, spatialZoneGivenType, sz_row['spatialzone_name'], sz_row['spatialzone_longname'],sz_row['spatialzone_type'], object_representation, created_at)
                    spatialZone = SpatialZone(
                        spatialZoneId = spatialZoneId,
                        spatialZoneName = sz_row['spatialzone_name'],
                        spatialZoneLongName = sz_row['spatialzone_longname'],
                        spatialZoneType = sz_row['spatialzone_type']
                    )
                    createdSpatialZones.append(spatialZone.dict())
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
            object = data.Db_Object(session, bundleId, spatialZoneId, spatialZoneGivenType, sz_row['spatialzone_name'], sz_row['spatialzone_longname'], sz_row['spatialzone_type'], object_representation, created_at)
            spatialZone = SpatialZone(
                spatialZoneId = spatialZoneId,
                spatialZoneName = sz_row['spatialzone_name'],
                spatialZoneLongName = sz_row['spatialzone_longname'],
                spatialZoneType = sz_row['spatialzone_type']
            )
            createdSpatialZones.append(spatialZone.dict())
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
        return createdSpatialZones
    except Exception as e:
        log.error(f'Error in create_and_relate_SpatialZones: {e}')
        raise Exception(f'Error in create_and_relate_SpatialZones: {e}')

from model.transform import CreateSpatialZonesInBundle_Instruction, CreateSpatialZonesInBundle_Result, SpatialZone   
class CreateSpatialZonesInBundle():
    def __init__(self, task_dict:dict):
        self.task_dict = task_dict
        try:
            instruction = CreateSpatialZonesInBundle_Instruction(**self.task_dict['CreateSpatialZonesInBundle_Instruction'])
            self.bundleId = instruction.bundleId
            self.task_dict['bundleId'] = instruction.bundleId
            self.spatialZoneGivenType = instruction.spatialZoneGivenType
            self.hasRepresentation = instruction.hasRepresentation
            self.sourceFileURL = instruction.sourceFileURL
            self.createdSpatialZones = []      
            self.BASE_PATH = task_dict['BASE_PATH']
            self.SPACES_PATH = task_dict['SPACES_PATH']
            self.PRINT = task_dict['debug']
            global PRINT
            PRINT = self.PRINT
            if self.PRINT:
                log.info(f'>>>>> In CreateSpatialZonesInBundle.init with instruction: {instruction}')
        except Exception as e:
            log.error(f'Error in CreateSpatialZonesInBundle.init: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in CreateSpatialZonesInBundle.init: {e}'

    def createSpatialZones(self):
        try:
            # get the csv file
            csvFilePath = common.setFilePath(self.sourceFileURL, self.BASE_PATH)               
            df_sz = get_csv(csvFilePath)  
            session = init.get_session()
            created_at = datetime.datetime.now()
            #
            # Add the representation to the df_sz
            #
            if self.hasRepresentation:
                df_sz = add_representation_to_df_sz(session, self.bundleId, df_sz)
            #
            # Create the Spatial Zones in the database and relate them to their container and to their member spaces
            #
            # Pass for spatialZonesTypes on building level
            #
            df_sz_building_parts = df_sz[df_sz['spatialzone_type'].isin(spatialZones_at_building_level)]
            if df_sz_building_parts.empty != True:
                df_sz_building_parts = df_sz_building_parts.copy()
                df_sz_building_parts.sort_values(['spatialzone_type', 'building_id', 'spatialzone_name', 'storey_id'], ascending=[True, True, True, True], inplace=True)
                if PRINT:
                    for index, row in df_sz_building_parts.iterrows():
                        print(row['spatialzone_name'],row['space_name'])
                containerType = 'IfcBuilding'
                self.createdSpatialZones = create_and_relate_SpatialZones(session, self.createdSpatialZones, self.bundleId, containerType, self.spatialZoneGivenType, df_sz_building_parts, created_at)
            
            df_sz_storey_parts = df_sz[df_sz['spatialzone_type'].isin(spatialZones_at_storey_level)]
            if df_sz_storey_parts.empty != True:
                df_sz_storey_parts = df_sz_storey_parts.copy()
                df_sz_storey_parts.sort_values(['spatialzone_type', 'building_id', 'spatialzone_name', 'storey_id'], ascending=[True, True, True, True], inplace=True) 
                containerType = 'IfcBuildingStorey'
                if PRINT:
                    for index, row in df_sz_storey_parts.iterrows():
                        print(row['spatialzone_name'],row['space_name'])  
                self.createdSpatialZones = create_and_relate_SpatialZones(session, self.createdSpatialZones, self.bundleId, containerType, self.spatialZoneGivenType, df_sz_storey_parts, created_at)   
            session.commit()
            session.close()
            # store the createdSpatialZones in a json
            result_rel_path = f'{self.SPACES_PATH}JSON/{uuid.uuid4()}_spatialZone.json' 
            result_path = f'{self.BASE_PATH}{result_rel_path}'       
            jsonContent = json.dumps({'createdSpatialZones': self.createdSpatialZones }, indent=2)
            file_store.write_file(result_path, jsonContent)
            result = CreateSpatialZonesInBundle_Result(
                resultPath = result_rel_path
            )
            self.task_dict['bundleId'] = self.bundleId
            self.task_dict['CreateSpatialZonesInBundle_Result'] = result.dict()
        except Exception as e:
            log.error(f'Error in CreateSpatialZonesInBundle.createSpatialZones: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in CreateSpatialZonesInBundle.createSpatialZones: {e}'
        return self.task_dict
