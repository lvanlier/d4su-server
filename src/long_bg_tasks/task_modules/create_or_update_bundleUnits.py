
from sqlmodel import create_engine, Session, select, text
import pandas as pd
import uuid


# Set up the logging
import logging
log = logging.getLogger(__name__)

from data import init as init
from model import common as model

def collect_data_for_bundleUnits(session:Session, bundleId:str, relationshipTypesList:str, objectTypesList:str):
    statement_literal = f"""
        create temp table t_bunit as
        select object.bundle_id as bundle_id,
            object.object_id as unit_id, 
            object.type as unit_type,
            object.name as unit_name,
            object.elementjson ->> 'longName' as unit_longname,
            relationship.relationship_id as relationship_id,
            relationship.type as relationship_type,
            relationship.relating_id as parent_id
        from object 
        join relatedmembership on relatedmembership.bundle_id = object.bundle_id 
            and relatedmembership.object_id = object.object_id 
        join relationship on relationship.bundle_id = object.bundle_id
            and relationship.relationship_id = relatedmembership.relationship_id
        where object.bundle_id = '{bundleId}' 
            and relationship.type in {relationshipTypesList}
            and object.type in {objectTypesList}
        """        
    statement = text(statement_literal)
    result = session.exec(statement)
    statement_literal = f"""
        select t_bunit.bundle_id as bundle_id,
            unit_id, 
            unit_type,
            unit_name,
            unit_longname,
            relationship_id,
            relationship_type,
            parent_id,
            object.type as parent_type,
            object.name as parent_name,
            object.elementjson ->> 'longName' as parent_longname
        from t_bunit
        join object on object.bundle_id = t_bunit.bundle_id
            and object.object_id = t_bunit.parent_id
        """
    statement = text(statement_literal)
    results = session.exec(statement)
    result_list = [row for row in results]
    session.close()
    df = pd.DataFrame(result_list)
    # add a unique id as a hash of the row values before adding the created_at column
    df['edge_id'] = df.apply(lambda row: uuid.uuid5(uuid.NAMESPACE_DNS, ''.join(row.values.astype(str))), axis=1)
    df['unitjson'] = ""
    df['created_at'] = pd.Timestamp.now()
    df = df[['edge_id', 'bundle_id', 'unit_id', 'unit_type', 'unit_name', 'unit_longname', 'relationship_id', 'relationship_type', 'parent_id', 'parent_type', 'parent_name', 'parent_longname', 'unitjson','created_at']]
    return df

##
#
#   MAIN PROCEDURE
#
##

def main_proc(task_dict:dict):
    try: 
        bundleId = int(task_dict['bundleId'])
        session = init.get_session()
        relationshipTypesList = "('IfcRelAggregates', 'IfcRelContainedInSpatialStructure', 'IfcRelReferencedInSpatialStructure', 'IfcRelAssignsToGroup')"
        objectTypesList = "('IfcProject', 'IfcSite', 'IfcBuilding', 'IfcBuildingStorey', 'IfcSpace', 'IfcZone', 'IfcSpatialZone', 'IfcGroup')"
        df_edges = collect_data_for_bundleUnits(session, bundleId, relationshipTypesList, objectTypesList)
        # should be bulkified
        session = init.get_session()
        for index, row in df_edges.iterrows():
            statement = select(model.bundleUnit).where(model.bundleUnit.edge_id == row['edge_id'])
            bundleUnit_i = session.exec(statement).first()
            if bundleUnit_i is None:
                # insert the row
                bundleUnit_i = model.bundleUnit(
                    edge_id  = row['edge_id'],
                    bundle_id = row['bundle_id'],
                    unit_id = row['unit_id'],
                    unit_type = row['unit_type'],
                    unit_name = row['unit_name'],
                    unit_longname = row['unit_longname'],
                    relationship_id = row['relationship_id'],
                    relationship_type = row['relationship_type'],
                    parent_id = row['parent_id'],
                    parent_type = row['parent_type'],
                    parent_name = row['parent_name'],
                    parent_longname = row['parent_longname'],
                    unitjson = row['unitjson'],
                    created_at = row['created_at']
                )
                session.add(bundleUnit_i)
                session.commit()
            else:
                # update the row (only the unitjson is expected to change)
                if bundleUnit_i.unitjson != row['unitjson']:
                    bundleUnit_i.unitjson = row['unitjson']
                    bundleUnit_i.updated_at = pd.Timestamp.now()
                    session.add(bundleUnit_i)
                    session.commit()
                else:
                    pass           
        session.close()
    except Exception as e:
        log.error(f'Error in main_proc of create_or_update_bundleUnits: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict