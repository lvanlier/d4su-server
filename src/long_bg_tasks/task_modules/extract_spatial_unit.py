##
#
# Select all elements in a container such as a building storey of an IfcSpatialZone and createa a json 
# file with the data so that it can thenafter be converted to an IFC file
#
# This is tested here with a 'Spatial Unit' corresponding to a 'IfcBuildingStorey' but also works for an 'IfcSpatialZone'
#
# This is a multistep process:
# (1) we need to get the header of the bundle (all ifc's have a header); but we could also create a new one from scratch
# (2) we need to get the container, i.e. 'Spatial Unit',  e.g. the storey
# (3) we need to get the parents of the container with respect to the IfcRelAggregates hierarchy 
#   (if the container is e.g., a storey, the parent is usually the building, then the site, then the project)
# (4) we need to get all 'children elements' of the container, and their own 'children', etc. where the 'children' are
# the elements that are in the graph from 'relating' to 'related' and that descent from the container (e.g. the storey)
# For those, we will not get the propertySets, as they are not descendent of the container in the graph 
# (4.1) need to get all representations for the container if any
# (4.2) need to get all representations for the 'children's' if any
# (5) now that we have all elements (except propertySets), we need to look at the elements that are referenced 
# in one the element that we have but are NOT in the elements that we have. 
# (6) we need to get all propertysets which are related to the container or to the elements in the container; there, 
# the properties are the relating elements and we only need to look at properties that have a related element
# in the previous list of elements. The attention point is that the IfcRelDefinesByProperties will list entities that are not related to the container
# We need to remove all these extra references from the IfcRelDefinesByProperties
#
##

from sqlmodel import create_engine, Session, select, text
import json
import pandas as pd
from flatten_json import flatten

# Set up the logging
import logging
log = logging.getLogger(__name__)

from data import init as init

# Get the IfcTypes from the reference csv
def get_IfcTypes_from_ref_csv():
    ifctypes_csv_path = '../db/csv/ifc-types-ref.csv'
    ifcTypes_df = pd.read_csv(ifctypes_csv_path, delimiter=';')
    return ifcTypes_df

def getBundleHeader(session, bundleId):
    statement_literal = f"SELECT header FROM bundle WHERE bundle_id = '{bundleId}'"
    statement = text(statement_literal)
    result = session.exec(statement).one()
    return result[0]


def get_object_by_id(session: Session, bundleId:str, elementId: str):
    try:
        isFound = False
        statement_literal =f"""select object.object_id, object.name, object.type, object.elementjson
            from object
            where object.bundle_id = '{bundleId}' and object.object_id = '{elementId}'"""
        statement = text(statement_literal)
        result = session.exec(statement).one()
        isFound = True
        return isFound, result.elementjson
    except:
        return isFound, None
    
def get_representation_by_id(session: Session, bundleId:str, elementId: str):
    isFound = False
    try:
        statement_literal =f"""select representation.representation_id, representation.elementjson
            from representation 
            where representation.bundle_id = '{bundleId}' and representation.representation_id = '{elementId}'"""
        statement = text(statement_literal)
        result = session.exec(statement).one()
        isFound = True
        return isFound, result.elementjson
    except:
        return isFound, None


def get_parent(session: Session, bundleId:str, objectId: str):
    # parent is from IfcRelAggregates e.g. get the building for the storey
    statement_literal =f"""select object.object_id, object.name, object.type, relationship.relationship_id as rel_id, object.elementjson 
	    from object
	    join relationship on relationship.bundle_id = object.bundle_id 
            and relationship.relating_id = object.object_id 
	    join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id 
            and relatedmembership.relationship_id = relationship.relationship_id
	    where 
            relationship.type = 'IfcRelAggregates' and
            object.bundle_id = '{bundleId}' and
            relatedmembership.object_id = '{objectId}'"""
    statement = text(statement_literal)
    result = session.exec(statement).one()
    return result.rel_id, result.elementjson

def prune_relationship_from_siblings(session: Session, bundleId:str, objectId: str, relationshipId: str):
    # e.g. limit the related objects to the the only object with id = objectId
    statement_literal =f"""select relationship.elementjson 
        from relationship 
        where relationship.bundle_id = '{bundleId}' and 
        relationship.relationship_id = '{relationshipId}'"""
    statement = text(statement_literal)
    result = session.exec(statement).one()
    relatedObjects = result.elementjson['relatedObjects']
    relatedObjects = [relatedObject for relatedObject in relatedObjects if relatedObject['ref'] == objectId]
    result.elementjson['relatedObjects'] = relatedObjects
    return result.elementjson

# create a temporary table with the relating and related objects for a bundle
def create_temp_relating_and_related_for_bundle(session:Session, bundleId: str, relationshipTypesList, TYPE='INCLUDE'):
    if TYPE == 'EXCLUDE':
        statement_literal = f"""create temporary table rr_bundle as
            select object.bundle_id as bundle_id,
                object.object_id as object_id, 
                object.type as object_type,
                relationship.relating_id as relating_id,
                relationship.relating_type as relating_type,
                relationship.type as relationship_type
            from object 
            join relatedmembership on relatedmembership.bundle_id = object.bundle_id
                and relatedmembership.object_id = object.object_id 
            join relationship on relationship.bundle_id = object.bundle_id  
                and relationship.relationship_id = relatedmembership.relationship_id 
            where object.bundle_id = '{bundleId}' 
                and not relationship.type in {relationshipTypesList}""" 
    else:
        statement_literal = f"""create temporary table rr_bundle as
            select object.bundle_id as bundle_id,
                object.object_id as object_id, 
                object.type as object_type,
                relationship.relating_id as relating_id,
                relationship.relating_type as relating_type,
                relationship.type as relationship_type
            from object 
            join relatedmembership on relatedmembership.bundle_id = object.bundle_id 
                and relatedmembership.object_id = object.object_id 
            join relationship on relationship.bundle_id = object.bundle_id
                and relationship.relationship_id = relatedmembership.relationship_id
            where object.bundle_id = '{bundleId}' 
                and relationship.type in {relationshipTypesList}"""        
    statement = text(statement_literal)
    result = session.exec(statement)
    return result
    
# for testing
def get_all_rr_bundle(session: Session):
    statement_literal = f"""select * from rr_bundle"""
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row for row in results]
    return result_list

# for testing
def get_nodes_for_container(session: Session, containerId: str):
    statement_literal = f"""WITH RECURSIVE parents AS
        (
        SELECT
            bundle_id::text             AS bundle_id,
            object_id::text             AS id,
            object_type                 AS type,
            relationship_type           AS relationship_type,
            relating_id:: text    	    AS parent,
            relating_type               AS parent_type
        FROM rr_bundle
        WHERE
            relating_id = '{containerId}'
        UNION
        SELECT
            child.bundle_id::text        AS bundle_id,
            child.object_id::text        AS id,
            child.object_type            AS type,
            child.relationship_type      AS relationship_type,
            child.relating_id::text      AS parent,
            child.relating_type          AS parent_type            
        FROM rr_bundle child
            INNER JOIN parents p ON p.id = child.relating_id::text
        )
        SELECT
        bundle_id,
        id,
        type,
        relationship_type,
        parent,
        parent_type
        FROM parents;        
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row for row in results]
    return result_list 

# create a temporary table with the 'children' of a container
def create_temp_nodes_for_container(session: Session, containerId: str):
    statement_literal = f"""create temporary table nodes_for_container as 
    WITH RECURSIVE parents AS
        (
        SELECT
            bundle_id::text             AS bundle_id,
            object_id::text             AS id,
            object_type                 AS type,
            relationship_type           AS relationship_type,
            relating_id:: text    	    AS parent,
            relating_type               AS parent_type
        FROM rr_bundle
        WHERE
            relating_id = '{containerId}'
        UNION
        SELECT
            child.bundle_id::text        AS bundle_id,
            child.object_id::text        AS id,
            child.object_type            AS type,
            child.relationship_type      AS relationship_type,
            child.relating_id::text      AS parent,
            child.relating_type          AS parent_type            
        FROM rr_bundle child
            INNER JOIN parents p ON p.id = child.relating_id::text
        )
        SELECT
        bundle_id,
        id,
        type,
        relationship_type,
        parent,
        parent_type
        FROM parents;
        """   
    statement = text(statement_literal)
    result = session.exec(statement)
    return result

def get_objects_in_container(session: Session) -> list:
    # get all objects in a container such as a building storey
    statement_literal =f"""select distinct on (object.bundle_id, object.object_id) 
        object.bundle_id, object.object_id, object.name, object.type, object.elementjson  
        from object 
        join nodes_for_container on object.bundle_id::text = nodes_for_container.bundle_id 
            and object.object_id::text = nodes_for_container.id
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list

def add_object_id_to_temp_nodes_for_container(session: Session, bundleId: int, id: str):
    statement_literal = f"""insert into nodes_for_container 
        (bundle_id, id, type, relationship_type, parent, parent_type) 
        values ({bundleId}, '{id}', NULL, NULL, NULL, NULL)"""
    statement = text(statement_literal)
    result = session.exec(statement)
    return result

def get_representations_for_objects_in_container(session: Session) -> list:       
    # get all representations in a container such as a building storey
    # for each object, take all representations
    statement_literal =f"""select distinct on (representation.bundle_id, representation.representation_id) 
        representation.bundle_id, representation.representation_id, representation.type, representation.elementjson 
        from representation
        join object on object.bundle_id = representation.bundle_id 
            and representation.representation_id::text = any(object.representation_ids) 
        join nodes_for_container on object.bundle_id::text = nodes_for_container.bundle_id 
            and object.object_id::text = nodes_for_container.id
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list  
    
def get_propertysets_for_objects_in_container(session: Session) -> list:
    # get all propertysets in a container such as a building storey
    statement_literal =f"""select distinct on (propertyset.bundle_id, propertyset.propertyset_id) 
        propertyset.bundle_id, propertyset.propertyset_id, propertyset.name, propertyset.elementjson, relationship.relationship_id 
        from propertyset
        join relationship on relationship.bundle_id = propertyset.bundle_id 
            and relationship.relating_id = propertyset.propertyset_id
        join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id 
            and relatedmembership.relationship_id = relationship.relationship_id
        where relatedmembership.object_id in (select
        object.object_id from object 
        join nodes_for_container on object.bundle_id::text = nodes_for_container.bundle_id 
        and object.object_id::text = nodes_for_container.id) 
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list

def get_relationships_for_objects_in_container(session: Session, relationshipTypesList) -> list:
    # get all relationships in a container such as a building storey
    statement_literal =f"""select distinct on (relationship.bundle_id, relationship.relationship_id) 
        relationship.bundle_id, relationship.relationship_id, relationship.elementjson 
        from relationship
        join nodes_for_container on relationship.bundle_id::text = nodes_for_container.bundle_id 
            and relationship.relating_id::text = nodes_for_container.parent
        where relationship.type in {relationshipTypesList}
    """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list

def get_relationships_for_propertysets(session: Session) -> list:
    # get all relationships for propertySets
    statement_literal =f"""select distinct on (relationship.bundle_id, relationship.relationship_id) 
        relationship.bundle_id, relationship.relationship_id, relationship.elementjson 
        from relationship 
        join propertyset on relationship.bundle_id = propertyset.bundle_id 
            and relationship.relating_id = propertyset.propertyset_id
        join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id 
            and relatedmembership.relationship_id = relationship.relationship_id
        where relatedmembership.object_id in (select
        object.object_id from object 
        join nodes_for_container on object.bundle_id::text = nodes_for_container.bundle_id 
            and object.object_id::text = nodes_for_container.id) 
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list

######################
# 
#   Start the process
#
######################

def main_proc(task_dict:dict):
    try:
        bundleId=task_dict['instruction_dict']['bundleId'] 
        containerType = task_dict['instruction_dict']['elementType']
        containerId=task_dict['instruction_dict']['elementId']
        includeRelationshipTypes = task_dict['instruction_dict']['includeRelationshipTypes']
        # the list needs to be a tuple
        includeRelationshipTypesList = tuple(includeRelationshipTypes)
        PRINT= task_dict['debug'] = True
        outFilePath = task_dict['jsonFilePath']
        
        engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')
        session = Session(engine) 
        
        header = getBundleHeader(session, bundleId)
        outJsonModel = dict(header)
        outJsonModel['data'] = list()
        
        # list from child to parent
        list_of_parents = []
        list_of_rel_aggregates = []    
        isFound, container = get_object_by_id(session, bundleId, containerId)
        if isFound == False:
            log.info(f'container with id: {containerId} not found')
            raise ValueError(f'container with id: {containerId} not found')
        root = False
        obj_id = containerId
        while root == False:
            rel_id, parent = get_parent(session, bundleId, obj_id)
            rel_aggregates = prune_relationship_from_siblings(session, bundleId, obj_id, rel_id)
            list_of_parents.append(parent)
            list_of_rel_aggregates.append(rel_aggregates)
            obj_id = parent['globalId']
            if parent['type'] == 'IfcProject':
                root = True 
        
        list_of_parents.reverse()
        list_of_rel_aggregates.reverse()
        
        if PRINT:
            log.info(f'length of list_of_parents: {len(list_of_parents)}')
            log.info(f'length of list_of_rel_aggregates: {len(list_of_rel_aggregates)}')
        
        # excludeRelationshipTypesList = ('IfcRelConnectsPathElements')
        relationshipTypesList = includeRelationshipTypesList
        create_temp_relating_and_related_for_bundle(session, bundleId, relationshipTypesList, TYPE='INCLUDE') 
        # results = get_all_rr_bundle(session)

        if PRINT:
            log.info('passed  create_temp_relating_and_related_for_bundle')    
        
        create_temp_nodes_for_container(session, containerId)
        # results = get_nodes_for_container(session, containerId)
        
        if PRINT:
            log.info('passed create_temp_nodes_for_container')

        objects = get_objects_in_container(session)
        
        if PRINT:
            log.info('passed get_objects_in_container')
        
        representations = get_representations_for_objects_in_container(session)
        
        if PRINT:
            log.info('passed get_representations_for_objects_in_container')

        relationships_for_obj = get_relationships_for_objects_in_container(session, relationshipTypesList)
        
        if PRINT:
            log.info('passed get_relationships_for_objects_in_container')
    
        # before getting the propertySets,  and the relationships for the propertySets
        # we need to add the container and its parents to the list of nodes
        
        add_object_id_to_temp_nodes_for_container(session, bundleId, container['globalId'])
        for item in list_of_parents:
            add_object_id_to_temp_nodes_for_container(session, bundleId, item['globalId'])
        
        if PRINT:
            log.info('passed add_object_id_to_temp_nodes_for_container')
        
        propertySets = get_propertysets_for_objects_in_container(session)
        
        if PRINT:
            log.info('passed get_propertysets_for_objects_in_container')
                
        relationships_for_pset = get_relationships_for_propertysets(session)
        
        if PRINT:
            log.info('passed get_relationships_for_propertysets')
        
        # get all the elements that are referenced in the objects that we have
        
        refs_in_objects = set()
        for item in objects:
            item_f = flatten(item)
            for key, value in item_f.items():
                if key.endswith('_ref'):
                    refs_in_objects.add(value)
        for item in list_of_parents:
            item_f = flatten(item)
            for key, value in item_f.items():
                if key.endswith('_ref'):
                    refs_in_objects.add(value)
        if PRINT:
            log.info(f'length of refs_in_objects: {len(refs_in_objects)}')
        
        refs_in_representations = set()
        for item in representations:
            item_f = flatten(item)
            for key, value in item_f.items():
                if key.endswith('_ref'):
                    refs_in_representations.add(value)
        if PRINT:
            log.info(f'length of refs_in_representations: {len(refs_in_representations)}')
                    
        refs_of_representations = set()
        for item in representations:
            refs_of_representations.add(item['globalId'])
        if PRINT:
            log.info(f'length of refs_of_representations: {len(refs_of_representations)}')
        
        refs_from_all = list()
        refs_from_all.extend(refs_in_objects)
        refs_from_all.extend(refs_in_representations)
        
        refs_to_add = [item for item in refs_from_all if item not in refs_of_representations]
        if PRINT:
            log.info(f'length of refs_to_add: {len(refs_to_add)}')
            
        elements_to_add = []
        for item in refs_to_add: 
            isFound, element = get_representation_by_id(session, bundleId, item)
            if isFound == True:
                elements_to_add.append(element)
            else:
                isFound, element = get_object_by_id(session, bundleId, item)
                if isFound == True:
                    elements_to_add.append(element)
                else:
                    log.info(f'element with id: {item} not found in relationship and object tables')
                    
        session.close()
        
        # need to prune the IfcRelDefinesByProperties for related elements that are not in the objects
        # create list of refs_of_objects
        refs_of_objects = set()
        for item in objects:
            refs_of_objects.add(item['globalId'])
        for item in relationships_for_pset:
            if item['type'] == 'IfcRelDefinesByProperties':
                relatedObjects = item['relatedObjects']
                relatedObjects = [relatedObject for relatedObject in relatedObjects if relatedObject['ref'] in refs_of_objects]
                item['relatedObjects'] = relatedObjects
        
        
        if len(list_of_parents) != None:
            outJsonModel['data'].extend(list_of_parents)
            if PRINT:
                log.info(f'length of list_of_parents: {len(list_of_parents)}')
        outJsonModel['data'].append(container)
        if len(objects) != None:
            outJsonModel['data'].extend(objects)
            if PRINT:
                log.info(f'length of objects: {len(objects)}')
        if len(representations) != None:
            outJsonModel['data'].extend(representations)
            if PRINT:
                log.info(f'length of representations: {len(representations)}')
        if len(propertySets) != None:
            outJsonModel['data'].extend(propertySets)
            if PRINT:
                log.info(f'length of propertySets: {len(propertySets)}')
        if len(list_of_rel_aggregates) != None:
            outJsonModel['data'].extend(list_of_rel_aggregates)
            if PRINT:
                log.info(f'length of list_of_rel_aggregates: {len(list_of_rel_aggregates)}')
        if len(relationships_for_obj) != None:
            outJsonModel['data'].extend(relationships_for_obj)
            if PRINT:
                log.info(f'length of relationships_for_obj: {len(relationships_for_obj)}')
        if relationships_for_pset != None:
            outJsonModel['data'].extend(relationships_for_pset)
            if PRINT:
                log.info(f'length of relationships_for_pset: {len(relationships_for_pset)}')
        if len(elements_to_add) != None:
            outJsonModel['data'].extend(elements_to_add)
            if PRINT:
                log.info(f'length of elements_to_add: {len(elements_to_add)}')

        indent=2
        with open(outFilePath, 'w') as outJsonFile:
            json.dump(outJsonModel, outJsonFile, indent=indent)
    except Exception as e:
        log.error(f'Error in main_proc of extract_spatial_unit: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict

