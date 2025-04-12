##
#
# Select all elements in a container such as a building storey or an IfcSpatialZone and createa a json 
# file with the data so that it can thenafter be converted to an IFC file
#
# This has been developed for a 'Spatial Unit' corresponding to a 'IfcBuildingStorey' but also works for an 'IfcSpatialZone'
#
# This is a multistep process:
# (1) we need to get the header of the bundle (all ifc's have a header); but we could also create a new one from scratch
# (2) we need to get the container, i.e. 'Spatial Unit',  e.g. the storey
# (3) we need to get the parents of the container with respect to the IfcRelAggregates hierarchy 
#   (if the container is e.g., a storey, the parent is usually the building, then the site, then the project)
# (4) we need to get all 'children elements' of the container, and their own 'children', etc. where the 'children' are
# the elements that are in the graph from 'relating' to 'related' and descent from the container (e.g. the storey)
# (4.1) we need to get the objects
# (4.2) we need to get the object types
# (4.3) we need to get the representations 
# (4.4) we need to get the 'down' relationships for the objects, i.e. the relationships where the object is the 'relating' element
# (4.5) we need to get the 'up' relationships for the objects, i.e. the relationships where the object is the 'related' element
# (4.6) we need to get the propertySets for the objects
# (5) now that we have all elements we need to look at the elements that are referenced 
# in one the element that we have but are NOT in the elements that we have. 
# These referenced elements can be objects, representations or propertysets (referenced by an object type)
# We need to get all these elements and add them to the list of elements. This is a recursive process as a referenced element can also reference
# other elements that are not in the list of elements
# (6) we need to prune the related list of relationships to ensure we do not include related elements that are not in the spatial unit
# This applies to IfcRelDefinesByProperties, IfcRelDefinesByType, IfcRelAssociatesMaterial
#
##

from sqlmodel import create_engine, Session, select, text
import json
import uuid
import pandas as pd
from flatten_json import flatten


# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter


from data import init as init
from data import files as file_store 
from data import transform as data

from model.transform import ExtractSpatialUnit_Instruction, ExtractSpatialUnit_Result

#
# Global cache of representations
#

def initialize_representations_cache(session, bundleId, PRINT=False):
    global REPRESENTATIONS_CACHE
    REPRESENTATIONS_CACHE = {}
    t_start = perf_counter()
    try:
        statement_literal =f"""select representation.representation_id, representation.elementjson
            from representation 
            where representation.bundle_id = '{bundleId}'
        """
        statement = text(statement_literal)
        result = session.exec(statement).all()
        REPRESENTATIONS_CACHE = {str(row.representation_id):row.elementjson for row in result}
        t_stop = perf_counter()
        if PRINT:
            log.info(f'initialize_representations_cache took {round(t_stop - t_start, 2)} seconds')
    except Exception as e:
        log.error(f'Error in initialize_representations_cache: {e}')
        raise ValueError(f'Error in initialize_representations_cache: {e}')
    return


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

def get_propertyset_by_id(session: Session, bundleId: str, elementId: str):
    isFound = False
    try:
        statement_literal =f"""select propertyset.propertyset_id, propertyset.name, propertyset.elementjson
            from propertyset 
            where propertyset.bundle_id = '{bundleId}' and propertyset.propertyset_id = '{elementId}'"""
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

def get_objectsTypes_for_objects_in_container(session: Session) -> list:
    # get all objectTypes in a container such as a building storey
    # i.e. the relatingTypeDefinition ref of IfcRelDefinesByType relationships that have 
    # a node object as relatedObject (e.g. if a node is an IfcWall, we need to have the IfcWallType if any)
    statement_literal =f"""select distinct on (object_type.bundle_id, object_type.object_id)
        object_type.bundle_id, object_type.object_id, object_type.type, object_type.elementjson
        from object as object_type
        join relationship on relationship.bundle_id = object_type.bundle_id
            and relationship.relating_id = object_type.object_id
        join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id
            and relatedmembership.relationship_id = relationship.relationship_id
        join object on object.bundle_id = relatedmembership.bundle_id
            and object.object_id = relatedmembership.object_id
        join nodes_for_container on object.bundle_id::text = nodes_for_container.bundle_id
            and object.object_id::text = nodes_for_container.id
        where relationship.type = 'IfcRelDefinesByType'
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
        join object on object.bundle_id = relatedmembership.bundle_id
            and object.object_id = relatedmembership.object_id
        join nodes_for_container on object.bundle_id::text = nodes_for_container.bundle_id
            and object.object_id::text = nodes_for_container.id
        where relationship.type = 'IfcRelDefinesByProperties'
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list    

def get_down_relationships_for_objects_in_container(session: Session, relationshipTypesList) -> list:
    # get all relationships in a container such as a building storey where the nodes are relating_id's
    # does not include the relationships for the propertySets or for the objectTypes where the nodes are related_id's
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

def get_up_relationships_for_objects_in_container(session: Session) -> list:
    # get all IfcRelDefinesByType, IfcRelAssociatesMaterial, IfcRelDefinesByProperties, IfcRelDefinesByObject 
    # relationships in a container such as a building storey
    # where the nodes are related_id's
    statement_literal =f"""select distinct on (relationship.bundle_id, relationship.relationship_id)
        relationship.bundle_id, relationship.relationship_id, relationship.elementjson
        from relationship 
        join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id
            and relatedmembership.relationship_id = relationship.relationship_id
        join object on object.bundle_id = relatedmembership.bundle_id
            and object.object_id = relatedmembership.object_id
        join nodes_for_container on object.bundle_id::text = nodes_for_container.bundle_id
            and object.object_id::text = nodes_for_container.id
        where relationship.type in ('IfcRelDefinesByType', 'IfcRelAssociatesMaterial', 'IfcRelDefinesByProperties', 'IfcRelDefinesByObject')
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list    


# use instead get_up_relationships_for_objects_in_container
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

# This function returns all the elements that are referenced in the objects that we have
# and that are not yet in the representations
def get_elements_to_add__recursion(session, bundleId, ele_args, PRINT=False): 

    refs_to_add = ele_args['refs_to_add']
    elements_to_add = ele_args['elements_to_add']
    elements_not_found = ele_args['elements_not_found']
    
    useRepresentationsCache = ele_args['useRepresentationsCache']
        
    counter_add_representation = ele_args['counter_add_representation']
    counter_add_object = ele_args['counter_add_object']
    counter_add_propertyset = ele_args['counter_add_propertyset']
    t1_start = ele_args['t1_start']
    times = ele_args['times'] 
    
    if t1_start == 0:
        t1_start = perf_counter()
        
    refs_to_add_next = set()
    if len(refs_to_add) > 0:   
        for item in refs_to_add:
            if useRepresentationsCache:
                try:
                    element = REPRESENTATIONS_CACHE[item]
                    elements_to_add.append(element)
                    counter_add_representation += 1
                    refs = get_refs_in_element(element)
                    refs_to_add_next.update(refs)
                except:
                    isFound, element = get_propertyset_by_id(session, bundleId, item)
                    if isFound == True:
                        elements_to_add.append(element)
                        counter_add_propertyset += 1
                        refs = get_refs_in_element(element)
                        refs_to_add_next.update(refs) 
                    else:  
                        isFound, element = get_object_by_id(session, bundleId, item)
                        if isFound == True:
                            elements_to_add.append(element)
                            counter_add_object += 1
                            refs = get_refs_in_element(element)
                            refs_to_add_next.update(refs)         
                        else:
                            elements_not_found.append(item)
            else: 
                isFound, element = get_representation_by_id(session, bundleId, item)
                if isFound == True:
                    elements_to_add.append(element)
                    counter_add_representation += 1
                    refs = get_refs_in_element(element)
                    refs_to_add_next.update(refs)       
                else:
                    isFound, element = get_propertyset_by_id(session, bundleId, item)
                    if isFound == True:
                        elements_to_add.append(element)
                        counter_add_propertyset += 1
                        refs = get_refs_in_element(element)
                        refs_to_add_next.update(refs) 
                    else:
                        isFound, element = get_object_by_id(session, bundleId, item)
                        if isFound == True:
                            elements_to_add.append(element)
                            counter_add_object += 1
                            refs = get_refs_in_element(element)
                            refs_to_add_next.update(refs)         
                        else:
                            elements_not_found.append(item)
        ele_args = {
            'refs_to_add': refs_to_add_next,
            'elements_to_add': elements_to_add,
            'elements_not_found': elements_not_found,
            'useRepresentationsCache': useRepresentationsCache,
            'counter_add_representation': counter_add_representation,
            'counter_add_object': counter_add_object,
            'counter_add_propertyset': counter_add_propertyset,
            't1_start': t1_start,
            'times': times + 1
        }
        ele_args = get_elements_to_add__recursion(session, bundleId, ele_args, PRINT)
    else:
        t1_stop = perf_counter()
        if PRINT:
            log.info(f'=== begin get_elements_to_add__recursion')
            log.info(f'depth of recursion: {times - 1}')
            log.info(f'useRepresentationsCache: {useRepresentationsCache}')
            log.info(f'get_elements_to_add_one_by_one took {round(t1_stop - t1_start, 2)} seconds')
            log.info(f'length of elements_to_add: {len(elements_to_add)}')
            log.info(f'length of elements_not_found: {len(elements_not_found)}')
            log.info(f'counter_add_representation: {counter_add_representation}')
            log.info(f'counter_add_object: {counter_add_object}')
            log.info(f'counter_add_propertyset: {counter_add_propertyset}')   
            log.info(f'=== end get_elements_to_add__recursion')
    return ele_args


def get_refs_in_element(element):
    refs_in_element = set()
    element_f = flatten(element)
    for key, value in element_f.items():
        if key.endswith('_ref'):
            refs_in_element.add(value)
    return refs_in_element
    
def get_refs_in_elements(elements_list):
    refs_in_elements = set()
    for element in elements_list:
        refs = get_refs_in_element(element)
        refs_in_elements.update(refs)
    return refs_in_elements


class ExtractSpatialUnit:
    
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = ExtractSpatialUnit_Instruction(**task_dict['ExtractSpatialUnit_Instruction'])
            self.bundleId = instruction.bundleId
            self.task_dict['bundleId']=instruction.bundleId
            self.useRepresentationsCache = instruction.useRepresentationsCache
            self.containerType = instruction.elementType
            self.containerId = instruction.elementId
            self.includeRelationshipTypes = instruction.includeRelationshipTypes
            self.includeRelationshipTypesList = tuple(self.includeRelationshipTypes)
            self.BASE_PATH = task_dict['BASE_PATH']
            self.IFCJSON_PATH = task_dict['IFCJSON_PATH']
            self.PRINT = task_dict['debug']
            self.start = perf_counter()
            if self.PRINT:
                print(f'>>>>> In ExtractSpatialUnit: {self.containerId}') 
        except Exception as e:
            log.error(f'Error in init of ConvertIfcToJson: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in init of ConvertIfcToJson: {e}'

    def extract(self):
        try:
            session = init.get_session()       
            header = getBundleHeader(session, self.bundleId)
            outJsonModel = dict(header)
            outJsonModel['data'] = list()
            
            # list from child to parent
            list_of_parents = []
            list_of_rel_aggregates = []    
            isFound, container = get_object_by_id(session, self.bundleId, self.containerId)
            if isFound == False:
                log.info(f'container with id: {self.containerId} not found')
                raise ValueError(f'container with id: {self.containerId} not found')
            root = False
            obj_id = self.containerId
            while root == False:
                rel_id, parent = get_parent(session, self.bundleId, obj_id)
                rel_aggregates = prune_relationship_from_siblings(session, self.bundleId, obj_id, rel_id)
                list_of_parents.append(parent)
                list_of_rel_aggregates.append(rel_aggregates)
                obj_id = parent['globalId']
                if parent['type'] == 'IfcProject':
                    root = True 
            
            list_of_parents.reverse()
            list_of_rel_aggregates.reverse()
            
            if self.PRINT:
                log.info(f'length of list_of_parents: {len(list_of_parents)}')
                log.info(f'length of list_of_rel_aggregates: {len(list_of_rel_aggregates)}')
            
            # excludeRelationshipTypesList = ('IfcRelConnectsPathElements')
            relationshipTypesList = self.includeRelationshipTypesList
            create_temp_relating_and_related_for_bundle(session, self.bundleId, relationshipTypesList, TYPE='INCLUDE') 
            # results = get_all_rr_bundle(session)

            if self.PRINT:
                log.info('passed  create_temp_relating_and_related_for_bundle')    
            
            create_temp_nodes_for_container(session, self.containerId)
            # results = get_nodes_for_container(session, containerId)
            
            if self.PRINT:
                log.info('passed create_temp_nodes_for_container')

            objects = get_objects_in_container(session)
            
            if self.PRINT:
                log.info(f'passed get_objects_in_container, length of objects: {len(objects)}')
                
            objectTypes = get_objectsTypes_for_objects_in_container(session)
            
            if self.PRINT:
                print(f'passed get_objectsTypes_in_container, length of objectTypes: {len(objectTypes)}')
            
            representations = get_representations_for_objects_in_container(session)
            
            if self.PRINT:
                log.info('passed get_representations_for_objects_in_container')

            down_relationships_for_obj = get_down_relationships_for_objects_in_container(session, relationshipTypesList)
            
            if self.PRINT:
                log.info('passed get_down_relationships_for_objects_in_container')
        
            # before getting the propertySets,  and the relationships for the propertySets
            # we need to add the container and its parents to the list of nodes
            
            add_object_id_to_temp_nodes_for_container(session, self.bundleId, container['globalId'])
            for item in list_of_parents:
                add_object_id_to_temp_nodes_for_container(session, self.bundleId, item['globalId'])
            
            if self.PRINT:
                log.info('passed add_object_id_to_temp_nodes_for_container')
            
            propertySets = get_propertysets_for_objects_in_container(session)
            
            if self.PRINT:
                print(f'passed get_propertysets_for_objects_in_container; length of propertySets: {len(propertySets)}')
                    
            # relationships_for_pset = get_relationships_for_propertysets(session)
            up_relationships_for_obj =  get_up_relationships_for_objects_in_container(session)
            
        
            if self.PRINT:
                log.info('passed get_up_relationships_for_objects_in_container')
            
            # get all the elements that are referenced in the objects that we have
            # first pass
            refs_in_objects = set()
            
            refs = get_refs_in_elements(objects)
            refs_in_objects.update(refs)
            
            refs = get_refs_in_elements(objectTypes)
            # should have ref to representations and refs to propertysets
            refs_in_objects.update(refs)
            
            refs = get_refs_in_elements(list_of_parents)
            refs_in_objects.update(refs)               
            
            if self.PRINT:
                log.info(f'length of refs_in_objects: {len(refs_in_objects)}')
            
            refs_in_representations = set()
            refs = get_refs_in_elements(representations)
            refs_in_representations.update(refs)
            
            if self.PRINT:
                log.info(f'length of refs_in_representations: {len(refs_in_representations)}')
                        
            refs_of_representations = set()
            for item in representations:
                refs_of_representations.add(item['globalId'])
            if self.PRINT:
                log.info(f'length of refs_of_representations: {len(refs_of_representations)}')
            
            refs_from_all = list()
            refs_from_all.extend(refs_in_objects)
            refs_from_all.extend(refs_in_representations)
            
            refs_to_add = [item for item in refs_from_all if item not in refs_of_representations]
            if self.PRINT:
                log.info(f'length of refs_to_add: {len(refs_to_add)}')
            
            # get the elements to add  
            
            if  self.useRepresentationsCache:
                initialize_representations_cache(session, self.bundleId, PRINT=self.PRINT)  
        
            ele_args = {
                'refs_to_add': refs_to_add, 
                'elements_to_add': [],
                'elements_not_found': [],
                'useRepresentationsCache': self.useRepresentationsCache,
                'counter_add_representation': 0,
                'counter_add_object': 0,
                'counter_add_propertyset': 0,
                't1_start': 0,
                'times': 0
            }
            ele_args = get_elements_to_add__recursion(session, self.bundleId, ele_args, PRINT=self.PRINT)
            elements_to_add = ele_args['elements_to_add']
                                  
            session.close()
            
            # need to prune the up_relationships_for_obj for related elements that are not in the objects
            # create list of refs_of_objects
            refs_of_objects = set()
            for item in objects:
                refs_of_objects.add(item['globalId'])
                
            for item in up_relationships_for_obj:
                if item['type'] in ('IfcRelDefinesByProperties', 'IfcRelDefinesByType', 'IfcRelAssociatesMaterial'):
                    relatedObjects = item['relatedObjects']
                    relatedObjects = [relatedObject for relatedObject in relatedObjects if relatedObject['ref'] in refs_of_objects]
                    item['relatedObjects'] = relatedObjects 
            
            if len(list_of_parents) != None:
                outJsonModel['data'].extend(list_of_parents)
                if self.PRINT:
                    log.info(f'length of list_of_parents: {len(list_of_parents)}')
            outJsonModel['data'].append(container)
            if len(objects) != None:
                outJsonModel['data'].extend(objects)
                if self.PRINT:
                    log.info(f'length of objects: {len(objects)}')
            if len(objectTypes) != None:
                outJsonModel['data'].extend(objectTypes)
                if self.PRINT:
                    log.info(f'length of objectTypes: {len(objectTypes)}')
            if len(representations) != None:
                outJsonModel['data'].extend(representations)
                if self.PRINT:
                    log.info(f'length of representations: {len(representations)}')
            if len(propertySets) != None:
                outJsonModel['data'].extend(propertySets)
                if self.PRINT:
                    log.info(f'length of propertySets: {len(propertySets)}')
            if len(list_of_rel_aggregates) != None:
                outJsonModel['data'].extend(list_of_rel_aggregates)
                if self.PRINT:
                    log.info(f'length of list_of_rel_aggregates: {len(list_of_rel_aggregates)}')
            if len(down_relationships_for_obj) != None:
                outJsonModel['data'].extend(down_relationships_for_obj)
                if self.PRINT:
                    log.info(f'length of relationships_for_obj: {len(down_relationships_for_obj)}')
            if len(up_relationships_for_obj) != None:
                outJsonModel['data'].extend(up_relationships_for_obj)
                if self.PRINT:
                    log.info(f'length of up_relationships_for_obj: {len(up_relationships_for_obj)}')
            if len(elements_to_add) != None:
                outJsonModel['data'].extend(elements_to_add)
                if self.PRINT:
                    log.info(f'length of elements_to_add: {len(elements_to_add)}')

            result_rel_path = f'{self.IFCJSON_PATH}{uuid.uuid4()}_EXTRACT.json' 
            result_path = f'{self.BASE_PATH}{result_rel_path}'
            file_store.write_file(result_path, json.dumps(outJsonModel, indent=2))    
    
            result = ExtractSpatialUnit_Result(
                resultPath = result_rel_path,
                runtime = round(perf_counter() - self.start, 2)
            )
            data.updateBundleUnitJson(self.bundleId, self.containerId, 'IfcJSON', result_rel_path)
            self.task_dict['unitId'] = self.containerId
            self.task_dict['result']['ExtractSpatialUnit_Result'] = result.dict()             
        except Exception as e:
            log.error(f'Error ExtractSpatialUnit.extract: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error ExtractSpatialUnit.extract: {e}'
        finally:
            pass
        return self.task_dict

