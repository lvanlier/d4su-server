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
#  If the container is not an IfcBuildingStorey but an IfcSpatialZone, things get a little more complex:
#  We also neetd the get the parents of all the spaces that are member of the spatialzone
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
from model import common as model

import long_bg_tasks.task_modules.common_module as common


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


def getBundleHeader(session, bundleId):
    statement_literal = f"SELECT header FROM bundle WHERE bundle_id = '{bundleId}'"
    statement = text(statement_literal)
    result = session.exec(statement).one()
    return result[0]

def getStyledItems(session: Session, bundleId:str):
    isFound = False
    try:   
        statement = select(model.object).where(model.object.bundle_id == int(bundleId), model.object.type == 'IfcStyledItem')
        results = session.exec(statement).all()
        result_list = [row.elementjson for row in results]
        isFound = True
        return isFound, result_list
    except:
        return isFound, None
            

def get_object_by_id(session: Session, bundleId:str, elementId: str):
    isFound = False
    try:   
        statement = select(model.object).where(model.object.bundle_id == int(bundleId), model.object.object_id == elementId)
        result = session.exec(statement).one()
        isFound = True
        return isFound, result.elementjson
    except:
        return isFound, None
    
def get_representation_by_id(session: Session, bundleId:str, elementId: str):
    isFound = False
    try:
        statement = select(model.representation).where(model.representation.bundle_id == int(bundleId), model.representation.representation_id == elementId)
        result = session.exec(statement).one()
        isFound = True
        return isFound, result.elementjson
    except:
        return isFound, None    

def get_propertyset_by_id(session: Session, bundleId: str, elementId: str):
    isFound = False
    try:
        statement = select(model.propertySet).where(model.propertySet.bundle_id == int(bundleId), model.propertySet.propertyset_id == elementId)
        result = session.exec(statement).one()
        isFound = True
        return isFound, result.elementjson
    except:
        return isFound, None

def get_parent(session: Session, bundleId:str, objectId: str):
    # parent is from IfcRelAggregates e.g. get the building for the storey
    # here we are looking for parent of a container or a space or their parents, i.e., of units in bundleunits
    statement = select(model.bundleUnit).where(model.bundleUnit.bundle_id == int(bundleId), model.bundleUnit.unit_id == objectId, model.bundleUnit.relationship_type == 'IfcRelAggregates')
    result = session.exec(statement).one()
    rel_id = result.relationship_id
    objectId = result.parent_id
    statement = select(model.object).where(model.object.bundle_id == int(bundleId), model.object.object_id == objectId)
    result = session.exec(statement).one()
    return rel_id, result.elementjson

def get_spaces_in_spatial_zone(session, bundleId, containerId):
    # get the spaces that are in the spatial zone
    statement  = select(model.bundleUnit).where(model.bundleUnit.bundle_id == int(bundleId), model.bundleUnit.parent_id == containerId, model.bundleUnit.relationship_type == 'IfcRelReferencedInSpatialStructure')
    result = session.exec(statement).all()
    result_list = [row.unit_id for row in result]
    return result_list

def get_contained_in_spatial_structure_relationships(session, bundleId, relatingId):
    isFound = False
    try:
        statement = select(model.relationship).where(model.relationship.bundle_id == int(bundleId), model.relationship.relating_id == relatingId, model.relationship.type == 'IfcRelContainedInSpatialStructure')
        result = session.exec(statement).one()
        isFound = True
        return isFound, result.elementjson
    except:
        return isFound, None
   
def prune_relationship_from_siblings(session: Session, bundleId:str, ids_to_keep: set[str], relationshipId: str):
    # e.g. limit the related objects to the the only object with id in ids_to_keep
    statement_literal =f"""select relationship.elementjson 
        from relationship 
        where relationship.bundle_id = '{bundleId}' and 
        relationship.relationship_id = '{relationshipId}'"""
    statement = text(statement_literal)
    result = session.exec(statement).one()
    relatedObjects = result.elementjson['relatedObjects']
    relatedObjects = [relatedObject for relatedObject in relatedObjects if relatedObject['ref'] in ids_to_keep]
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
        object_type.bundle_id, 
        object_type.object_id, 
        object_type.type, 
        object_type.elementjson
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
            self.STYLED = False
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
            # get the container
            isFound, container = get_object_by_id(session, self.bundleId, self.containerId)
            if isFound == False:
                log.info(f'container with id: {self.containerId} not found')
                raise ValueError(f'container with id: {self.containerId} not found')
            #
            # working on parents of the container and on parents of the spaces in the container 
            # when their parents are not the parent of the container, e.g. for a container 
            # that is a spatialzone with the building as parent and the spaces as children of storeys 
            #
            dict_of_parents = dict() # used as a set but is not a set given that set elements must be hashable, 
            dict_of_rel_aggregates = dict()    
            root = False
            ids_to_keep = set()
            ids_to_keep.add(self.containerId)
            if self.containerType == 'IfcBuildingStorey':
                pass
            elif self.containerType == 'IfcSpatialZone':
                # get the parents of the spaces that are in the spatial zone
                spaces_ids = get_spaces_in_spatial_zone(session, self.bundleId, self.containerId)
                ids_to_keep.update([str(id) for id in spaces_ids])
                # if the SpatialZone is a child of the building, I'll need to keep  
                # the Storeys that are the parents of the spaces in the SpatialZone
                rel_id, parent = get_parent(session, self.bundleId, self.containerId)
                if parent['type'] == 'IfcBuilding':
                    for id in spaces_ids:
                        rel_id, parent = get_parent(session, self.bundleId, id)
                        rel_aggregates = prune_relationship_from_siblings(session, self.bundleId, ids_to_keep, rel_id)
                        ids_to_keep.add(parent['globalId'])
            else:
                raise ValueError(f'containerType: {self.containerType} not supported at this stage')
            childrens_ids = ids_to_keep.copy()
            while root == False:
                next_ids = set()
                for obj_id in childrens_ids:
                    rel_id, parent = get_parent(session, self.bundleId, obj_id)
                    rel_aggregates = prune_relationship_from_siblings(session, self.bundleId, ids_to_keep, rel_id)
                    id = parent['globalId']
                    dict_of_parents[id] = parent
                    dict_of_rel_aggregates[rel_id] = rel_aggregates  
                    next_ids.add(id)
                    ids_to_keep.add(id)
                    if parent['type'] == 'IfcProject':
                        root = True
                childrens_ids = next_ids
            list_of_parents = [value for key, value in dict_of_parents.items()]
            list_of_rel_aggregates = [value for key, value in dict_of_rel_aggregates.items()]
            
            list_of_parents.reverse()
            list_of_rel_aggregates.reverse()
                    
            if self.PRINT:
                log.info(f'length of list_of_parents: {len(list_of_parents)}')
                log.info(f'length of list_of_rel_aggregates: {len(list_of_rel_aggregates)}')
            
            #
            # Need to get the IfcRelContainedInSpatialStructure relationships for the elements in the 
            # parents of the container, i.e. the storeys that are in the spatial zone or that are parents
            # of the spaces in the spatial zone
            #
            # We will prune the relationships at the end when we have all the elements in the container
            #
            list_of_rel_contained_in_spatial_structure = []
            for item in list_of_parents:
                isFound, relationship  = get_contained_in_spatial_structure_relationships(session, self.bundleId, item['globalId'])
                if isFound == True:
                    list_of_rel_contained_in_spatial_structure.append(relationship)

            print(f'length of list_of_rel_contained_in_spatial_structure: {len(list_of_rel_contained_in_spatial_structure)}')
            
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
                log.info(f'passed get_objectsTypes_in_container, length of objectTypes: {len(objectTypes)}')
            
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
                log.info(f'passed get_propertysets_for_objects_in_container; length of propertySets: {len(propertySets)}')
                    
            # relationships_for_pset = get_relationships_for_propertysets(session)
            up_relationships_for_obj =  get_up_relationships_for_objects_in_container(session)
            
        
            if self.PRINT:
                log.info('passed get_up_relationships_for_objects_in_container')
        
            # get all the references 'of' elements and 'in' elements that we have
            # first pass
            refs_of_objects = set()
            for item in objects:
                refs_of_objects.add(item['globalId'])

            refs_in_objects = set()            
            refs = get_refs_in_elements(objects)
            refs_in_objects.update(refs)   
            
            for item in objectTypes:
                refs_of_objects.add(item['globalId'])                    
            
            refs = get_refs_in_elements(objectTypes)
            refs_in_objects.update(refs)

            for item in list_of_parents:
                refs_of_objects.add(item['globalId'])                                            
            refs = get_refs_in_elements(list_of_parents)
            refs_in_objects.update(refs)               
            
            if self.PRINT:
                log.info(f'length of refs_in_objects: {len(refs_in_objects)}')
            
            refs_of_representations = set()
            for item in representations:
                refs_of_representations.add(item['globalId'])
            if self.PRINT:
                log.info(f'length of refs_of_representations: {len(refs_of_representations)}')

            refs_in_representations = set()
            refs = get_refs_in_elements(representations)
            refs_in_representations.update(refs)
            
            if self.PRINT:
                log.info(f'length of refs_in_representations: {len(refs_in_representations)}')                 
            
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
            elements_to_add_0 = ele_args['elements_to_add']
            
            refs_of_elements_to_add = set()
            for item in elements_to_add_0:
                refs_of_elements_to_add.add(item['globalId'])
            
            # dedup refs to add    
            refs_of_elements_to_add = [item for item in refs_of_elements_to_add if item not in refs_of_objects]
            refs_of_elements_to_add = [item for item in refs_of_elements_to_add if item not in refs_of_representations]
              
            # dedup elements to add
            elements_to_add = [item for item in elements_to_add_0 if item['globalId'] in refs_of_elements_to_add]
            
            #
            # Before proceeding to process StyledItems, we need to have all references
            #
            # The ref_from_all contains already:
            #   refs_in_objects, (objects, objectTypes,list_of_parents)
            #   refs_in_representations
            #   refs_of_elements_to_add
            # We add:
            refs_all = []
            refs_all.extend(refs_of_objects) # including objectTypes
            refs_all.extend(refs_of_representations)
            refs_all.extend(refs_in_objects)
            refs_all.extend(refs_in_representations)
            refs_all.extend(refs_of_elements_to_add)
            
            
            df_ref_all = pd.DataFrame(refs_all, columns=['ref'])
            
            #
            # we process the styledItems
            # ++++++++++++++++++++++++++
            # 
            
            isFound, styledItems = getStyledItems(session, self.bundleId)

            if isFound:
                self.STYLED = True
                if self.PRINT:
                    log.info(f' number Styled Item: {len(styledItems)}')
            else:
                log.info(f'No StyledItems')

            
            if self.STYLED:                        
                # NEED TO PUT THIS ALL IN A FUNCTION THAT IS ONLY CALLED IF isFound
                
                df_ref_of_styledItems = pd.DataFrame([item['globalId'] for item in styledItems], columns=['ref'])
                rows = [
                    {'ref': item['item']['ref'], 'globalId': item['globalId']}
                    for item in styledItems
                    if item.get('item') and item['item'].get('ref') is not None
                ]
                df_ref_in_styledItems = pd.DataFrame(rows, columns=['ref', 'globalId'])
                # Inner join of df_ref_of_styledItems and df_ref_all on 'ref'
                df_1 = pd.merge(df_ref_of_styledItems, df_ref_all, on='ref', how='inner')
                df_2 = pd.merge(df_ref_in_styledItems, df_ref_all, on='ref', how='inner')
                # styledItem_1 is the list of all styledItems where globalId is a ref in df_1 (material presentaton assignment)
                styledItem_1 = [
                    item for item in styledItems
                    if item.get('globalId') in df_1['ref'].values
                ]
                # styledItem_2 is the list of all styledItems that refers another representation (geometry presentaton assignment)            
                styledItems_2 = [
                    item for item in styledItems
                    if item.get('globalId') in df_2['globalId'].values
                ] 
                styledItems_to_add = styledItem_1 + styledItems_2
                
                if self.PRINT:
                    log.info(f'styledItems_to_add: {len(styledItems_to_add)}')
                            
                refs_to_add_for_styled_items = get_refs_in_elements(styledItems_to_add)    
                ele_args = {
                    'refs_to_add': refs_to_add_for_styled_items, 
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
                elements_to_add_forStyledItems = ele_args['elements_to_add']
                
                if self.PRINT:
                    log.info(f'elements_to_add_forStyledItems: {len(elements_to_add_forStyledItems)}')
                                            
            session.close()
            
            # need to prune the up_relationships_for_obj for related elements that are not in the objects
            # list of refs_of_objects already created above
            #   refs_of_objects = set()
            #   for item in objects:
            #       refs_of_objects.add(item['globalId'])
                
            for item in up_relationships_for_obj:
                if item['type'] in ('IfcRelDefinesByProperties', 'IfcRelDefinesByType', 'IfcRelAssociatesMaterial'):
                    relatedObjects = item['relatedObjects']
                    relatedObjects = [relatedObject for relatedObject in relatedObjects if relatedObject['ref'] in refs_of_objects]
                    item['relatedObjects'] = relatedObjects
                    
            for item in list_of_rel_contained_in_spatial_structure:
                relatedElements = item['relatedElements']
                relatedElements = [relatedElement for relatedElement in relatedElements if relatedElement['ref'] in refs_of_objects]
                item['relatedElements'] = relatedElements     
            
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
            if len(list_of_rel_contained_in_spatial_structure) != None:
                outJsonModel['data'].extend(list_of_rel_contained_in_spatial_structure)
                if self.PRINT:
                    log.info(f'length of list_of_rel_contained_in_spatial_structure: {len(list_of_rel_contained_in_spatial_structure)}')
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
            if len(styledItems_to_add) != None:
                outJsonModel['data'].extend(styledItems_to_add)
                if self.PRINT:
                    log.info(f'length of styledItems_to_add: {len(styledItems_to_add)}')
            if len(elements_to_add_forStyledItems) != None:
                outJsonModel['data'].extend(elements_to_add_forStyledItems)
                if self.PRINT:
                    log.info(f'length of elements_to_add_forStyledItems: {len(elements_to_add_forStyledItems)}')    

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

