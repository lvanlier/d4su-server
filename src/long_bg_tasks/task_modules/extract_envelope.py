##
#
# Find all all elements in a bundle that are marked as 'external'; create a csv file with the data (for testing purpose)
# and create an IfcJSON file with all the needed data: objects, representations, relationships and propertysets
#
# The process is as follows: 
#
# (1) select all property sets that mark object as 'external' in a bundle; save that to a temporary table
# (2) select the objects that have a relationship with the property set, select also their parent 
# with respect to the IfcRelContainedInSpatialStructure (should be only IfcBuildinStorey); save that to a temporary table;
# for testing purpose, save the content of the table to a csv file
# (3) get the data to produce a corresponding IfcFSON file
# (4) produce and save the IfcJSON file
#
# Issue to be resolved: the materials create problems and not added at this stage
# 
from sqlmodel import create_engine, Session, select, text
import pandas as pd
import uuid
import json
from flatten_json import flatten
from time import perf_counter

# Set up the logging
import logging
log = logging.getLogger(__name__)

from data import init as init
from data import files as file_store 

#
# Global cache of representations
#

def initialize_representations_cache(session, bundleId):
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
        # create a dictionary with the representation_id as key and the elementjson as value
        REPRESENTATIONS_CACHE = {str(row.representation_id):row.elementjson for row in result}
        t_stop = perf_counter()
        if PRINT:
            log.info(f'initialize_representations_cache took {round(t_stop - t_start, 2)} seconds')
    except Exception as e:
        log.error(f'Error in initialize_representations_cache: {e}')
        raise ValueError(f'Error in initialize_representations_cache: {e}')
    return


#
# Common functions
#
def getBundleHeader(session:Session, bundleId:str):
    statement_literal = f"SELECT header FROM bundle WHERE bundle_id = '{bundleId}'"
    statement = text(statement_literal)
    result = session.exec(statement).one()
    return result[0]

def get_parent(session: Session, bundleId:str, objectId: str):
    # parent is from IfcRelAggregates e.g. get the building for the storey
    statement_literal =f"""select parent.object_id, parent.name, parent.type, relationship.relationship_id as rel_id, parent.elementjson as parentjson, relationship.elementjson as relationshipjson
	    from object
		join relatedmembership on relatedmembership.bundle_id = object.bundle_id
			and relatedmembership.object_id = object.object_id
	    join relationship on relationship.bundle_id = relatedmembership.bundle_id 
            and relationship.relationship_id = relatedmembership.relationship_id 
	    join object as parent on parent.bundle_id = relationship.bundle_id 
            and parent.object_id  = relationship.relating_id
	    where 
            relationship.type = 'IfcRelAggregates' and
            object.bundle_id = '{bundleId}' and
            object.object_id = '{objectId}'"""
    statement = text(statement_literal)
    result = session.exec(statement).one()
    return result.rel_id, result.parentjson, result.relationshipjson

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

# This function returns all propertysets in a bundle that are marked as 'external'

# There are elements that do correspond to meaningful components of the envelop and others that do not
includePsetList = ('Pset_WallCommon', 'Pset_PlateCommon', 'Pset_WindowCommon', 'Pset_DoorCommon', 'Pset_SlabCommon',
 'Pset_CurtainWallCommon', 'Pset_ColumnCommon', 'Pset_BeamCommon', 'Pset_CoveringCommon')

# Properties that are not likely to be related to the envelope
excludePsetList = ('Pset_MemberCommon', 'Pset_RampCommon', 'Pset_RailingCommon', 'Pset_StairCommon', 'Pset_BuildingElementProxyCommon')


def get_pset_for_external_elements_in_a_bundle(session, bundleId):
    path_to_value = '{nominalValue,value}' # path to the value of the property
    t_start = perf_counter()
    statement_literal = f""" create temporary table tt_external as select
		propertyset.propertyset_id as id, 
		propertyset.name as name,    
		properties ->> 'name' as property_name,
		properties::json#>>'{path_to_value}' as is_external
	from 
		propertyset,
		json_array_elements(elementjson -> 'hasProperties') as properties
	where
		propertyset.propertyset_id in (select propertyset.propertyset_id 
			from propertyset 
			where propertyset.bundle_id = '{bundleId}'
		) and
		properties ->> 'name' = 'IsExternal' and
		(properties::json#>>'{path_to_value}')::text = 'true'
        and propertyset.name not in {excludePsetList}
	"""
    statement = text(statement_literal)
    session.exec(statement)
    t_stop = perf_counter()
    if PRINT:
        print(f'get_pset_for_external_elements_in_a_bundle took {round(t_stop - t_start, 2)} seconds')
    return

def get_external_elements_in_a_bundle(session, bundleId):
    t_start = perf_counter()
    statement_literal = f""" create temporary table tt_envelope as
    select
		object.object_id as object_id,
		object.type as object_type,
		relationship.relationship_id as relationship_id,
		parent_object.object_id as parent_object_id,
		parent_object.type as parent_object_type,
		parent_object.name as parent_object_name,
		parentrelationship.relationship_id as parent_relationship_id,
		tt_external.id as propertyset_id,
		tt_external.name as propertyset_name,
		tt_external.is_external as is_external 
        from tt_external
        join relationship on relationship.bundle_id = '{bundleId}'
			and relationship.relating_id = tt_external.id
        join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id
			and relatedmembership.relationship_id = relationship.relationship_id
   	 	join object on object.bundle_id = relatedmembership.bundle_id	
      		and object.object_id = relatedmembership.object_id
        join relatedmembership as parentmembership 
        	on parentmembership.bundle_id = object.bundle_id
			and parentmembership.object_id = object.object_id
        join relationship as parentrelationship 
        	on parentrelationship.bundle_id = parentmembership.bundle_id
			and parentrelationship.relationship_id = parentmembership.relationship_id
		join object as parent_object
			on parent_object.bundle_id = parentrelationship.bundle_id
			and parent_object.object_id = parentrelationship.relating_id
        where parentrelationship.type = 'IfcRelContainedInSpatialStructure'
	"""
    statement = text(statement_literal)
    session.exec(statement)
    t_stop = perf_counter()
    if PRINT:
        print(f'get_external_elements_in_a_bundle took {round(t_stop - t_start, 2)} seconds')
    return

def save_external_elements_in_a_bundle_to_a_csv(session: Session, csvFilePath: str):
    try:
        statement_literal = f""" select * from tt_envelope """
        statement = text(statement_literal)
        result = session.exec(statement).all()
        result_list = [row for row in result]
        df_result = pd.DataFrame(result_list)
        df_result.to_csv(csvFilePath, index=False)
    except Exception as e:
        log.error(f'Error in save_external_elements_in_a_bundle_to_a_csv: {e}')
    return

def get_envelope_objects_parents(session: Session, bundleId: str):
	try:
		statement_literal = f""" select distinct on (parent_object_id) parent_object_id, object.elementjson as parentjson
			from tt_envelope
			join object on object.bundle_id = '{bundleId}' and
				object.object_id = tt_envelope.parent_object_id
		"""
		statement = text(statement_literal)
		result = session.exec(statement).all()
		envelope_parents_ids = [row.parent_object_id for row in result]
		envelope_parents_json = [row.parentjson for row in result]
		statement_literal = f""" select distinct on (tt_envelope.parent_relationship_id) tt_envelope.parent_relationship_id, relationship.elementjson as relationshipjson
			from tt_envelope
			join relationship on relationship.bundle_id = '{bundleId}' and
				relationship.relationship_id = tt_envelope.parent_relationship_id 
		"""
		statement = text(statement_literal)
		result = session.exec(statement).all()
		envelope_parents_relationships_json = [row.relationshipjson for row in result]
		return envelope_parents_ids, envelope_parents_json, envelope_parents_relationships_json
	except Exception as e:
		log.error(f'Error in get_envelope_objects_parents: {e}')
		return None, None, None

def get_envelope_objects(session, bundleId):
	try:
		statement_literal = f""" select distinct on (tt_envelope.object_id) object.elementjson as objectjson
			from tt_envelope 
			join object on object.bundle_id = '{bundleId}'
				and object.object_id = tt_envelope.object_id
		"""
		statement = text(statement_literal)
		result = session.exec(statement).all()
		envelope_objects = [row.objectjson for row in result]
		return envelope_objects
	except Exception as e:
		log.error(f'Error in get_envelope_objects: {e}')
		return None	

def get_objectTypes_for_objects_in_envelope(session: Session, bundleId: str) -> list:
    # get all objectTypes for the objects in the envelope
    # i.e. the relatingTypeDefinition ref of IfcRelDefinesByType relationships that have 
    # a envelope object as relatedObject (e.g. if a node is an IfcWall, we need to have the IfcWallType if any)
    statement_literal =f"""select distinct on (object_type.bundle_id, object_type.object_id)
        object_type.bundle_id, object_type.object_id, object_type.type, object_type.elementjson
        from object as object_type
        join relationship on relationship.bundle_id = object_type.bundle_id
            and relationship.relating_id = object_type.object_id
        join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id
            and relatedmembership.relationship_id = relationship.relationship_id
        join object on object.bundle_id = relatedmembership.bundle_id
            and object.object_id = relatedmembership.object_id
        join tt_envelope on tt_envelope.object_id = object.object_id
        where relationship.type = 'IfcRelDefinesByType'
        and object_type.bundle_id = '{bundleId}' 
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list    


def get_envelope_representations(session, bundleId):
	try:
		statement_literal = f""" select distinct on (representation.bundle_id, representation.representation_id) representation.bundle_id, representation.representation_id, representation.elementjson  as representationjson 
            from tt_envelope 
			join object on object.bundle_id = '{bundleId}'
				and object.object_id = tt_envelope.object_id
			join representation on representation.bundle_id = object.bundle_id
				and representation.representation_id::text = any(object.representation_ids)
		"""
		statement = text(statement_literal)
		result = session.exec(statement).all()
		representations = [row.representationjson for row in result]
		return representations
	except Exception as e:
		log.error(f'Error in get_envelope_representations: {e}')
		return None
    

def get_propertysets_for_objects_in_envelope(session: Session, bundleId:str) -> list:
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
        join tt_envelope on tt_envelope.object_id = object.object_id
        where relationship.type = 'IfcRelDefinesByProperties' 
            and propertyset.bundle_id = '{bundleId}' 
        """
    statement = text(statement_literal)
    results = session.exec(statement).all()
    result_list = [row.elementjson for row in results]
    return result_list    
    
def get_up_relationships_for_objects_in_envelope(session: Session, bundleId:str) -> list:
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
        join tt_envelope on tt_envelope.object_id = object.object_id
        where relationship.type in ('IfcRelDefinesByType', 'IfcRelAssociatesMaterial', 'IfcRelDefinesByProperties', 'IfcRelDefinesByObject')
        and relationship.bundle_id = '{bundleId}'
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


from model.transform import ExtractEnvelope_Instruction, ExtractEnvelope_Result

class ExtractEnvelope:
    def __init__(self, task_dict:dict):
        try:       
            self.task_dict = task_dict
            instruction = ExtractEnvelope_Instruction(**self.task_dict['ExtractEnvelope_Instruction'])
            self.bundleId = instruction.bundleId
            self.useRepresentationsCache = instruction.useRepresentationsCache
            self.withIFC = instruction.withIFC
            self.BASE_PATH = self.task_dict['BASE_PATH']
            self.TEMP_PATH = self.task_dict['TEMP_PATH']
            self.IFCJSON_PATH = self.task_dict['IFCJSON_PATH']
            self.PRINT = self.task_dict['debug']
            global PRINT
            PRINT = self.PRINT
            if self.PRINT:
                    print(f'>>>>> In ExtractEnvelope.init: {self.bundleId}')
        except Exception as e:
            log.error(f'Error in ExtractEnvelope.init : {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in ExtractEnvelope.init : {e}'    

    def extract(self):
        try:
            t1_start = perf_counter()
                        
            session = init.get_session() 

            get_pset_for_external_elements_in_a_bundle(session, self.bundleId)
            get_external_elements_in_a_bundle(session, self.bundleId)
            if self.PRINT:
                csvFilePath = f'{self.TEMP_PATH}CSV/{uuid.uuid4()}_external_elements.csv'
                save_external_elements_in_a_bundle_to_a_csv(session, csvFilePath)
                t1_stop = perf_counter()
                t_get_external_elements = round(t1_stop - t1_start, 2)
                print(f'csvFilePath created after {t_get_external_elements} seconds')
            header = getBundleHeader(session, self.bundleId)
            outJsonModel = dict(header)
            outJsonModel['data'] = list()
            # select parents (storeys) from the temporary table tt_envelope
            envelope_parents_ids , envelope_parents, envelope_parents_relationships = get_envelope_objects_parents(session, self.bundleId)
            # starting with those storey, collect the parent hierarchy data
            # list from child to parent
            list_of_parents = list()
            list_of_rel_aggregates = list()
            set_of_ids = set() 
            for i in range(len(envelope_parents_ids)):
                root = False
                obj_id = envelope_parents_ids[i]
                while root == False:
                    rel_id, parentjson, relationshipjson = get_parent(session, self.bundleId, obj_id)
                    if parentjson['globalId'] in set_of_ids:
                        break
                    set_of_ids.add(parentjson['globalId'])	
                    list_of_parents.append(parentjson)
                    list_of_rel_aggregates.append(relationshipjson)
                    obj_id = parentjson['globalId']
                    if parentjson['type'] == 'IfcProject':
                        root = True
            list_of_parents.reverse()
            list_of_rel_aggregates.reverse()
            
            # select objects (walls, slabs,..., doors, windows) from the temporary table tt_envelope
            envelope_objects = get_envelope_objects(session, self.bundleId)
            # get all objectTypes for the objects in the envelope
            envelope_objectTypes = get_objectTypes_for_objects_in_envelope(session, self.bundleId)
            # select all representations for the objects
            envelope_representations = get_envelope_representations(session, self.bundleId) 
            # select all propertysets and corresponding relationships applying to the objects from the temporary table tt_envelope
            envelope_propertysets = get_propertysets_for_objects_in_envelope(session, self.bundleId)
            # select all material relationships for the objects in the temporary table tt_envelope
            up_relationships_for_objects_in_envelope = get_up_relationships_for_objects_in_envelope(session, self.bundleId)
            
            # get all the elements that are referenced in the objects that we have
            # first pass
            refs_in_objects = set()
            
            refs = get_refs_in_elements(envelope_objects)
            refs_in_objects.update(refs)
            
            refs = get_refs_in_elements(envelope_objectTypes)
            refs_in_objects.update(refs)
            
            refs = get_refs_in_elements(envelope_parents)
            refs_in_objects.update(refs)
            
            refs = get_refs_in_elements(list_of_parents)
            refs_in_objects.update(refs)               
            
            if PRINT:
                log.info(f'length of refs_in_objects: {len(refs_in_objects)}')
                
            refs_in_representations = set()
            refs = get_refs_in_elements(envelope_representations)
            refs_in_representations.update(refs)
            
            if PRINT:
                log.info(f'length of refs_in_representations: {len(refs_in_representations)}')
            
            refs_of_representations = set()
            for item in envelope_representations:
                refs_of_representations.add(item['globalId'])
            if PRINT:
                log.info(f'length of refs_of_representations: {len(refs_of_representations)}')
            
            refs_from_all = list()
            refs_from_all.extend(refs_in_objects)
            refs_from_all.extend(refs_in_representations)
            
            refs_to_add = [item for item in refs_from_all if item not in refs_of_representations]
            if PRINT:
                log.info(f'length of refs_to_add: {len(refs_to_add)}')
        
            # get the elements to add  
            
            if  self.useRepresentationsCache:
                initialize_representations_cache(session, self.bundleId)  
        
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
            ele_args = get_elements_to_add__recursion(session, self.bundleId, ele_args)
            elements_to_add = ele_args['elements_to_add']
                                    
            session.close()
            
            # need to prune the IfcRelDefinesByProperties for related elements that are not in the objects
            # create list of refs_of_objects
            refs_of_objects = set()
            for item in envelope_objects:
                refs_of_objects.add(item['globalId'])
            for item in up_relationships_for_objects_in_envelope:
                if item['type'] in ('IfcRelDefinesByProperties', 'IfcRelDefinesByType', 'IfcRelAssociatesMaterial'):
                    relatedObjects = item['relatedObjects']
                    relatedObjects = [relatedObject for relatedObject in relatedObjects if relatedObject['ref'] in refs_of_objects]
                    item['relatedObjects'] = relatedObjects
            
            # need to prune the IfcRelContainedInSpatialStructure for related elements that are not in the objects
            # create list of refs_of_objects
            refs_of_objects = set()
            for item in envelope_objects:
                refs_of_objects.add(item['globalId'])
            for item in envelope_parents_relationships:
                if item['type'] == 'IfcRelContainedInSpatialStructure':
                    relatedElements = item['relatedElements']
                    relatedElements = [relatedElement for relatedElement in relatedElements if relatedElement['ref'] in refs_of_objects]
                    item['relatedElements'] = relatedElements
            

            # need to prune de list_of_rel_aggregates for related elements that are not in the objects (such as spaces, ...)
            refs_of_objects = set()
            for item in list_of_parents:
                refs_of_objects.add(item['globalId'])
            for item in envelope_parents:
                refs_of_objects.add(item['globalId'])  
            for item in list_of_rel_aggregates:
                if item['type'] == 'IfcRelAggregates':
                    relatedObjects = item['relatedObjects']
                    relatedObjects = [relatedObject for relatedObject in relatedObjects if relatedObject['ref'] in refs_of_objects]
                    item['relatedObjects'] = relatedObjects    
        
            #
            # Populate outJsonModel['data'] = list() with the data
            #
            
            if len(list_of_parents) != None: # these are the parents of the storeys that are envelope_parents of the envelope_objects
                outJsonModel['data'].extend(list_of_parents)
                if PRINT:
                    log.info(f'length of list_of_parents: {len(list_of_parents)}')
            if len(envelope_parents) != None:
                outJsonModel['data'].extend(envelope_parents)
                if PRINT:
                    log.info(f'length of envelope_parents: {len(envelope_parents)}')            
            if len(envelope_objects) != None:
                outJsonModel['data'].extend(envelope_objects)
                if PRINT:
                    log.info(f'length of envelope_objects: {len(envelope_objects)}')
            if len(envelope_objectTypes) != None:
                outJsonModel['data'].extend(envelope_objectTypes)
                if PRINT:
                    log.info(f'length of envelope_objectTypes: {len(envelope_objectTypes)}')
            if len(envelope_representations) != None:
                outJsonModel['data'].extend(envelope_representations)
                if PRINT:
                    log.info(f'length of envelope_representations: {len(envelope_representations)}')
            if len(envelope_propertysets) != None:
                outJsonModel['data'].extend(envelope_propertysets)
                if PRINT:
                    log.info(f'length of envelope_propertysets: {len(envelope_propertysets)}')
            if len(list_of_rel_aggregates) != None: # for the parents of the storeys (the building) and their parents ...
                outJsonModel['data'].extend(list_of_rel_aggregates)
                if PRINT:
                    log.info(f'length of list_of_rel_aggregates: {len(list_of_rel_aggregates)}')
            if len(envelope_parents_relationships) != None: # for the parents of the envelope objects (the storeys)
                outJsonModel['data'].extend(envelope_parents_relationships)
                if PRINT:
                    log.info(f'length of envelope_parents_relationships: {len(envelope_parents_relationships)}')
            if len(up_relationships_for_objects_in_envelope) != None:
                outJsonModel['data'].extend(up_relationships_for_objects_in_envelope)
                if PRINT:
                    log.info(f'length of up_relationships_for_objects_in_envelope: {len(up_relationships_for_objects_in_envelope)}')
            
            if elements_to_add != None:
                outJsonModel['data'].extend(elements_to_add)
                if PRINT:
                    log.info(f'length of elements_to_add: {len(elements_to_add)}')		
                  
            result_rel_path = f'{self.IFCJSON_PATH}{uuid.uuid4()}_ENVELOPE.json' 
            result_path = f'{self.BASE_PATH}{result_rel_path}'
            file_store.write_file(result_path, json.dumps(outJsonModel, indent=2))    
            t2_stop = perf_counter()
            if PRINT:
                log.info(f"ExtractEnvelope took {round(t2_stop - t1_start, 2)} seconds")
            result = ExtractEnvelope_Result(
                resultPath = result_rel_path
            )
            self.task_dict['result']['ExtractEnvelope_Result'] = result.dict()             

        except Exception as e:
            log.error(f'Error in ExtractEnvelope.extract : {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in ExtractEnvelope.extract : {e}'    
        return self.task_dict

