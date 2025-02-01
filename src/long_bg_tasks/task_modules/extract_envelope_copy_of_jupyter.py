#
# Select all elements in a bundle that are marked as 'external' 
#
# (1) select all property sets that mark object as 'external' in a bundle; save that to a temporary table
# (2) select the objects that have a relationship with the property set, select also their parent 
# with respect to the IfcRelContainedInSpatialStructure (should be only IfcBuildinStorey); save that to a temporary table;
# for testing purpose, save the content of the table to a csv file
# (3) get the data to produce a corresponding IfcFSON file
# (4) produce and save the IfcJSON file
# 
from sqlmodel import create_engine, Session, select, text
import pandas as pd
import json
from flatten_json import flatten
from time import perf_counter

# Set up the logging
import sys
import logging
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

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

#############

# This function returns all propertysets in a bundle that are marked as 'external'

# There are elements that do correspond to meaningful components of the envelop and others that do not
includePsetList = ('Pset_WallCommon', 'Pset_PlateCommon', 'Pset_WindowCommon', 'Pset_DoorCommon', 'Pset_SlabCommon',
 'Pset_CurtainWallCommon', 'Pset_ColumnCommon', 'Pset_BeamCommon', 'Pset_CoveringCommon')

# Properties that are not likely to be related to the envelope
excludePsetList = ('Pset_MemberCommon', 'Pset_RampCommon', 'Pset_RailingCommon', 'Pset_StairCommon', 'Pset_BuildingElementProxyCommon')


def get_pset_for_external_elements_in_a_bundle(session, bundleId):
	path_to_value = '{nominalValue,value}' # path to the value of the property
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
	return

def get_external_elements_in_a_bundle(session, bundleId):
	statement_literal = f""" create temporary table tt_envelope as select
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
	return

def save_external_elements_in_a_bundle_to_a_csv(session, bundleId, csvFilePath):
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

def get_envelope_objects_parents(session, bundleId):
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

def get_envelope_representations(session, bundleId):
	try:
		statement_literal = f""" select distinct on (tt_envelope.object_id) representation.elementjson  as representationjson from tt_envelope 
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
    
def get_envelope_propertysets(session, bundleId):
	try:
		statement_literal = f""" select distinct on (tt_envelope.object_id) propertyset.elementjson as propertysetjson, relationship.elementjson as relationshipjson	 
		from tt_envelope 
			join object on object.bundle_id = '{bundleId}'
				and object.object_id = tt_envelope.object_id
			join relatedmembership on relatedmembership.bundle_id = object.bundle_id
				and relatedmembership.object_id = object.object_id
			join relationship on relationship.bundle_id = relatedmembership.bundle_id
				and relationship.relationship_id = relatedmembership.relationship_id
			join propertyset on propertyset.bundle_id = relationship.bundle_id
				and propertyset.propertyset_id = relationship.relating_id
		"""
		statement = text(statement_literal)
		result = session.exec(statement).all()
		propertysets = [row.propertysetjson for row in result]
		relationshipsjson = [row.relationshipjson for row in result]
		return propertysets, relationshipsjson
	except Exception as e:
		log.error(f'Error in get_envelope_propertysets: {e}')
		return None, None
    
def get_envelope_materials(session, bundleId):
	try:
		statement_literal = f""" select distinct on (tt_envelope.object_id) relationship.elementjson as relationshipjson 
		from tt_envelope 
			join object on object.bundle_id = '{bundleId}'
				and object.object_id = tt_envelope.object_id
			join relatedmembership on relatedmembership.bundle_id = object.bundle_id
				and relatedmembership.object_id = object.object_id
			join relationship on relationship.bundle_id = relatedmembership.bundle_id
				and relationship.relationship_id = relatedmembership.relationship_id
			where relationship.type = 'IfcRelAssociatesMaterial'
		"""
		statement = text(statement_literal)
		result = session.exec(statement).all()
		relationshipsjson = [row.relationshipjson for row in result]
		return relationshipsjson
	except Exception as e:
		log.error(f'Error in get_envelope_materials: {e}')
		return None

def get_elements_to_add__recursion(session, bundleId, ele_args): 

    refs_to_add = ele_args['refs_to_add']
    elements_to_add = ele_args['elements_to_add']
    elements_not_found = ele_args['elements_not_found']
    
    useRepresentationsCache = ele_args['useRepresentationsCache']
        
    counter_add_representation = ele_args['counter_add_representation']
    counter_add_object = ele_args['counter_add_object']
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
            't1_start': t1_start,
            'times': times + 1
        }
        ele_args = get_elements_to_add__recursion(session, bundleId, ele_args)
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

######################
# 
#   Start the process
#
######################

def main_proc(task_dict):
    try:
        bundleId = task_dict['instruction_dict']['bundleId']
        useRepresentationsCache = task_dict['instruction_dict']['useRepresentationsCache']
        global PRINT
        PRINT = task_dict['debug']
        
        # !!! there is an issue with the materials; apparently they reference elements that are not there
        # !!! good to pursue this issue that may be related to the way the materials lose their style in the ifcJSON conversion

        MATERIALS = False
        
        csvFilePath = task_dict['csvFilePath']
        jsonFilePath = task_dict['jsonFilePath']
        engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')
        session = Session(engine)
        get_pset_for_external_elements_in_a_bundle(session, bundleId)
        get_external_elements_in_a_bundle(session, bundleId)
        save_external_elements_in_a_bundle_to_a_csv(session, bundleId, csvFilePath)
        header = getBundleHeader(session, bundleId)
        outJsonModel = dict(header)
        outJsonModel['data'] = list()
        # select parents (storeys) from the temporary table tt_envelope
        envelope_parents_ids , envelope_parents, envelope_parents_relationships = get_envelope_objects_parents(session, bundleId)
        # starting with those storey, collect the parent hierarchy data
        # list from child to parent
        list_of_parents = list()
        list_of_rel_aggregates = list()
        set_of_ids = set() 
        for i in range(len(envelope_parents_ids)):
            root = False
            obj_id = envelope_parents_ids[i]
            while root == False:
                rel_id, parentjson, relationshipjson = get_parent(session, bundleId, obj_id)
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
        envelope_objects = get_envelope_objects(session, bundleId)
		# select all representations for the objects
        envelope_representations = get_envelope_representations(session, bundleId) 
		# select all propertysets and corresponding relationships applying to the objects from the temporary table tt_envelope
        envelope_propertysets, envelope_propertysets_relationships = get_envelope_propertysets(session, bundleId)
		# select all material relationships for the objects in the temporary table tt_envelope
        if MATERIALS:
            envelope_materials_relationships = get_envelope_materials(session, bundleId)
        
        # get all the elements that are referenced in the objects that we have
        
        refs_in_objects = set()
        
        refs = get_refs_in_elements(envelope_objects)
        refs_in_objects.update(refs)
        
        refs = get_refs_in_elements(envelope_parents)
        refs_in_objects.update(refs)
        
        refs = get_refs_in_elements(list_of_parents)
        refs_in_objects.update(refs)               
        
        if PRINT:
            log.info(f'length of refs_in_objects: {len(refs_in_objects)}')
        
        if MATERIALS:
            refs_in_materials = set()
            refs = get_refs_in_elements(envelope_materials_relationships)
            refs_in_materials.update(refs)
            if PRINT:
                log.info(f'length of refs_in_materials: {len(refs_in_materials)}')
            
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
        if MATERIALS:
            refs_from_all.extend(refs_in_materials)
        
        refs_to_add = [item for item in refs_from_all if item not in refs_of_representations]
        if PRINT:
            log.info(f'length of refs_to_add: {len(refs_to_add)}')
    
        # get the elements to add  
        
        if  useRepresentationsCache:
            initialize_representations_cache(session, bundleId)  
       
        ele_args = {
            'refs_to_add': refs_to_add, 
            'elements_to_add': [],
            'elements_not_found': [],
            'useRepresentationsCache': useRepresentationsCache,
            'counter_add_representation': 0,
            'counter_add_object': 0,
            't1_start': 0,
            'times': 0
        }
        ele_args = get_elements_to_add__recursion(session, bundleId, ele_args)
        elements_to_add = ele_args['elements_to_add']
                                
        session.close()
        
        # need to prune the IfcRelDefinesByProperties for related elements that are not in the objects
        # create list of refs_of_objects
        refs_of_objects = set()
        for item in envelope_objects:
            refs_of_objects.add(item['globalId'])
        for item in envelope_propertysets_relationships:
            if item['type'] == 'IfcRelDefinesByProperties':
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
        if len(envelope_propertysets_relationships) != None:
            outJsonModel['data'].extend(envelope_propertysets_relationships)
            if PRINT:
                log.info(f'length of envelope_propertysets_relationships: {len(envelope_propertysets_relationships)}')

        if MATERIALS:
            if envelope_materials_relationships != None:
                outJsonModel['data'].extend(envelope_materials_relationships)
                if PRINT:
                    log.info(f'length of envelope_materials_relationships: {len(envelope_materials_relationships)}')
        
        if elements_to_add != None:
            outJsonModel['data'].extend(elements_to_add)
            if PRINT:
                log.info(f'length of elements_to_add: {len(elements_to_add)}')		
		#
        #   Write ifcJSON to a file
        #
        outFilePath = jsonFilePath
        # if in Jupyter notebook, use the following code
        indent=2
        with open(outFilePath, 'w') as outJsonFile:
            json.dump(outJsonModel, outJsonFile, indent=indent)
          
        #file_store.write_file(outFilePath, json.dumps(outJsonModel, indent=2))            
    except Exception as e:
        log.error(f'Error in main_proc: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict

# START THE PROCESS    
task_dict = {}
task_dict['instruction_dict'] = {} 
task_dict['debug'] = True
task_dict['instruction_dict']['bundleId'] = 66
task_dict['instruction_dict']['useRepresentationsCache'] = False
task_dict['csvFilePath'] = '/Users/lucvanlier/X_BIM_FILES/TEMP_FILES/CSV/'+str(task_dict['instruction_dict']['bundleId'])+'_external_elements_in_bundle.csv'
task_dict['jsonFilePath'] = '/Users/lucvanlier/X_BIM_FILES/TEMP_FILES/JSON/'+str(task_dict['instruction_dict']['bundleId'])+'_external_elements_in_bundle.json'

main_proc(task_dict)
