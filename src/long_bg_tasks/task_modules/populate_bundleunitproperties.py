##
#
#   Populate bundleunitproperties with the properties of elements 
#   of the units in the bundleunit of a bundle
#   
#   Objects are space boundary elements (walls, slabs, ...) or distribution elements contained in a space, storey or building. 
#   Their properties are propertysets associated to the space boundary objects or to the distributed elments
#   element. The objects can also have a type (e.g. an IfcWindow with an IfcWindowType and/or an IfcWindowStyle)
#   that in turn has properties which are not necessarily in a propertyset and do not adhere to te structure of a
#   propertyset and are found in the object table.
#   For propetyset we have
#   - propertyset_id
#   - propertyset_name
#   - propertyset_json
#   For objectTypes we have
#   - type_object_id
#   - type_object_type
#   - type_object_name
#   For properties of objectTypes found in the 'hasProperties' and not in the 
#   propertyset, we have
#   - properties_id
#   - properties_type
#   - properties_name
#   - properties_json
#      
##
from sqlmodel import Session, text
import uuid
import pandas as pd
import json
from pydantic import BaseModel
from time import perf_counter

# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter

from data import init2 as init

from data import insert_copy as data

from model.transform import PopulateBundleUnitProperties_Instruction, PopulateBundleUnitProperties_Result

# objectProperties: 
# set of properties that are not in a a propertyset
# but referred to in the hasProperties of an objectType or objectStyle
objectTypeProperties = (
    'IfcDoorLiningProperties',
    'IfcDoorPanelProperties',
    'IfcExtendedProperties',
    'IfcMaterialProperties',
    'IfcPermeableCoveringProperties',
    'IfcPreDefinedProperties',
    'IfcProfileProperties',
    'IfcReinforcementBarProperties',
    'IfcReinforcementDefinitionProperties',
    'IfcSectionProperties',
    'IfcSectionReinforcementProperties',
    'IfcWindowLiningProperties',
    'IfcWindowPanelProperties'
)

def get_bundleunits(session:Session, bundleId: str):
    # gets the bundle units of a bundle
    try:
        statement_literal = f"""select 
                bundleunit.bundle_id as bundle_id,
                bundleunit.unit_id as unit_id,
                bundleunit.unit_type as unit_type,
                bundleunit.unit_name as unit_name,
                bundleunit.parent_id as sz_id,
                bundleunit.parent_type as sz_type,
                bundleunit.parent_name as sz_name
            from bundleunit
            where bundleunit.bundle_id = '{bundleId}'
            """ 
        statement = text(statement_literal)
        result = session.exec(statement).all()
        result_list = [row for row in result]
        df_bundleunits = pd.DataFrame(result_list)
        return df_bundleunits
    except Exception as e:      
        raise Exception(f'exception in get_bundleunits: {e}')

def get_bu_boundaryElements(session:Session, bundleId: str):
    # gets the space boundary elements of for all spaces in a bundle
    try:
        statement_literal = f"""select 
                object.object_id as object_id, 
                object.type as object_type,
                object.name as object_name,
                parent.object_id as unit_id
            from object 
            join relatedmembership on relatedmembership.bundle_id = object.bundle_id
                and relatedmembership.object_id = object.object_id 
            join relationship on relationship.bundle_id = object.bundle_id  
                and relationship.relationship_id = relatedmembership.relationship_id
            join object as parent on parent.bundle_id = relationship.bundle_id
                and parent.object_id = relationship.relating_id
            where object.bundle_id = '{bundleId}' 
                and relationship.type = 'IfcRelSpaceBoundary'
            """
        statement = text(statement_literal)
        result = session.exec(statement).all()
        result_list = [row for row in result]
        df_bu_BoundaryElements = pd.DataFrame(result_list)
        return df_bu_BoundaryElements
    except Exception as e:      
        raise Exception(f'exception in get_spaceBoundaryElements: {e}')
    
def get_bu_distributionElements(session:Session, bundleId: str):
    # gets the distribution elements contained in a spatial structure in a bundle
    # containement can be at the space, storey or building level
    try:
        statement_literal = f"""select 
                object.object_id as object_id, 
                object.type as object_type,
                object.name as object_name,
                parent.object_id as unit_id
            from object 
            join relatedmembership on relatedmembership.bundle_id = object.bundle_id
                and relatedmembership.object_id = object.object_id 
            join relationship on relationship.bundle_id = object.bundle_id  
                and relationship.relationship_id = relatedmembership.relationship_id
            join object as parent on parent.bundle_id = relationship.bundle_id
                and parent.object_id = relationship.relating_id
            where object.bundle_id = '{bundleId}' 
                and relationship.type = 'IfcRelContainedInSpatialStructure'
                and object.type = 'IfcDistributionElement'
            """
        statement = text(statement_literal)
        result = session.exec(statement).all()
        result_list = [row for row in result]
        df_bu_distributionElements = pd.DataFrame(result_list)
        return df_bu_distributionElements
    except Exception as e:      
        raise Exception(f'exception in get_distributionElements: {e}')
    
    
def get_propertysets(session:Session, bundleId: str):
    try:
        statement_literal = f"""select distinct on (propertyset.propertyset_id)
                propertyset.propertyset_id as propertyset_id,
                propertyset.name as propertyset_name,
                propertyset.elementjson propertyset_json,
                object.object_id as object_id
            from propertyset
            join relationship on relationship.bundle_id = propertyset.bundle_id
                and relationship.relating_id = propertyset.propertyset_id
            join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id
                and relatedmembership.relationship_id = relationship.relationship_id
            join object on object.bundle_id = relatedmembership.bundle_id
                and object.object_id = relatedmembership.object_id
            where propertyset.bundle_id = '{bundleId}'
            """ 
        statement = text(statement_literal)
        result = session.exec(statement).all()
        result_list = [row for row in result]
        df_propertyset = pd.DataFrame(result_list)
        return df_propertyset
    except Exception as e:      
        raise Exception(f'exception in get_propertysets: {e}')

    
def get_objectTypes(session:Session, bundleId: str):
    try:
        statement_literal = f"""select distinct on (type_object.object_id)
            type_object.object_id as type_object_id,
            type_object.type as type_object_type,
            type_object.name as type_object_name,
            object.object_id as object_id
            from object as type_object
            join relationship on relationship.bundle_id = type_object.bundle_id
                and relationship.relating_id = type_object.object_id
            join relatedmembership on relatedmembership.bundle_id = relationship.bundle_id
                and relatedmembership.relationship_id = relationship.relationship_id
            join object on object.bundle_id = relatedmembership.bundle_id
                and object.object_id = relatedmembership.object_id
            where type_object.bundle_id = '{bundleId}'
            and relationship.type = 'IfcRelDefinesByType'
            """
        statement = text(statement_literal)
        results = session.exec(statement).all()
        result_list = [row for row in results]
        df_objectTypes = pd.DataFrame(result_list)
        return df_objectTypes      
    except Exception as e:      
        raise Exception(f'exception in get_objectTypes: {e}')
    
    
def get_propertiesOfObjectTypes(session:Session, bundleId: str):
    # only objectTypes are objects that can have an 'hasPropertySets'
    try:
        statement_literal = f"""select
                type_object.object_id as type_object_id,
                properties ->> 'ref' as propertyset_id
            from 
                object as type_object,
                json_array_elements(type_object.elementjson -> 'hasPropertySets') as properties
            where 
                type_object.bundle_id = '{bundleId}'
                and type_object.elementjson::jsonb ? 'hasPropertySets'
        """
        statement = text(statement_literal)
        results = session.exec(statement).all()
        result_list = [row for row in results]
        df_propertiesOfObjectTypes = pd.DataFrame(result_list)
        return df_propertiesOfObjectTypes
    except Exception as e:      
        raise Exception(f'exception in get_objectTypes: {e}')

def get_objectTypeProperties(session:Session, bundleId: str):
    try:
        statement_literal = f"""select distinct on (object.object_id)
            object.object_id::text as properties_id,
            object.type as properties_type,
            object.name as properties_name,
            object.elementjson as properties_json
            from object
            where object.bundle_id = '{bundleId}'
            and object.type in {objectTypeProperties}
            """
        statement = text(statement_literal)
        results = session.exec(statement).all()
        result_list = [row for row in results]
        df_objectTypeProperties = pd.DataFrame(result_list)
        return df_objectTypeProperties
    except Exception as e:
        raise Exception(f'exception in get_objectTypeProperties: {e}')

class PopulateBundleUnitProperties():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = PopulateBundleUnitProperties_Instruction(**self.task_dict['PopulateBundleUnitProperties_Instruction'])
            self.bundleId = instruction.bundleId
            self.task_dict['bundleId'] = instruction.bundleId
            self.ifNotPopulated = instruction.ifNotPopulated
            self.withCSVExport = instruction.withCSVExport
            self.TEMP_PATH = self.task_dict['TEMP_PATH']
            self.BASE_PATH = self.task_dict['BASE_PATH']
            self.EXPORT_PATH = self.task_dict['EXPORT_PATH']
            self.PRINT = self.task_dict['debug']
            self.start = perf_counter()
            if self.PRINT:
                print(f'>>>>> In PopulateBundleUnitProperties.init: {self.bundleId}') 
        except Exception as e:
            log.error(f'Error in PopulateBundleUnitProperties.init : {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in PopulateBundleUnitProperties.init : {e}'

    def populate(self):
        try:
            # test if the bundleunitproperties table is already populated for the given bundleId
            session = init.get_session()
            statement_literal = f"""select count(*) from bundleunitproperties where bundle_id = '{self.bundleId}'"""
            statement = text(statement_literal)
            result = session.exec(statement).one()[0]
            if result > 0:
                if self.ifNotPopulated:
                    session.close()
                    result = PopulateBundleUnitProperties_Result(
                        skipped = True,
                        nbrEntries = 0,
                        resultPath = ''
                    )
                    self.task_dict['result']['PopulateBundleUnitProperties_Result'] = result.dict()
                    return self.task_dict
                else:
                    statement_literal = f"""delete from bundleunitproperties where bundle_id = '{self.bundleId}'"""
                    statement = text(statement_literal)
                    session.exec(statement)
                    session.commit() 
                    session.close() 
            # get the bundle units  
            session = init.get_session() 
            df_bundleunits = get_bundleunits(session, self.bundleId)
            df_bu_boundaryElements = get_bu_boundaryElements(session, self.bundleId)
            df_bu_distributionElements = get_bu_distributionElements(session, self.bundleId)
            df_propertysets = get_propertysets(session, self.bundleId)
            df_objectTypes = get_objectTypes(session, self.bundleId)
            df_propertiesOfObjectTypes = get_propertiesOfObjectTypes(session, self.bundleId)
            df_objectTypeProperties = get_objectTypeProperties(session, self.bundleId)
            if self.PRINT:
                log.info(f'len(df_bundleunits): {len(df_bundleunits)}')
                log.info(f'len(df_boundaryElements): {len(df_bu_boundaryElements)}')
                log.info(f'len(df_distributionElements): {len(df_bu_distributionElements)}')
                log.info(f'len(df_propertysets): {len(df_propertysets)}')
                log.info(f'len(df_objectTypes): {len(df_objectTypes)}')
                log.info(f'len(df_propertiesOfObjectTypes): {len(df_propertiesOfObjectTypes)}')
                log.info(f'len(df_objectTypeProperties): {len(df_objectTypeProperties)}')
                filePath = f'{self.TEMP_PATH}{uuid.uuid4()}_propertiesOfObjectTypes.csv'
                df_propertiesOfObjectTypes.to_csv(filePath, index=False, encoding='utf-8')
                filePath = f'{self.TEMP_PATH}{uuid.uuid4()}_objectTypeProperties.csv'
                df_objectTypeProperties.to_csv(filePath, index=False, encoding='utf-8')   
            df_1 = df_bundleunits.merge(df_bu_boundaryElements, on="unit_id", how="left")
            if len(df_bu_distributionElements) > 0:
                df_2 = df_bundleunits.merge(df_bu_distributionElements, on="unit_id", how="inner")
                df_3 = pd.concat([df_1, df_2])
            else:
                df_3 = df_1
            df_4 = df_3.merge(df_propertysets, on="object_id", how="left")
            if self.PRINT:
                log.info(f'df_1 = df_bundleunits.merge(df_bu_boundaryElements, on="unit_id", how="left"): {len(df_1)}')
                if len(df_bu_distributionElements) > 0:
                    log.info(f'df_2 = df_bundleunits.merge(df_bu_distributionElements, on="unit_id", how="inner"): {len(df_2)}')
                else: 
                    log.info(f'df_2 = df_bundleunits.merge(df_bu_distributionElements, on="unit_id", how="inner"): 0')
                log.info(f'df_3 = pd.concat([df_1, df_2]): {len(df_3)}')
                log.info(f'df_4 = df_3.merge(df_propertysets, on="object_id", how="left"): {len(df_4)}')
            if len(df_objectTypes) > 0:
                df_5 = df_3.merge(df_objectTypes, on="object_id", how="inner")
                df_6a = df_propertiesOfObjectTypes.merge(df_propertysets, on="propertyset_id", how="inner")
                df_6b = pd.merge(df_propertiesOfObjectTypes, df_objectTypeProperties, left_on="propertyset_id", right_on="properties_id", how="inner")
                df_6 = pd.concat([df_6a, df_6b])
                df_7 = df_5.merge(df_6, on="type_object_id", how="left")
                df_8 = pd.concat([df_4, df_7])
                if self.PRINT:
                    log.info(f'df_5 = df_3.merge(df_objectTypes, on="object_id", how="inner"): {len(df_5)}')
                    log.info(f'df_6a = df_propertiesOfObjectTypes.merge(df_propertysets, on="propertyset_id", how="inner"): {len(df_6a)}')
                    log.info(f'df_6b = pd.merge(df_propertiesOfObjectTypes, df_objectTypeProperties, left_on="propertyset_id", right_on="properties_id", how="inner"): {len(df_6b)}')
                    log.info(f'df_6 = pd.concat([df_6a, df_6b]): {len(df_6)}')
                    log.info(f'df_7 = df_5.merge(df_6, on="type_object_id", how="left"): {len(df_7)}')
                    log.info(f'df_8 = pd.concat([df_4, df_7]): {len(df_8)}')
                    filePath = f'{self.TEMP_PATH}{uuid.uuid4()}_df_6b.csv'
                    df_6b.to_csv(filePath, index=False, encoding='utf-8')   
            else:
                df_8 = df_4
            df_9 = df_8[["bundle_id", "sz_id", "sz_type", "sz_name" , "unit_id", "unit_name", "unit_type", "object_id", "object_type", "object_name", "type_object_id", "type_object_type", "type_object_name", "propertyset_id", "propertyset_name", "propertyset_json", "properties_id", "properties_type", "properties_name", "properties_json"]]
            df_10 = df_9.sort_values(["sz_id", "unit_id", "object_id", "type_object_id", "propertyset_id", "properties_id"])
            df_11 = df_10.drop_duplicates(subset=["sz_id", "unit_id", "object_id", "type_object_id", "propertyset_id"], keep="first")
            df_11.insert(len(df_11.columns), 'created_at', pd.Timestamp.now())
            df_11.insert(0,'id', df_11.apply(lambda _: uuid.uuid4(), axis=1))
            # format for json columns
            df_11 = self.formatForJson(df_11)
            if self.PRINT:
                log.info(f'df_9 = df_8[["bundle_id", "sz_id", "sz_type", "sz_name" , "unit_id", "unit_name", "unit_type", "object_id", "object_type", "object_name", "type_object_id", "type_object_type", "type_object_name", "propertyset_id", "propertyset_name", "propertyset_json", "properties_id", "properties_type", "properties_name", "properties_json"]]: {len(df_9)}')
                log.info(f'df_10 = df_9.sort_values(["sz_id", "unit_id", "object_id", "propertyset_id"]): {len(df_10)}')     
                log.info(f'df_11 = df_10.drop_duplicates(subset=["sz_id", "unit_id", "object_id", "type_object_id", "propertyset_id"], keep="first"): {len(df_11)}')    
            if self.withCSVExport:
                result_rel_path = f'{self.EXPORT_PATH}{uuid.uuid4()}_{self.bundleId}_bundleunitproperties.csv' 
                result_path = f'{self.BASE_PATH}{result_rel_path}'    
                df_11.to_csv(result_path, index=False, sep=';', encoding='utf-8')
            
            # data.bulk_insert_df(df_11, 'bundleunitproperties')
            self.save_dataframe_to_sql_in_chunks(df_11, 'bundleunitproperties')
            statement_literal = f"""select count(*) from bundleunitproperties where bundle_id = '{self.bundleId}'"""
            statement = text(statement_literal)
            nbrEntries = session.exec(statement).one()[0]
            result = PopulateBundleUnitProperties_Result(
                skipped = False,
                nbrEntries = nbrEntries,
                resultPath = result_rel_path,
                runtime = round(perf_counter() - self.start, 2)
            )
            self.task_dict['result']['PopulateBundleUnitProperties_Result'] = result.dict()
        except Exception as e:
            log.error(f'Error in PopulateBundleUnitProperties.populate: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in PopulateBundleUnitProperties.populate: {e}'
            session.close()    
        return self.task_dict
                
    def formatForJson(self, df):
        df['propertyset_json'] = df['propertyset_json'].apply(lambda x: json.dumps(x) if len(str(x)) != 3 and len(str(x))!=4 else '{}')
        df['properties_json'] = df['properties_json'].apply(lambda x: str(x).replace("'",'"') if len(str(x)) != 3 and len(str(x))!=4 else '{}')
        return df
    
    def save_dataframe_to_sql_in_chunks(self, df, table_name, chunk_size=2000):
        try:
            for start in range(0, len(df), chunk_size):
                end = start + chunk_size
                chunk = df.iloc[start:end]
                data.bulk_insert_df(chunk, table_name)
                if self.PRINT:
                    log.info(f'Saved rows {start} to {end} into table {table_name}')
        except Exception as e:
            raise Exception(f'Error in save_dataframe_to_sql_in_chunks: {e}')