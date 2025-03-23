import uuid
import pandas as pd
import json

from sqlmodel import select, delete, text

from model import common as model
from data import init

PRINT=False

# Set up the logging
import logging
log = logging.getLogger(__name__)
              
async def createSpatialUnit(spatialUnit:model.spatialUnit):
    session = init.get_session()
    spatialUnit = model.spatialUnit(
        id=uuid.uuid4(),
        name=spatialUnit.name,
        type=spatialUnit.type,
        description=spatialUnit.description,
        unit_guide=spatialUnit.unit_guide
    )
    spatialUnitId = str(spatialUnit.id)
    session.add(spatialUnit)
    session.commit()
    session.close()
    return spatialUnitId

def getBundleByName(name:str):
    session = init.get_session()
    bundle = session.exec(select(model.bundle).where(model.bundle.name == name)).one()
    session.close()
    return bundle

def getBundleById(id:str):
    session = init.get_session()
    bundle = session.exec(select(model.bundle).where(model.bundle.bundle_id == int(id))).one()
    session.close()
    return bundle

def deleteBundleById(id:str):
    try:
        session = init.get_session()   
        session.exec(delete(model.bundle).where(model.bundle.bundle_id == int(id)))
        session.exec(delete(model.spatialUnitBundleUnit).where(model.spatialUnitBundleUnit.bundle_id == int(id)))
        session.exec(delete(model.bundleJournal).where(model.bundleJournal.bundle_id == int(id)))
        session.exec(delete(model.bundleUnit).where(model.bundleUnit.bundle_id == int(id)))    
        session.exec(delete(model.object).where(model.object.bundle_id == int(id)))
        session.exec(delete(model.representation).where(model.representation.bundle_id == int(id)))
        session.exec(delete(model.relationship).where(model.relationship.bundle_id == int(id)))
        session.exec(delete(model.relatedMembership).where(model.relatedMembership.bundle_id == int(id)))
        session.exec(delete(model.propertySet).where(model.propertySet.bundle_id == int(id)))
        session.exec(delete(model.bundleUnitProperties).where(model.bundleUnitProperties.bundle_id == int(id)))
        session.commit()
        session.close()
    except Exception as e:
        log.error(f'Error in deleteBundleById: {e}')
    return

def setPRINT(value:bool):
    global PRINT
    PRINT = value
    return  

def getBundleTree(bundleId:int):
    return

class Db_Object():
    def __init__(self, session, bundleId, objectId, type, name, longName, objectType, objectRepresentation, created_at):
        self.session = session
        self.bundleId = bundleId
        self.objectId = objectId
        self.type = type
        self.name = name
        self.longName = longName
        self.objectType = objectType
        self.objectRepresentation = objectRepresentation
        self.representation_ids = []
        self.elementjson = {}
        self.created_at = created_at
        self.set_format_and_store()
    
    def set_format_and_store(self):
        try:
            if self.objectId == '':
                self.objectId = str(uuid.uuid4())
            representation = {"type": "IfcProductDefinitionShape", "representations": []}
            if self.objectRepresentation != []:
                representation_ids = []
                for item in self.objectRepresentation:
                    rep = json.loads(item)
                    representation['representations'].extend(rep['representations'])
                    representation_ids.extend([x['ref'] for x in rep['representations']])
            self.elementjson = {
                'type':self.type,
                'globalId':self.objectId,
                'name':self.name,
                'longName':self.longName,
                'objectType':self.objectType,
                'representation':representation
            }
            self.elementjson = str(self.elementjson).replace("'", '"')
            self.representation_ids = str(self.representation_ids).replace("'", '"')
            self.representation_ids = '{' + self.representation_ids[1:-1] + '}'
            self.store()
            return
        except Exception as e:
            log.error(f'Error in Db_Object.set_format: {e}')
            raise Exception(f'Error in Db_Object.set_format: {e}') 
        
    def store(self):
        if PRINT:
            log.info(f'Creating Object: {self.type} - {self.objectId} - {self.name}')
        try:
            statement_literal = f"""
                insert into object (bundle_id, object_id, type, name, representation_ids, elementjson, created_at)
                values ({self.bundleId}, '{self.objectId}','{self.type}','{self.name}','{self.representation_ids}','{self.elementjson}', '{self.created_at}' )
                """
            statement = text(statement_literal)
            if PRINT: 
                log.info(statement_literal)
                return
            self.session.exec(statement)
            return
        except Exception as e:
            log.error(f'Error in Object.store: {e}')
            raise Exception(f'Error in Object.store: {e}')

    
class Db_Relationship():
    def __init__(self, session, bundleId, relationshipId, relationshipType, relatingType, relatingId, relatedType, relatedList, created_at):
        self.session = session
        self.bundleId = bundleId
        self.relationshipId = relationshipId
        self.relationshipType = relationshipType
        self.relatingType = relatingType
        self.relatingId = relatingId
        self.relatedType = relatedType 
        self.relatedList = relatedList
        self.elementjson = {}
        self.created_at = created_at
        self.set_format_and_store()
        
    def set_format_and_store(self):
        if PRINT:
            log.info(f'Creating Relationship: {self.relatingType} - {self.relatingId} - {self.relatedType} with Id: {self.relationshipId}')
        try:
            if self.relationshipId == '':
                self.relationshipId = str(uuid.uuid4())
            if PRINT:
                log.info(f'Creating Relationship: {self.relatingType} - {self.relatingId} - {self.relatedType} with Id: {self.relationshipId}')
            relating_dict = {
                "type":self.relatingType, 
                "ref":self.relatingId
            }
            related_objects = []
            for item in self.relatedList:
                related_objects.append({'type':self.relatedType, 'ref':item})
            self.elementjson = {
                'type':self.relationshipType,
                'globalId':self.relationshipId,
                'relatingObject':relating_dict,
                'relatedObjects':related_objects
            }
            self.elementjson = str(self.elementjson).replace("'", '"') 
            self.store()   
            return
        except Exception as e:
            log.error(f'Error in Db_Relationship.set_format_and_store: {e}')
            raise Exception(f'Error in Db_Relationship.set_format_and_store: {e}')
        
    def store(self):
        try:
            statement_literal = f"""
                insert into relationship (bundle_id, relationship_id, type, relating_type, relating_id, elementjson, created_at)
                values ({self.bundleId},'{self.relationshipId}', '{self.relationshipType}', '{self.relatingType}', '{self.relatingId}', '{self.elementjson}', '{self.created_at}')
                """
            statement = text(statement_literal)
            if PRINT: 
                log.info(statement_literal)
                return
            self.session.exec(statement)
            return
        except Exception as e:
            log.error(f'Error in Relationship.store: {e}')
            raise Exception(f'Error in Relationship.store: {e}')
        
class Db_RelatedMembership():
    def __init__(self, session, bundleId, relationshipId, objectType, objectList, created_at):
        self.session = session
        self.bundleId = bundleId
        self.relationshipId = relationshipId
        self.objectType = objectType
        self.objectList = objectList
        self.created_at = created_at
        self.set_format_and_store()
        
    def set_format_and_store(self):
        if PRINT:
            log.info(f'Creating RelatedMembership: {self.objectType} - {self.objectList}')
        try:
            values = []
            for item in self.objectList:
                id = str(uuid.uuid4())
                values.append((id, self.bundleId, self.relationshipId, self.objectType, item, str(self.created_at)))
            values_str = ','.join([str(v) for v in values])
            statement_literal = f"""
                insert into relatedmembership (id, bundle_id, relationship_id, object_type, object_id, created_at)
                values {values_str}
                """
            statement = text(statement_literal)
            if PRINT: 
                log.info(statement_literal)
                return
            self.session.exec(statement)
            return
        except Exception as e:
            log.error(f'Error in RelatedMembership.set_format_and_store: {e}')
            raise Exception(f'Error in RelatedMembership.set_format_and_store: {e}')
