import uuid
import pandas as pd
import json

from sqlmodel import select, delete, text, join
from sqlalchemy import update

from model import common as model
from data import init

PRINT=False

# Set up the logging
import logging
from datetime import datetime
import pytz
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

async def readBundleList(from_date: str | None = None, to_date: str | None=None, limit:int = 100):
    session = init.get_session()
    statement = select(model.bundle).order_by(model.bundle.created_at.desc()).limit(limit)
    if from_date is not None:
        from_date = datetime.strptime(from_date, "%Y-%m-%d").astimezone(pytz.utc)
        statement = statement.where(model.bundle.created_at >= from_date)
    if to_date is not None:
        to_date = datetime.strptime(to_date, "%Y-%m-%d").astimezone(pytz.utc)
        statement = statement.where(model.bundle.created_at <= to_date)
    bundleList = session.exec(statement).all()
    session.close()
    if bundleList is None:
        return None
    result = []
    for row in bundleList:
        bundle = {}
        bundle['bundle_id'] = row.bundle_id
        bundle['name'] = row.name
        bundle['source_type'] = row.source_type
        bundle['created_at'] = row.created_at
        bundle['created_by'] = row.created_by
        bundle['updated_at'] = row.updated_at
        bundle['updated_by'] = row.updated_by
        result.append(bundle)
    return result

async def readBundle(bundle_id: str):
    session = init.get_session()
    statement = select(model.bundle).where(model.bundle.bundle_id == int(bundle_id))
    bundle = session.exec(statement).one()
    session.close()
    if bundle is None:
        return None
    return bundle

async def readBundleJournal(bundle_id: str, from_date: str | None = None, to_date: str | None=None, limit:int = 100):
    session = init.get_session()
    statement = select(model.bundleJournal).where(model.bundleJournal.bundle_id == int(bundle_id)).order_by(model.bundleJournal.created_at.desc()).limit(limit)
    if from_date is not None:
        from_date = datetime.strptime(from_date, "%Y-%m-%d").astimezone(pytz.utc)
        statement = statement.where(model.bundleJournal.created_at >= from_date)
    if to_date is not None:
        to_date = datetime.strptime(to_date, "%Y-%m-%d").astimezone(pytz.utc)
        statement = statement.where(model.bundleJournal.created_at <= to_date)
    bundleJournalItems = session.exec(statement).all()
    session.close()
    if bundleJournalItems is None:
        return None
    return bundleJournalItems

async def readBundleJournalByToken(proctoken:str):
    session = init.get_session()
    statement = select(model.bundleJournal).where(model.bundleJournal.proctoken == uuid.UUID(proctoken)).order_by(model.bundleJournal.created_at.desc())
    bundleJournalItems = session.exec(statement).all()
    session.close()
    if bundleJournalItems is None:
        return None
    return bundleJournalItems

async def readSpatialUnitList(from_date: str | None = None, to_date: str | None=None, limit:int = 100):
    session = init.get_session()
    statement = select(model.spatialUnit).order_by(model.spatialUnit.created_at.desc()).limit(limit)
    if from_date is not None:
        from_date = datetime.strptime(from_date, "%Y-%m-%d").astimezone(pytz.utc)
        statement = statement.where(model.spatialUnit.created_at >= from_date)
    if to_date is not None:
        to_date = datetime.strptime(to_date, "%Y-%m-%d").astimezone(pytz.utc)
        statement = statement.where(model.spatialUnit.created_at <= to_date)
    spatialUnitList = session.exec(statement).all()
    session.close()
    if spatialUnitList is None:
        return None
    return spatialUnitList

async def readSpatialUnit(spatial_unit_id: str):
    session = init.get_session()
    statement = select(model.spatialUnit).where(model.spatialUnit.id == spatial_unit_id)
    spatialUnit = session.exec(statement).one()
    session.close()
    if spatialUnit is None:
        return None
    return spatialUnit

async def readSpatialUnitRootBundleUnitList(spatial_unit_id: str):
    session = init.get_session()
    statement_literal = f"""
        select spatialunit.id::text as spatialunit_id,
        spatialunit.name as spatialunit_name,
        spatialunit.type as spatialunit_type, 
        spatialunitbundleunit.bundle_id::text as bundle_id,
        spatialunitbundleunit.bundleunit_id::text as bundleunit_id,
        bundleunit.unit_id::text as unit_id,
        bundleunit.unit_type::text as unit_type,
        bundleunit.unit_name::text as unit_name,
        bundleunit.unit_longname::text as unit_longname
        bundleunit.unit_objecttype::text as unit_objecttype
        from spatialunit
        join spatialunitbundleunit on spatialunit.id = spatialunitbundleunit.spatial_unit_id
        join bundleunit on spatialunitbundleunit.bundleunit_id = bundleunit.bundleunit_id
        where spatialunit.id = '{spatial_unit_id}'
        order by spatialunitbundleunit.created_at desc
        """
    statement = text(statement_literal)
    spatialUnitRootBundleUnitList = session.exec(statement).all()
    session.close()
    if spatialUnitRootBundleUnitList is None:
        return None
    result = []
    for row in spatialUnitRootBundleUnitList:
        spatialUnitBundle = {}
        spatialUnitBundle['spatialunit_id'] = row.spatialunit_id
        spatialUnitBundle['spatialunit_name'] = row.spatialunit_name
        spatialUnitBundle['spatialunit_type'] = row.spatialunit_type
        spatialUnitBundle['bundle_id'] = row.bundle_id
        spatialUnitBundle['bundleunit_id'] = row.bundleunit_id
        spatialUnitBundle['unit_id'] = row.unit_id
        spatialUnitBundle['unit_type'] = row.unit_type
        spatialUnitBundle['unit_name'] = row.unit_name
        spatialUnitBundle['unit_longname'] = row.unit_longname
        spatialUnitBundle['unit_objecttype'] = row.unit_objecttype
        result.append(spatialUnitBundle)
    return result


async def readBundleUnitList(bundle_id: str, unit_type: str | None = None):
    session = init.get_session()
    statement = select(model.bundleUnit).where(model.bundleUnit.bundle_id == int(bundle_id)).order_by(model.bundleUnit.created_at.desc())
    if unit_type is not None:
        unit_type = unit_type.translate({ord(i): None for i in '"[]'})
        unit_type = unit_type.split(",")
        print(f'unit_type: {unit_type}')
        statement = statement.where(model.bundleUnit.unit_type.in_(unit_type))
    bundleUnitList = session.exec(statement).all()
    session.close()
    if bundleUnitList is None:
        return None
    result = []
    for row in bundleUnitList:
        bundleUnit = {}
        bundleUnit['bundleunit_id'] = row.bundleunit_id
        bundleUnit['bundle_id'] = row.bundle_id
        bundleUnit['unit_id'] = row.unit_id
        bundleUnit['unit_type'] = row.unit_type
        bundleUnit['unit_name'] = row.unit_name
        bundleUnit['unit_longname'] = row.unit_longname
        bundleUnit['unit_objecttype'] = row.unit_objecttype
        bundleUnit['parent_id'] = row.parent_id
        bundleUnit['parent_type'] = row.parent_type
        bundleUnit['parent_name'] = row.parent_name
        bundleUnit['parent_longname'] = row.parent_longname
        bundleUnit['created_at'] = row.created_at
        bundleUnit['created_by'] = row.created_by
        bundleUnit['updated_at'] = row.updated_at
        bundleUnit['updated_by'] = row.updated_by
        result.append(bundleUnit)
    return result    

async def readBundleUnit(bundle_id: str, unit_id: str):
    session = init.get_session()
    statement = select(model.bundleUnit).where(model.bundleUnit.bundle_id == int(bundle_id), model.bundleUnit.unit_id == unit_id)
    bundleUnit = session.exec(statement).one()
    session.close()
    if bundleUnit is None:
        return None
    return bundleUnit

async def readBundleUnitProperties(bundle_id:str, sz_id:str, unit_type:str | None = None, unit_id:str | None = None, object_type: str | None = None, propertyset_name: str | None = None, properties_type: str | None = None, limit:int = 100):
    session = init.get_session()
    statement = select(model.bundleUnitProperties).where(model.bundleUnitProperties.bundle_id == int(bundle_id), model.bundleUnitProperties.sz_id == sz_id).limit(limit)
    statement = statement.order_by(model.bundleUnitProperties.unit_type.asc(),model.bundleUnitProperties.unit_name.asc())
    statement = statement.order_by(model.bundleUnitProperties.object_type.asc(),model.bundleUnitProperties.object_name.desc())
    if unit_type is not None:
        statement = statement.where(model.bundleUnitProperties.unit_type == unit_type)
    if unit_id is not None:
        statement = statement.where(model.bundleUnitProperties.unit_id == unit_id)
    if object_type is not None:
        statement = statement.where(model.bundleUnitProperties.object_type == object_type)
    if propertyset_name is not None:
        statement = statement.where(model.bundleUnitProperties.propertyset_name == propertyset_name)
    if properties_type is not None:
        statement = statement.where(model.bundleUnitProperties.properties_type == properties_type)
    bundleUnitProperties = session.exec(statement).all()
    session.close()
    if bundleUnitProperties is None:
        return None
    return bundleUnitProperties

async def readObject(bundle_id:str, object_id:str):
    session = init.get_session()
    statement = select(model.object).where(model.object.bundle_id == int(bundle_id), model.object.object_id == object_id)
    ifcobject = session.exec(statement).one()
    session.close()
    if ifcobject is None:
        return None
    return ifcobject

async def updateObject(bundle_id:str, object_id:str, ifcobject:model.UpdateObject):
    session = init.get_session()
    statement = select(model.object).where(model.object.bundle_id == int(bundle_id), model.object.object_id == object_id)
    existing_object = session.exec(statement).one()
    if existing_object is None:
        session.close()
        return None
    # Create the beforeimage in the elementhistory table
    elementHist = model.elementHistory(
        id = uuid.uuid4(),
        bundle_id = existing_object.bundle_id,
        element_id = existing_object.object_id,
        element_type = existing_object.type,
        elementjson = existing_object.elementjson
    )
    session.add(elementHist)
    # Update the existing object with the new values
    if ifcobject.name is not None:
        existing_object.name = ifcobject.name
    if ifcobject.representation_ids is not None:
        existing_object.representation_ids = ifcobject.representation_ids
    if ifcobject.elementjson is not None:
        existing_object.elementjson = ifcobject.elementjson
    existing_object.updated_at = datetime.now(pytz.utc)  
    session.add(existing_object)
    session.commit()
    session.refresh(existing_object) 
    session.close()
    return existing_object

##
#
# Create SpatialZones
#
##
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
        results = self.session.exec(select(model.relationship).where(model.relationship.bundle_id == int(self.bundleId), model.relationship.relating_id==self.relatingId, model.relationship.type==self.relationshipType))
        try:
            self.relationship = results.one()
        except Exception as e:
            self.relationship = None
            print(f'>>> No relationship found by Db_Relationship in relationship for bundle_id:{self.bundleId}, relating_id:{self.relatingId}, relating_type:{self.relatingType}, relationshipType:{self.relationshipType}')
        if self.relationship is not None:
            # update the relationship 
            self.relationshipId = self.relationship.relationship_id           
            self.merge_and_update()
        else:
        # create a new relationship
            self.set_format_and_store()
    
    def merge_and_update(self):
        if PRINT:
            log.info(f'Updating Relationship: {self.relatingType} - {self.relatingId} - {self.relatedType} with Id: {self.relationshipId} and type: {self.relationshipType}')
        try:
            self.elementjson = self.relationship.elementjson
            print(f'\n>>> self.elementjson (before update): {self.elementjson}\n')
            related_objects = []
            for item in self.relatedList:
                related_objects.append({'type':self.relatedType, 'ref':item})
            self.elementjson['relatedObjects'].extend(related_objects)    
            self.relationship.elementjson = self.elementjson
            print(f'\n>>> self.relationship.elementjson (for update): {self.relationship.elementjson}\n')
            self.relationship.updated_at=self.created_at
            self.session.add(self.relationship) # update the relationship doesn't work !!!
            self.session.execute(
                update(model.relationship)
                .where(model.relationship.bundle_id == int(self.bundleId), model.relationship.relationship_id==self.relationshipId)
                .values(
                    elementjson=self.elementjson,
                    updated_at=self.created_at
                )
                .execution_options(synchronize_session="fetch")
            )   
        except Exception as e:
            log.error(f'Error in Db_Relationship.merge_and_update: {e}')
            raise Exception(f'Error in Db_Relationship.merge_and_update: {e}')
     
    def set_format_and_store(self):
        if PRINT:
            log.info(f'Creating Relationship: {self.relatingType} - {self.relatingId} - {self.relatedType} with Id: {self.relationshipId}')
        try:
            self.relationshipId = str(uuid.uuid4())
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
            self.relationship = model.relationship(
                bundle_id=self.bundleId,
                relationship_id=self.relationshipId,
                type=self.relationshipType,
                relating_type=self.relatingType,
                relating_id=self.relatingId,
                elementjson=self.elementjson,
                created_at=self.created_at
            ) 
            self.session.add(self.relationship)  
            return
        except Exception as e:
            log.error(f'Error in Db_Relationship.set_format_and_store: {e}')
            raise Exception(f'Error in Db_Relationship.set_format_and_store: {e}')
        
        
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
