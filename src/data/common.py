import uuid
import pandas as pd
import json

from sqlmodel import select, delete, text, join

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
        statement = statement.where(model.bundleUnit.unit_type == unit_type)
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
