from sqlmodel import SQLModel, Field, Column, ARRAY, String, TEXT
from typing import Optional, Any
import datetime
from pydantic import BaseModel, UUID4, UUID5
from sqlalchemy import JSON, Column, DateTime, func, Sequence, Integer, Index
from sqlalchemy.ext.mutable import MutableDict
from geoalchemy2 import Geometry
from humps import camelize

def to_camel(string):
    return camelize(string)

class ifctype(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    type: str = Field(nullable=False)
    typename:str = Field(nullable=False)
    category: str = Field(nullable=False)
    description: str = Field(sa_column=Column(TEXT))
    attributes: list[str] = Field(sa_column=Column(ARRAY(String(80))))
    

#================================================================================================
# Common tables for the database
#================================================================================================
class spatialUnit(SQLModel, table=True):
    class Config:
        alias_generator = to_camel
        populate_by_name = True
        
    id: None | UUID4 = Field(primary_key=True)
    name: str = Field(nullable=False, index=True)
    type: str = Field(nullable=False)
    description: str = Field(sa_column=Column(TEXT))
    unit_guide: dict = Field(sa_column=Column(JSON), alias='unitGuide', default={})
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')
    __table_args__ = (
            Index("spatialunit_name_idx", "name", postgresql_using="btree"), 
    )
# can also be created with
# CREATE INDEX IF NOT EXISTS spatialunit_name_idx ON spatialunit (name);

class CreateSpatialUnit(BaseModel): # model for the request
    name: str | None = "Test Spatial Unit"
    type: str | None = "Building"
    description: str | None = "This is a test spatial unit"
    unitGuide: dict | None = {'overview': 'this is a test spatial unit'}
    
class bundle(SQLModel, table=True):
    bundle_id: int = Field(sa_column=Column("bundle_id", Integer, Sequence("bundle_id_seq", start=1),primary_key=True))
    parent_id: int = Field(nullable=True, index=True, alias='parentId')
    name: str = Field(nullable=False, index=True)
    source_type: str = Field(nullable=False, alias='sourceType')
    files: dict = Field(sa_column=Column(JSON), default={})
    header: dict = Field(sa_column=Column(JSON), default={})
    description: str = Field(sa_column=Column(TEXT))
    active: bool = Field(default=True)
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')

# The field unitjson must be set as 'mutable' for sqlalchemy to be able to see the update of the json field
# and to secure that the field is updated in the database  
class bundleUnit(SQLModel, table=True):
    bundleunit_id: UUID5 = Field(primary_key=True, alias='bundleUnitId')
    bundle_id: int = Field(nullable=False, alias='bundleId')
    unit_id: UUID4 = Field(nullable=False, alias='unitId')
    unit_type: str = Field(nullable=False, alias='unitType')
    unit_name: str = Field(nullable=True, alias='unitName')
    unit_longname: str = Field(nullable=True, alias='unitLongName')
    unit_objecttype: str = Field(nullable=True, alias='unitObjectType')
    relationship_id: UUID4 = Field(nullable=False, alias='relationshipId')
    relationship_type: str = Field(nullable=False, alias='relationshipType')
    parent_id: UUID4 = Field(nullable=False, alias='parentId')
    parent_type: str = Field(nullable=False, alias='parentType')
    parent_name: str = Field(nullable=True, alias='parentName')
    parent_longname: str = Field(nullable=True, alias='parentLongName')
    unitjson: dict = Field(sa_column=Column(MutableDict.as_mutable(JSON)), default={}, alias='unitJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')
    __table_args__ = (
        Index("bundleunit_unit_idx", "bundle_id", "unit_id", postgresql_using="btree"), 
    )
# can also be created with
# CREATE INDEX IF NOT EXISTS bundleunit_unit_idx ON bundleunit (bundle_id, unit_id); 

# team_id: int | None = Field(default=None, foreign_key="team.id")
class spatialUnitBundleUnit(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    spatial_unit_id: UUID4 = Field(nullable=False, alias='spatialUnitId', index=True)
    bundle_id: int = Field(nullable=False, alias='bundleId', index=True)
    bundleunit_id: UUID5 = Field(nullable=False, alias='bundleUnitEdgeId', index=True)
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')
    
class object(SQLModel, table=True):
    bundle_id: int = Field(nullable=False, alias='bundleId', primary_key=True)
    object_id: UUID4 = Field(primary_key=True)
    type: str = Field(nullable=False)
    name: str = Field(nullable=True) # have object with no name (e.g. IfcOpeningElement)
    representation_ids: list[str] = Field(sa_column=Column(ARRAY(String(36))), default=[], alias='representationIds')
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')
    
class UpdateObject(BaseModel): # model for the request
    name: str | None = None
    representation_ids: list[str] | None = None
    elementjson: dict | None = None

class representation(SQLModel, table=True):
    bundle_id: int = Field(nullable=False, alias='bundleId', primary_key=True)
    representation_id: UUID4 = Field(primary_key=True)
    type: str = Field(nullable=False)
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    geom: Optional[Any] = Field(sa_column=Column(Geometry(), nullable=True))
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')


class propertySet(SQLModel, table=True):
    bundle_id: int = Field(nullable=False, alias='bundleId', primary_key=True)
    propertyset_id: UUID4 = Field(primary_key=True)
    name: str = Field(nullable=False)
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')

    
class relationship(SQLModel, table=True):
    bundle_id: int = Field(nullable=False, alias='bundleId', primary_key=True)
    relationship_id: UUID4 = Field(primary_key=True)
    type: str = Field(nullable=False)
    relating_type: str = Field(nullable=False, alias='relatingType')
    relating_id: UUID4 = Field(nullable=True, alias='relatingId')
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')
    __table_args__ = (
        Index("relationship_relating_idx", "bundle_id", "relating_id", postgresql_using="btree"), 
    )
  
# can also be created with
# CREATE INDEX IF NOT EXISTS relationship_relating_idx ON relationship (bundle_id, relating_id);
    
class relatedMembership(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    bundle_id: int = Field(nullable=False, alias='bundleId')    
    relationship_id: UUID4 = Field(nullable=False, alias='relationshipId')
    object_type: str = Field(nullable=False, alias='objectType')
    object_id: UUID4 = Field(nullable=False, alias='objectId')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')
    __table_args__ = (
        Index('relatedmembership_object_idx', 'bundle_id', 'object_id', postgresql_using='btree'),
        Index('relatedmembership_relationship_idx', 'bundle_id', 'relationship_id', postgresql_using='btree') 
    )
    
# can also be created with
# CREATE INDEX IF NOT EXISTS relatedmembership_object_idx ON relatedmembership (bundle_id, object_id); 
# CREATE INDEX IF NOT EXISTS relatedmembership_relationship_idx ON relatedmembership (bundle_id, relationship_id);
    
class bundleHistory(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    bundle_id: int = Field(nullable=False, alias='bundleId', index=True)
    parent_id: int = Field(nullable=True, alias='parentId')
    name: str = Field(nullable=False)
    source_type: str = Field(nullable=False, alias='sourceType')
    files: dict = Field(sa_column=Column(JSON), default={})
    header: str = Field(nullable=False)
    description: str = Field(sa_column=Column(TEXT))
    active: bool = Field(default=True)
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')
    
class elementHistory(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    bundle_id: int = Field(nullable=False, alias='bundleId')
    element_id: UUID4 = Field(nullable=False, alias='elementId')
    element_type: str = Field(nullable=False, alias='elementType')
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')

    __table_args__ = (
        Index('elementhistory_element_idx', 'bundle_id', 'element_id', postgresql_using='btree'),
    )

class bundleJournal(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    bundle_id: int = Field(nullable=False, alias='bundleId', index=True)
    operation_json: dict = Field(sa_column=Column(JSON), default={}, alias='operationJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    updated_by: str = Field(nullable=True, alias='updatedBy')

class bundleUnitProperties (SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    bundle_id: int = Field(nullable=False, alias='bundleId')
    sz_id: UUID4 = Field(nullable=True, alias='szId')
    sz_type: str = Field(nullable=True, alias='szType')
    sz_name: str = Field(nullable=True, alias='szName')
    unit_id: UUID4 = Field(nullable=True, alias='unitId')
    unit_type: str = Field(nullable=True, alias='unitType')
    unit_name: str = Field(nullable=True, alias='unitName')
    object_id: UUID4 = Field(nullable=True, alias='objectId')
    object_type: str = Field(nullable=True, alias='objectType')
    object_name: str = Field(nullable=True, alias='objectName')
    type_object_id: UUID4 = Field(nullable=True, alias='typeObjectId')
    type_object_type: str = Field(nullable=True, alias='typeObjectType')
    type_object_name: str = Field(nullable=True, alias='typeObjectName')
    propertyset_id: UUID4 = Field(nullable=True, alias='propertysetId')
    propertyset_name: str = Field(nullable=True, alias='propertysetName')
    propertyset_json: dict = Field(sa_column=Column(JSON), default={}, alias='propertysetJson')
    properties_id: UUID4 = Field(nullable=True, alias='propertiesId')
    properties_type: str = Field(nullable=True, alias='propertiesType')
    properties_name: str = Field(nullable=True, alias='propertiesName')
    properties_json: dict = Field(sa_column=Column(JSON), default={}, alias='propertiesJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    created_by: str = Field(nullable=True, alias='createdBy')
    __table_args__ = (
        Index('bundleunitproperties_sz_unit_idx', 'bundle_id', 'sz_id', 'unit_id',  postgresql_using='btree'),
    )
    
# can also be created with
# CREATE INDEX IF NOT EXISTS bundleunitproperties_sz_unit_idx ON bundleunitproperties (bundle_id, sz_id, unit_id); 
