from sqlmodel import SQLModel, Field, Column, ARRAY, String, TEXT
from typing import Optional, Any
import datetime
from pydantic import BaseModel, UUID4, UUID5
from sqlalchemy import JSON, Column, DateTime, func, Sequence, Integer, Index
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
    
class CreateSpatialUnit(BaseModel): # model for the request
    name: str | None = "Test Spatial Unit"
    type: str | None = "Building"
    description: str | None = "This is a test spatial unit"
    unitGuide: dict | None = {'overview': 'this is a test spatial unit'}
    
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
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))


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
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))

class bundleUnit(SQLModel, table=True):
    bundleunit_id: UUID5 = Field(primary_key=True, alias='bundleUnitId')
    bundle_id: int = Field(nullable=False, alias='bundleId')
    unit_id: UUID4 = Field(nullable=False, alias='unitId')
    unit_type: str = Field(nullable=False, alias='unitType')
    unit_name: str = Field(nullable=True, alias='unitName')
    unit_longname: str = Field(nullable=True, alias='unitLongName')
    relationship_id: UUID4 = Field(nullable=False, alias='relationshipId')
    relationship_type: str = Field(nullable=False, alias='relationshipType')
    parent_id: UUID4 = Field(nullable=False, alias='parentId')
    parent_type: str = Field(nullable=False, alias='parentType')
    parent_name: str = Field(nullable=True, alias='parentName')
    parent_longname: str = Field(nullable=True, alias='parentLongName')
    unitjson: dict = Field(sa_column=Column(JSON), default={}, alias='unitJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    __table_args__ = (
        Index("bunit_index", "bundle_id", "unit_id", postgresql_using="btree"), 
    )
    
class spatialUnitBundleUnit(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    spatial_unit_id: UUID4 = Field(nullable=False, alias='spatialUnitId', index=True)
    bundle_id: int = Field(nullable=False, alias='bundleId', index=True)
    bundleunit_id: UUID5 = Field(nullable=False, alias='bundleUnitEdgeId', index=True)
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    
class object(SQLModel, table=True):
    bundle_id: int = Field(nullable=False, alias='bundleId', primary_key=True)
    object_id: UUID4 = Field(primary_key=True)
    type: str = Field(nullable=False)
    name: str = Field(nullable=True) # have object with no name (e.g. IfcOpeningElement)
    representation_ids: list[str] = Field(sa_column=Column(ARRAY(String(36))), default=[], alias='representationIds')
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    
class representation(SQLModel, table=True):
    bundle_id: int = Field(nullable=False, alias='bundleId', primary_key=True)
    representation_id: UUID4 = Field(primary_key=True)
    type: str = Field(nullable=False)
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    geom: Optional[Any] = Field(sa_column=Column(Geometry(), nullable=True))
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))


class propertySet(SQLModel, table=True):
    bundle_id: int = Field(nullable=False, alias='bundleId', primary_key=True)
    propertyset_id: UUID4 = Field(primary_key=True)
    name: str = Field(nullable=False)
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))

    
class relationship(SQLModel, table=True):
    bundle_id: int = Field(nullable=False, alias='bundleId', primary_key=True)
    relationship_id: UUID4 = Field(primary_key=True)
    type: str = Field(nullable=False)
    relating_type: str = Field(nullable=False, alias='relatingType')
    relating_id: UUID4 = Field(nullable=True, alias='relatingId')
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    __table_args__ = (
        Index("rela_index", "bundle_id", "relating_id", postgresql_using="btree"), 
    )
  
# can also be created with
# CREATE INDEX rela_index ON relationship (bundle_id, relating_id);
    
class relatedMembership(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    bundle_id: int = Field(nullable=False, alias='bundleId')    
    relationship_id: UUID4 = Field(nullable=False, alias='relationshipId')
    object_type: str = Field(nullable=False, alias='objectType')
    object_id: UUID4 = Field(nullable=False, alias='objectId')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    __table_args__ = (
        Index('relmemb_index', 'bundle_id', 'object_id', postgresql_using='btree'),
        Index('relmemb_index2', 'bundle_id', 'relationship_id', postgresql_using='btree') 
    )
    
# can also be created with
# CREATE INDEX relmemb_index ON relatedmembership (bundle_id, object_id); 
# CREATE INDEX relmemb_index2 ON relatedmembership (bundle_id, relationship_id);
    
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
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))
    
class elementHistory(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    bundle_id: int = Field(nullable=False, alias='bundleId')
    element_type: str = Field(nullable=False, alias='elementType')
    elementjson: dict = Field(sa_column=Column(JSON), default={}, alias='elementJson')
    version: int = Field(nullable=False)
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))

    
class bundleJournal(SQLModel, table=True):
    id: UUID4 = Field(primary_key=True)
    bundle_id: int = Field(nullable=False, alias='bundleId', index=True)
    operation_json: dict = Field(sa_column=Column(JSON), default={}, alias='operationJson')
    created_at: datetime.datetime = Field(sa_column=Column(DateTime(), default=func.now(), nullable=False))
    updated_at: Optional[datetime.datetime] = Field(sa_column=Column(DateTime(), nullable=True ))

