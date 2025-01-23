from pydantic import BaseModel, UUID4, Field, model_validator, computed_field
from typing import Literal, Optional

class ImportFilter(BaseModel):
    categoryList: list[Literal['construction','furniture','group','material','ownership','project','propertySet','quantity','relationship','representation','space','system']] | None = None
    excludeTypeList: list[str] | None = None

class ImportInstruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/IFC_Schependomlaan.ifc"
    modelType: Literal['ifc']
    migrateSchema: bool | None = True
    tessellate: bool | None = True
    spatialUnitId: str | None = "3693decc-405b-475d-a4ad-e1223de14ef9"
    parentBundleId : str | None = None  
    bundleId: str | None = None
    withFilter: bool | None = False
    filter: ImportFilter | None = {
        'categoryList': ['construction','group','material','ownership','project','propertySet','quantity','relationship','representation','space'],
        'excludeTypeList': ['IfcBuildingElement','IfcBuildingElementPart','IfcBuildingElementProxy','IfcBuildingElementProxyType']
    }
    
class IfcFromDBInstruction(BaseModel):
    byBundleId: str | None = None
    byBundleName: str | None = None
    
class IfcExtractElementsInstruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/JSON2IFC_OUTFILES/Duplex_A_20110907_optimized_OUT.ifc"
    elementTypes: list[str] | None = ["IfcWall,IfcSlab,IfcBeam,IfcColumn,IfcWindow,IfcDoor,IfcSpace,IfcStair"]

class IfcSplitStoreysInstruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/JSON2IFC_OUTFILES/Duplex_A_20110907_optimized_OUT.ifc"

class IfcConvertInstruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/Duplex_A_20110907_optimized.ifc"
    targetFormat: Literal['glTF','COLLADA','CityJSON']
    timeout: Optional[int] = Field(60, ge=30, le=180)

class CityJson2IfcInstruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/CONVERSION_OUTFILES/CityJSON/twobuildings.city.json"

class ExportSpacesFromBundleInstruction(BaseModel):
    bundleId: str | None = "1"

class CreateSpatialZonesInBundleInstruction(BaseModel):
    bundleId: str | None = "1"
    spatialZoneGivenType: Literal['IfcSpatialZone','IfcZone','IfcGroup'] | None = 'IfcSpatialZone'
    hasRepresentation: bool | None = False
    csvFileURL: str | None = "http://localhost:8002/TEMP_FILES/CSV/IFC_Schependomlaan_spaces_with_spatialzones.csv"
    @model_validator(mode='before')
    def has_representationage_only_for_spatialzone(cls, values):
        given_type = values.get("spatialZoneGivenType")
        has_representation = values.get("hasRepresentation")
        if given_type != 'IfcSpatialZone' and has_representation:
            raise ValueError("hasRepresentation is only for IfcSpatialZone")
        return values
        
class ExtractSpatialUnitInstruction(BaseModel):
    bundleId: str | None = "1"
    elementType: Literal['IfcBuildingStorey', 'IfcZone', 'IfcSpatialZone', 'IfcSpace', 'IfcGroup'] | None = 'IfcSpatialZone'
    elementId: str | None = 'e58c68d0-1297-4210-9416-2412e1e6acc1'

    @computed_field()
    def includeRelationshipTypes(self) -> str:
        if self.elementType == 'IfcBuildingStorey':
            return ['IfcRelAggregates','IfcRelContainedInSpatialStructure','IfcRelFillsElement','IfcRelVoidsElement']
        elif self.elementType == 'IfcSpatialZone' or self.elementType == 'IfcSpace':
            return ['IfcRelAggregates','IfcRelContainedInSpatialStructure','IfcRelFillsElement','IfcRelVoidsElement','IfcRelSpaceBoundary','IfcRelReferencedInSpatialStructure']
        elif self.elementType == 'IfcZone' or self.elementType == 'IfcGroup':
            return ['IfcRelAggregates','IfcRelContainedInSpatialStructure','IfcRelFillsElement','IfcRelVoidsElement','IfcRelSpaceBoundary','IfcRelReferencedInSpatialStructure']
        else:
            return ['IfcRelAggregates','IfcRelContainedInSpatialStructure','IfcRelFillsElement','IfcRelVoidsElement']   
        
            
import_instruction_dict = {
    "sourceFileURL": None,
    "type": "ifc",
    "spatialUnitId": None,
    "parentBundleId": None,
    "bundleId": None,
    "filter": {
        "categoryList": None,
        "excludeTypeList": None
    }
}

# this  documents the format of the task_dict which is passed to the task modules
task_dict: dict = {
    "taskName": None,
    "status": 'processing',
    "error": None,
    "procToken_str": None,
    "instruction_dict": None,
    "ifcJsonFilePath": None,
    "filteredIfcJsonFilePath": None,
    "ifcOutFilePath": None,  
    "bundleId": None
}

task_result : dict = {
    "taskId" : None,
    "taskResult" : None
}
