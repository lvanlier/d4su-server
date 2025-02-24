from pydantic import BaseModel, UUID4, Field, model_validator, computed_field
from typing import Literal, Optional

#
# Validate the IFC model against the Information Delivery Specification (IDS)
#
class ValidateIfcAgainstIds_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IDS/IDS_wooden-windows_IFC.ifc"
    idsFilesURLs: list[str] | None = ["http://localhost:8002/IDS/IDS_wooden-windows.ids"]
    resultType: Literal['json','html','bcfzip'] | None = 'html'

class ValidateIfcAgainstIds_ResultItem(BaseModel):
    isSuccess: bool
    percentChecksPass: float
    percentRequirementsPass: float
    percentSpecificationsPass: float
    resultPath: str # relative path of the result file (json, html or bfczip)
class ValidateIfcAgainstIds_Result(BaseModel):
    result: list[ValidateIfcAgainstIds_ResultItem]

#
# Migrate an IFC file to a different schema (IFC2X3 -> IFC4)
#
class MigrateIfcSchema_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/Duplex_A_20110907_optimized.ifc"
    targetSchema: str | None = "IFC4"

class MigrateIfcSchema_Result(BaseModel):
    resultPath: str # relative path of the result file (ifc)

#
# Tessellate selected product elements in an IFC 
#
class TessellateIfcElements(BaseModel):
    elementTypes: list[str] | None = ["IfcWall,IfcWallStandardCase,IfcSlab,IfcBeam,IfcColumn,IfcWindow,IfcDoor,IfcSpace"]
    forcedFacetedBREP: bool | None = False

class TessellateIfcElements_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/AC20-FZK-Haus.ifc"
    tessellateIfcElements: TessellateIfcElements
    
class TessellateIfcElements_Result(BaseModel):
    resultPath: str # relative path of the result file (ifc)

#
# Convert an IFC to IFCJSON with IFC2JSON
#
class ConvertIfcToIfcJson_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/Duplex_A_20110907_optimized.ifc"

class ConvertIfcToIfcJson_Result(BaseModel):
    rootObjectId: str
    rootObjectType: str
    rootObjectName: str 
    resultPath: str # relative path of the result file (json)

#
# Filter an IFCJSON 
#
class IfcJsonFilter(BaseModel):
    categoryList: list[Literal['construction','furniture','group','material','ownership','project','propertySet','quantity','relationship','representation','space','system']] | None = ['construction','group','material','ownership','project','propertySet','quantity','relationship','representation','space'] 
    excludeTypeList: list[str] | None = ['IfcBuildingElement','IfcBuildingElementPart','IfcBuildingElementProxy','IfcBuildingElementProxyType']

class FilterIfcJson_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFCJSON/597e4482-c5ab-4f8d-a02b-e7db99a14a37_NI.json"
    filter: IfcJsonFilter
    
class FilterIfcJson_Result(BaseModel):
    rootObjectId: str
    rootObjectType: str
    rootObjectName: str 
    resultPath: str # relative path of the result file (json)    
    
#
# Store an IFCJSON in the database
#
class StoreIfcJsonInDb_Instruction(BaseModel):
    spatialUnitId: str | None = "5f2d17b0-43fb-445d-9c67-7dafb3292c33"
    bundleName: str | None = "Duplex_A_20110907_optimized" # name of the bundle
    sourceFileURL: str | None = "http://localhost:8002/IFCJSON/7be56a97-3db3-4c85-94e8-86ac45b63ff6_FIL.json" 
    parentBundleId: str | None = None
    
class StoreIfcJsonInDb_Result(BaseModel):
    bundleId: str    

class ImportInstruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/AC20-FZK-Haus.ifc"
    modelType: Literal['ifc']
    migrateSchema: bool | None = False
    idsFileURLs: list[str] | None = None
    tessellate: bool | None = True
    spatialUnitId: str | None = "b6fc5402-ca87-47ba-a9f4-e29173d51656"
    parentBundleId : str | None = None  
    bundleId: str | None = None
    withFilter: bool | None = False
    filter: IfcJsonFilter | None = None 
    tessellateElements: TessellateIfcElements
    
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
    useRepresentationsCache: bool | None = False
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

class ExtractEnvelopeInstruction(BaseModel):
    bundleId: str | None = "1"
    useRepresentationsCache: bool | None = False
        
            
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
    "result": None,
    "ifcJsonFilePath": None,
    "filteredIfcJsonFilePath": None,
    "ifcOutFilePath": None,  
    "bundleId": None
}

task_result : dict = {
    "taskId" : None,
    "taskResult" : None
}
