from pydantic import BaseModel, UUID4, Field, model_validator, computed_field
from pydantic.json_schema import SkipJsonSchema
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

#
# Import and process an IFC (convert it to an IFCJSON and store in the database)
#
class Store_Instruction(BaseModel):
    spatialUnitId: str | None = "5f2d17b0-43fb-445d-9c67-7dafb3292c33"
    bundleName: str | None = "" # name of the bundle
    parentBundleId: str | None = None
    
class Filter_Instruction(BaseModel):
    filter: IfcJsonFilter

class ImportAndProcessIfc_Instruction(BaseModel):
    source: ConvertIfcToIfcJson_Instruction
    filter: Filter_Instruction | SkipJsonSchema[None] = None
    store: Store_Instruction

# ImportAndProcessIfc_Result will be available in the 
#    ConvertIfcToIfcJson_Result
#    FilterIfcJson_Result (if filter is not None)
#    StoreIfcJsonInDb_Result  

#
#   Get the IFCJSON from the database 
#  
class GetIfcJsonFromDb_Instruction(BaseModel):
    bundleId: str | None = None # must be an integer
    
class GetIfcJsonFromDb_Result(BaseModel):
    resultPath: str # relative path of the result file (json)
    
#
#   Convert the IFCJSON to IFC
#
class ConvertIfcJsonToIfc_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFCJSON/cd5af173-e5a0-4ded-b005-d76660e80dc7_OUT.json"

class ConvertIfcJsonToIfc_Result(BaseModel):
    resultPath: str # relative path of the result file (ifc)

#
#   Extract elements from an IFC
#
class IfcExtractElements_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/Duplex_A_20110907_optimized.ifc"
    elementTypes: list[str] | None = ["IfcWall,IfcSlab,IfcBeam,IfcColumn,IfcWindow,IfcDoor,IfcSpace,IfcStair"]

class IfcExtractElements_Result(BaseModel):
    resultPath: str # relative path of the result file (ifc)

#
#   Extract storeys from an IFC using IfcPatch Split Storeys
#
class IfcSplitStoreys_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/Duplex_A_20110907_optimized.ifc"

class IfcSplitStoreys_Result(BaseModel):
    resultPath: str # relative path of the zip file containing all storey files (ifc)

#
# Extract a spatial unit from the database
#
class ExtractSpatialUnit_Instruction(BaseModel):
    bundleId: str | None = "1"
    useRepresentationsCache: bool | None = False
    elementType: Literal['IfcBuildingStorey', 'IfcZone', 'IfcSpatialZone', 'IfcSpace', 'IfcGroup']
    elementId: str | None = 'e58c68d0-1297-4210-9416-2412e1e6acc1'
    withIFC: bool | None = True

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

class ExtractSpatialUnit_Result(BaseModel):
    resultPath: str # relative path of the result file (json)


# When withIFC = True, the conversion to IFC is also included and its result also     
class ConvertIfcJsonToIfc_Result(BaseModel):
    resultPath: str # relative path of the result file (ifc)

#
#   Export Spaces from a Bundle 
#
class ExportSpacesFromBundle_Instruction(BaseModel):
    bundleId: str | None = "1"
    format: Literal['csv','json'] | None = 'csv'
    
class ExportSpacesFromBundle_Result(BaseModel):
    resultPath: str # relative path of the result file (cvs, or json)

class CreateSpatialZonesInBundle_Instruction(BaseModel):
    bundleId: str | None = "1"
    spatialZoneGivenType: Literal['IfcSpatialZone','IfcZone','IfcGroup'] | None = 'IfcSpatialZone'
    hasRepresentation: bool | None = False
    # This could/should be an URL from an Integration store
    sourceFileURL: str | None = "http://localhost:8002/SPACES/CSV/Duplex_A_20110907_optimized_spaces_with_spatialzones.csv"
    @model_validator(mode='before')
    def has_representationage_only_for_spatialzone(cls, values):
        given_type = values.get("spatialZoneGivenType")
        has_representation = values.get("hasRepresentation")
        if given_type != 'IfcSpatialZone' and has_representation:
            raise ValueError("hasRepresentation is only for IfcSpatialZone")
        return values
class CreateSpatialZonesInBundle_Result(BaseModel):
    resultPath: str # relative path of the result file (json) with the spatial zones

# The Information for each spatial zone will be as listed hereunder 
class SpatialZone(BaseModel):
    spatialZoneId: str
    spatialZoneName: str
    spatialZoneLongName: str
    spatialZoneType: str

#
#   Extract the envelope 
#
class ExtractEnvelope_Instruction(BaseModel):
    bundleId: str | None = "1"
    useRepresentationsCache: bool | None = False
    withIFC: bool | None = True

class ExtractEnvelope_Result(BaseModel):
    resultPath: str # relative path of the result file (json) with the envelope

#
#  Convert an IFC to glTF, COLLADA or CityJSON
#
class IfcConvert_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/IFC_SOURCE_FILES/Duplex_A_20110907_optimized.ifc"
    targetFormat: Literal['glTF','COLLADA','CityJSON']
    timeout: Optional[int] = Field(60, ge=30, le=180)

class IfcConvert_Result(BaseModel):
    resultPath: str # relative path of the result file (gltf, dae, or json)

#
#  Convert a CityJSON to IFC
#
class CityJsonToIfc_Instruction(BaseModel):
    sourceFileURL: str | None = "http://localhost:8002/CONVERSION_OUTFILES/CityJSON/twobuildings.city.json"

class CityJsonToIfc_Result(BaseModel):
    resultPath: str # relative path of the result file (ifc)

#
#  Populate bundeUnitProperties with the properties of the spatial units
#
class PopulateBundleUnitProperties_Instruction(BaseModel):
    bundleId: str | None = "1"  
    ifNotPopulated: bool | None = True # True if the bundleUnitProperties was not populated
    withCSVExport: bool | None = True
class PopulateBundleUnitProperties_Result(BaseModel):
    skipped: bool # True if the bundleUnitProperties was already populated
    nbrEntries: int | None = 0
    resultPath: str | None = '' # relative path of the result file (csv)
      
# this  documents the format of the task_dict which is passed to the task modules
task_dict: dict = {
    "taskName": None,
    "status": 'processing',
    "error": '',
    "procToken_str": None,
    "result": {},
    "bundleId": None
}

task_result : dict = {
    "taskId" : None,
    "taskResult" : None
}


import asyncio
resultQueue = asyncio.Queue()
