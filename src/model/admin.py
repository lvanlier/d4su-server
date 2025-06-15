from typing import Literal
from pydantic import BaseModel

#
#   IfcFileQuery: executes queries on Ifc and ifcJSON files
#    + listTypes: provides in a csv the list all the types in an Ifc File or IfcJSON file
#    + compareListTypes: provides in a csv the comparative list of types of Ifc files
#    + compareIfcJsonListTypes: provides in a csv the comparative list of types in an Ifc file and an IfcJSON file
#

class IfcFileQuery_Instruction(BaseModel):
    sourceFileURL : list[str] | None = ["http://localhost:8002/ISSUE_STYLE/Duplex_doors.ifc","http://localhost:8002/ISSUE_STYLE/Duplex_doors_NI.json"]
    queryType : Literal['listTypes','compareListTypes','compareIfcJsonListTypes'] | None = 'compareIfcJsonListTypes'
    queryString: str | None = ''
    
class IfcFileQuery_Result(BaseModel):
    resultPath: str # relative path of the result file
    runtime: float | None = 0.0 # in seconds

class Ifc2Sql_Instruction(BaseModel):
    sourceFileURL : str | None = "http://localhost:8002/ISSUE_STYLE/Duplex_doors.ifc"
    sql_type : Literal['SQLite', 'MySQL'] | None = 'SQLite'
    database_name : str | None = 'ifc2sqldb'

class Ifc2Sql_Result(BaseModel):
    runtime: float | None = 0.0 # in seconds
    