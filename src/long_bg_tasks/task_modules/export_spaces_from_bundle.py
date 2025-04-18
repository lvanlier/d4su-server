##
#
# Find all spaces for all storeys & buildings in a bundle - Provides the response as a csv file
#
##

from sqlmodel import create_engine, Session, select, text
import json
import pandas as pd
import uuid

# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter


from data import init as init
from model.transform import ExportSpacesFromBundle_Instruction, ExportSpacesFromBundle_Result

def get_all_spaces_from_bundleunit(session:Session, bundlId:int):
    statement_literal =f"""select * from bundleunit
        where bundle_id = {bundlId}
    """
    statement = text(statement_literal)
    result = session.exec(statement).all()
    result_dict = {str(row.unit_id):row for row in result}
    spaces_list = []
    for row in result:
        if row.unit_type != 'IfcSpace':
            continue
        space = {}
        space['space_id'] = row.unit_id
        space['space_name'] = row.unit_name
        space['space_longname'] = row.unit_longname
        if row.parent_type == 'IfcBuildingStorey':
            space['storey_id'] = row.parent_id
            space['storey_name'] = row.parent_name
            key = str(row.parent_id)
            if key in result_dict:
                if result_dict[key].parent_type == 'IfcBuilding':
                    space['building_id'] = result_dict[key].parent_id
                    space['building_name'] = result_dict[key].parent_name
                else:
                    space['building_id'] = ''
                    space['building_name'] = ''
            else:
                space['building_id'] = ''
                space['building_name'] = ''
        elif row.parent_type == 'IfcBuilding':
            space['storey_id'] = ''
            space['storey_name'] = ''
            space['building_id'] = row.parent_id
            space['building_name'] = row.parent_name
        else:
            space['storey_id'] = ''
            space['storey_name'] = ''
            space['building_id'] = ''
            space['building_name'] = ''    
        space['parent_id'] = row.parent_id
        space['parent_name'] = row.parent_name
        space['parent_type'] = row.parent_type
        space['parent_longname'] = row.parent_longname
        spaces_list.append(space)
    return spaces_list


def write_spaces_in_csv(df_spaces, filePath):
    df_spaces.to_csv(filePath, index=False, encoding='utf-8')
    
def write_spaces_in_json_file(json_spaces, filePath):
    with open(filePath, 'w', encoding='utf-8') as f:
        f.write(json_spaces)


class ExportSpacesFromBundle():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = ExportSpacesFromBundle_Instruction(**self.task_dict['ExportSpacesFromBundle_Instruction'])
            self.bundleId = instruction.bundleId
            self.task_dict['bundleId'] = instruction.bundleId
            self.format = instruction.format
            self.BASE_PATH = self.task_dict['BASE_PATH']
            self.SPACES_PATH = self.task_dict['SPACES_PATH']
            self.PRINT = self.task_dict['debug']
            self.start = perf_counter()
            if self.PRINT:
                    print(f'>>>>> In ExportSpacesFromBundle.init: {self.bundleId}')
        except Exception as e:
            log.error(f'Error in ExportSpacesFromBundle.init : {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in ExportSpacesFromBundle.init : {e}'    

    def export(self):
        try:
            session = init.get_session() 
            spaces = get_all_spaces_from_bundleunit(session, self.bundleId)
            session.close()
            df_spaces = pd.DataFrame(spaces, columns=['space_id', 'space_name','space_longname','storey_id', 'storey_name','building_id', 'building_name', 'parent_id', 'parent_name', 'parent_type', 'parent_longname'])
            df_spaces['spatialzone_name'] = ''
            df_spaces['spatialzone_longname'] = ''
            df_spaces['spatialzone_type'] = ''
            df_spaces = df_spaces.iloc[:, [0, 1, 2, 11, 12, 13, 3, 4, 5, 6, 7, 8, 9, 10]]
            if self.format == 'json':
                # we store the spaces in a json file
                result_rel_path = f'{self.SPACES_PATH}JSON/{uuid.uuid4()}_spaces.json' 
                result_path = f'{self.BASE_PATH}{result_rel_path}'       
                json_spaces = df_spaces.to_json(orient='records', default_handler=str)
                write_spaces_in_json_file(json_spaces, result_path)
            else:
                # we write the spaces in a csv file 
                result_rel_path = f'{self.SPACES_PATH}CSV/{uuid.uuid4()}_spaces.csv' 
                result_path = f'{self.BASE_PATH}{result_rel_path}'       
                write_spaces_in_csv(df_spaces, result_path)
            result = ExportSpacesFromBundle_Result(
                resultPath = result_rel_path,
                runtime = round(perf_counter() - self.start, 2)
            )
            self.task_dict['result']['ExportSpacesFromBundle_Result'] = result.dict()
        except Exception as e:
            log.error(f'Error in ExportSpacesFromBundle.export: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in ExportSpacesFromBundle.export: {e}'
        return self.task_dict

