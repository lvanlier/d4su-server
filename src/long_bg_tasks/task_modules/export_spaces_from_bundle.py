##
#
# Find all spaces for all storeys & buildings in a bundle - Provides the response as a csv file
#
##

from sqlmodel import create_engine, Session, select, text
import json
import pandas as pd

# Set up the logging
import logging
log = logging.getLogger(__name__)

from data import init as init


def get_all_spaces_in_a_bundle(session:Session, bundlId:int):
    statement_literal =f"""select space.object_id as space_id, space.name as space_name,
        space.elementjson ->> 'longName' as space_longname,
        storey.object_id as storey_id, storey.name as storey_name,
        building.object_id as building_id, building.name as building_name
        from object as space
        join relatedmembership as related_for_space 
            on related_for_space.bundle_id = space.bundle_id and
                related_for_space.object_id = space.object_id
        join relationship as relationship_for_storey  
            on relationship_for_storey.bundle_id = related_for_space.bundle_id and
                relationship_for_storey.relationship_id = related_for_space.relationship_id
        join object as storey on storey.bundle_id = relationship_for_storey.bundle_id and
                storey.object_id = relationship_for_storey.relating_id
        join relatedmembership as related_for_storey 
            on related_for_storey.bundle_id = storey.bundle_id and
                related_for_storey.object_id = storey.object_id
        join relationship as relationship_for_building
            on relationship_for_building.bundle_id = related_for_storey.bundle_id and
                relationship_for_building.relationship_id = related_for_storey.relationship_id
        join object as building 
            on building.bundle_id = relationship_for_building.bundle_id and
                building.object_id = relationship_for_building.relating_id
        where space.type = 'IfcSpace' 
            and space.bundle_id = {bundlId}
            and relationship_for_storey.type = 'IfcRelAggregates'
            and relationship_for_building.type = 'IfcRelAggregates'
        order by building.name, storey.name, space.name
    """
    statement = text(statement_literal)
    result = session.exec(statement).all()
    result_list = [row for row in result]
    return result_list

def write_spaces_in_csv(df_spaces, filePath):
    df_spaces.to_csv(filePath, index=False, encoding='utf-8')
    
def write_spaces_in_json_file(json_spaces, filePath):
    with open(filePath, 'w', encoding='utf-8') as f:
        f.write(json_spaces)

######################
# 
#   Start the process
#
######################

def main_proc(task_dict:dict):
    try:
        bundleId=task_dict['instruction_dict']['bundleId'] 
        PRINT = task_dict['debug']
        csv_file_path = task_dict['csvFilePath'] 
        json_file_path = task_dict['jsonFilePath']
        session = init.get_session() 
        spaces = get_all_spaces_in_a_bundle(session, bundleId)
        session.close()
        df_spaces = pd.DataFrame(spaces, columns=['space_id', 'space_name','space_longname','storey_id', 'storey_name','building_id', 'building_name'])
        df_spaces['spatialzone_name'] = ''
        df_spaces['spatialzone_longname'] = ''
        df_spaces['spatialzone_type'] = ''
        df_spaces = df_spaces.iloc[:, [0, 1, 2, 7, 8, 9, 3, 4, 5, 6]]
        # we store the spaces in a csv file
        write_spaces_in_csv(df_spaces, csv_file_path)
        # we write the spaces in a json that we could return to the client 
        json_spaces = df_spaces.to_json(orient='records', default_handler=str)
        write_spaces_in_json_file(json_spaces, json_file_path)
    except Exception as e:
        log.error(f'Error in main_proc of export_spaces_from_bundle: {e}')
        task_dict['status'] = 'failed'
        task_dict['error'] = str(e)
    return task_dict
