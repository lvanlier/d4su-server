import uuid
import pandas as pd

from sqlmodel import select, delete, text
from model import common as model
from data import init
import csv

# Set up the logging
import logging
log = logging.getLogger(__name__)

def importIfcTypes():
    try:
        # Get the IfcTypes from the reference csv
        ifctypes_csv_path = './db/csv/ifc-types-ref.csv'
        ifcTypes_df_0 = pd.read_csv(ifctypes_csv_path, delimiter=';') 
        ifcTypes_df = ifcTypes_df_0[['id','type','typename','category']] ## don't need here the description and attributes
        session = init.get_session()
        for index, row in ifcTypes_df.iterrows():
            ifc_type = model.ifctype(id=row['id'], type=row['type'], typename=row['typename'], category=row['category'])
            session.add(ifc_type)
        session.commit()
        session.close()
    except Exception as e:
        log.error(f'Error in importIfcTypes: {e}')
        raise e
    return

def select_IfcType():
    session = init.get_session()
    statement = select(model.IfcType)
    results = session.exec(statement)
    session.commit()
    session.close()
    return results

def load_IfcType_df(): 
    ifctypes_df = pd.read_sql_query('select * from ifctype',con=init.engine)
    return ifctypes_df
    
##
#
#   Initial Cleanup (only for testing)
#
##
def delete_all_p1():
    try:
        session = init.get_session()   
        statement = delete(model.bundleJournal)
        session.exec(statement)
        statement = delete(model.spatialUnitBundle)
        session.exec(statement)       
        statement = delete(model.bundle)
        session.exec(statement) 
        session.commit()
        session.close()
    except Exception as e:
        log.error(f'Error in delete_all_p1: {e}')
        raise e
    return

def delete_all_p2():
    try:
        session = init.get_session()    
        statement = delete(model.object)
        session.exec(statement)
        statement = delete(model.propertySet)
        session.exec(statement)
        statement = delete(model.representation)
        session.exec(statement)
        statement = delete(model.relationship)
        session.exec(statement)
        statement = delete(model.relatedMembership) 
        session.exec(statement)    
        session.commit()
        session.close()
    except Exception as e:
        log.error(f'Error in delete_all_p2: {e}')
        raise e
    return

def drop_all_p1():
    try:
        session = init.get_session()    
        statement = text('DROP TABLE IF EXISTS bundlejournal')
        session.exec(statement)
        statement = text('DROP TABLE IF EXISTS spatialunitbundle')
        session.exec(statement)       
        statement = text('DROP TABLE IF EXISTS bundle')
        session.exec(statement)
        statement = text('DROP TABLE IF EXISTS bundlehistory')
        session.exec(statement) 
        session.commit()
        session.close()
    except Exception as e:
        log.error(f'Error in drop_all_p1: {e}')
        raise e
    return

def drop_all_p2():
    try:
        session = init.get_session()    
        statement = text('DROP TABLE IF EXISTS object')
        session.exec(statement)
        statement = text('DROP TABLE IF EXISTS propertySet')
        session.exec(statement)
        statement = text('DROP TABLE IF EXISTS representation')
        session.exec(statement)
        statement = text('DROP TABLE IF EXISTS relationship')
        session.exec(statement)
        statement = text('DROP TABLE IF EXISTS relatedmembership') 
        session.exec(statement) 
        statement = text('DROP TABLE IF EXISTS elementhistory') 
        session.exec(statement)
        statement = text('DROP TABLE IF EXISTS spatialunitrootobject') 
        session.exec(statement)    
        session.commit()
        session.close()
    except Exception as e:
        log.error(f'Error in delete_all_p2: {e}')
        raise e
    return
