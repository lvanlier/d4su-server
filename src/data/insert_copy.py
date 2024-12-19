##
#
#   Bulk insert Dataframe in Postgres
#   from https://github.com/askintamanli/Fastest-Methods-to-Bulk-Insert-Pandas-Dataframe-into-PostgreSQL/blob/main/method_3.py
#
##

import time
import csv
from io import StringIO
    
from data import init2 as init

# Set up the logging
import logging
log = logging.getLogger(__name__)


def psql_insert_copy(table, conn, keys, data_iter): #method
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def bulk_insert_df(df, table_name:str, PRINT:bool=False):
    start_time = time.time() # get start time before insert
    try:
        df.to_sql(
            name=table_name,
            con=init.engine,
            if_exists="append",
            index=False,
            method=psql_insert_copy
        )
        end_time = time.time() # get end time after insert
        if PRINT:
            total_time = end_time - start_time # calculate the time
            log.info(f"Insert time: {total_time} seconds") # print tim
    except Exception as e:
        log.error(f'Error in bulk_insert_df: {e}')
        raise e
    return