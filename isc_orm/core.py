from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import select
from sqlalchemy.sql import func

import pandas as pd
from tqdm import tqdm

ISC_MySQL_URL = 'mysql+mysqldb://internalro@isc/isc'
ISC_ENGINE = create_engine(ISC_MySQL_URL)

def query(limit):
    with ISC_ENGINE.connect() as conn:
        res = conn.execute("select * from moab_iteration_times "
                           "limit {limit};".format(limit))

def get_isc_db():
    isc_db = automap_base()
    isc_db.prepare(ISC_ENGINE, reflect=True)
    return isc_db

def get_jobs_df(db):
    df_list = [df for df, _ in get_jobs_df_generator(db)]
    return pd.concat(df_list)

def get_jobs_to_csv(db, file_name):
    if file_name[-4:] == '.csv':
        _file_name = file_name
    else:
        _file_name = ''.join([file_name,'.csv'])
    df_iter = iter(get_jobs_df_generator(db))
    df, num_chunks = next(df_iter)
    with open(_file_name, 'w') as f:
        df.to_csv(f)

    counter = 1
    for df, _ in tqdm(df_iter, length = num_chunks):
        with open(_file_name, 'a') as f:
            df.to_csv(f, header=False)
        counter += 1
        # print "Finished ({}, {})".format(counter, num_chunks)


    return _file_name

def get_jobs_df_generator(db):
    CHUNK_SIZE = 30000
    s = select([func.count(db.classes.jobs.ctime)])
    with ISC_ENGINE.connect() as conn:
        table_length = conn.execute(s).scalar()

    num_chunks = len(range(0, table_length, CHUNK_SIZE))
    for i in range(0, table_length, CHUNK_SIZE):
        s = select([db.classes.jobs]).offset(i).limit(CHUNK_SIZE)
        df_chunk = pd.read_sql(s, ISC_ENGINE)
        df_chunk['ctime'] = pd.to_datetime(df_chunk['ctime'], unit='s')
        yield df_chunk, num_chunks


def get_moab_iteration_times_df(db):
    s = select([db.classes.moab_iteration_times])
    df = pd.read_sql(s, ISC_ENGINE)
    df['ctime'] = pd.to_datetime(df['ctime'], unit='s')
    return df

def get_df(table_name):
    pass

def scratch(db):
    s = select([db.classes.moab_iteration_times])
    s = s.limit(10)
    db_moab_iter_times = pd.read_sql(s, ISC_ENGINE)
    return db_moab_iter_times
    
    '''
    with ISC_ENGINE.connect() as conn:
        res = conn.execute(s)
    db_df = pd.DataFrame(res.fetchall())'''
    #return db_df
