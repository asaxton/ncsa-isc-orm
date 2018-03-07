from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from sqlalchemy.sql import func
from sqlalchemy.orm import Session
from sqlalchemy import and_
from datetime import datetime as dt

import pandas as pd
from tqdm import tqdm

#ISC_MySQL_URL = 'mysql+mysqldb://internalro@isc/isc'
ISC_MySQL_URL = 'mysql+mysqldb://internalro@127.0.0.1:3310/isc'
ISC_ENGINE = create_engine(ISC_MySQL_URL)
ISC_SESSION = sessionmaker(ISC_ENGINE)
PROGRESS_BAR_LEAVE=True

def query(limit):
    with ISC_ENGINE.connect() as conn:
        res = conn.execute("select * from moab_iteration_times "
                           "limit {limit};".format(limit))
def get_isc_db():
    isc_db = automap_base()
    # TO DO
    # automap_base does not map tables with out indices.
    # Have to build a custom class.
    # class nidmap_current(isc_db):
    #    __tablename__ = 'nid_map_current'

    isc_db.prepare(ISC_ENGINE, reflect=True)
    return isc_db

def get_jobs_df(db):
    df_iter = iter(get_jobs_df_generator(db))
    df, num_chunks = next(df_iter)
    df_list = [df]
    df_list.extend([df for df, _ in tqdm(df_iter, initial=1, total=num_chunks, leave=PROGRESS_BAR_LEAVE)])

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

    for df, _ in tqdm(df_iter, initial=1, total=num_chunks, leave=PROGRESS_BAR_LEAVE):
        with open(_file_name, 'a') as f:
            df.to_csv(f, header=False)

    return _file_name

def get_jobs_df_generator(db):
    session = Session(ISC_ENGINE)
    CHUNK_SIZE = 30000

    q = session.query(db.classes.jobs)
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    table_length = q.session.execute(count_q).scalar()

    num_chunks = len(range(0, table_length, CHUNK_SIZE))

    for i in range(0, table_length, CHUNK_SIZE):
        df_chunk = pd.read_sql(session.query(db.classes.jobs).slice(i, i+CHUNK_SIZE).statement, ISC_ENGINE)
        # df_chunk['ctime'] = pd.to_datetime(df_chunk['ctime'], unit='s')
        yield df_chunk, num_chunks

def get_df_generator(db, table_name):
    '''
    :param db sqlalchemy.ext.automap.Base: db returned from get_isc_db
    :param table_name str: name of table to grab
    '''

    session = Session(ISC_ENGINE)
    CHUNK_SIZE = 30000
    table = getattr(db.classes, table_name)

    q = session.query(table)
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    table_length = q.session.execute(count_q).scalar()

    num_chunks = len(range(0, table_length, CHUNK_SIZE))

    for i in range(0, table_length, CHUNK_SIZE):
        df_chunk = pd.read_sql(session.query(table).slice(i, i+CHUNK_SIZE).statement, ISC_ENGINE)
        # df_chunk['ctime'] = pd.to_datetime(df_chunk['ctime'], unit='s')
        yield df_chunk, num_chunks

def get_job(jobid, db=None):
    if db:
        isc_db = db
    else:
        isc_db = get_isc_db()

    jobs_table = getattr(isc_db.classes, 'jobs')
    job_record = pd.read_sql(select([jobs_table]).where(jobs_table.jobid==jobid), ISC_ENGINE)

    return job_record
    

def get_job_in_time_range(start_dt, end_dt, db):
    jobs_table = getattr(db.classes, 'jobs')
    session = Session(ISC_ENGINE)
    s = select([jobs_table]).where(
        and_(jobs_table.start>start_dt.strftime('%s'),
             jobs_table.end<end_dt.strftime('%s')))
    
    return pd.read_sql(s, ISC_ENGINE)

def get_ccs_of_job(jobid, db):
    '''
    (c)ase (c)hassis (s)lot
    '''

    #job_hosts_table = getattr(db.classes, 'job_hosts')
    #session = Session(ISC_ENGINE)
    #s = select([job_hosts_table.nid]).where(job_hosts_table.jobid == jobid)
    #pd.read_sql(s, ISC_ENGINE)
    q_str = \
    """
    select cname
    from (
    select *
    from job_hosts
    where jobid = "%(jobid)s" )
    as job_nid
    inner join nidmap_current
    on job_nid.nid = nidmap_current.nid;
    """
    with ISC_ENGINE.connect() as conn:
        r = conn.execute( q_str % {"jobid": jobid})
    r_df = pd.DataFrame(r.fetchall())
    r_df.columns = r.keys()
    ccs_of_jobid = map(lambda s: s[:-2], r_df['cname'].tolist())
    return ccs_of_jobid

def get_job_id_from_ccs(timestamp_in, ccs):
    """
    Expecting timestamp in UTC time.

    timestamp can be in epoch time or %Y-%m-%dT%H:%M:%S.%f format
    """

    try:
        timestamp = int(dt.strptime(timestamp_in, "%Y-%m-%dT%H:%M:%S.%f").strftime('%s'))
    except:
        timestamp = int(timestamp_in)


    q_str = """
    select *
    from (
    select j_1.jobid, nid 
    from (
    select jobid
    from jobs 
    where
    start < %(timestamp)d 
    AND 
    end >= %(timestamp)d 
    )
    as j_1
    inner join job_hosts 
    on j_1.jobid = job_hosts.jobid
    ) as j_2 
    inner join nidmap_current 
    on j_2.nid = nidmap_current.nid 
    where 
    cname LIKE '%%%%%(ccs)s%%%%';
    """
    with ISC_ENGINE.connect() as conn:
        r = conn.execute( q_str % {'timestamp' : timestamp, 'ccs': ccs})
    r_df = pd.DataFrame(r.fetchall())
    if len(r_df) == 0:
        raise Exception('No job running on %s at %s' % (ccs, timestamp_in))
    r_df.columns = r.keys()
    num_job_id = len(set(r_df['jobid']))
    if num_job_id == 0:
        return None
    else:
        return list(set(r_df['jobid']))

def get_moab_iteration_times_df(db):
    s = select([db.classes.moab_iteration_times])
    df = pd.read_sql(s, ISC_ENGINE)
    # df['ctime'] = pd.to_datetime(df['ctime'], unit='s')
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
