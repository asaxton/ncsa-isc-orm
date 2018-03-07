
from isc_orm.core import get_job_in_time_range
from isc_orm.core import get_ccs_of_job
from isc_orm.core import get_isc_db
from isc_orm.core import get_job
from datetime import datetime as dt

def graylog_query_of_good_condidate(**kwargs):
    max_nodes = kwargs.get('max_nodes')
    min_nodes = kwargs.get('min_nodes')

    start_date = kwargs.get('start_date', dt(2016, 5, 12))
    end_date = kwargs.get('end_date', dt(2016, 5, 13))
    
    if max_nodes or min_nodes:
        db = get_isc_db()
        if not min_nodes:
            min_nodes = 1

        jobs_df = get_job_in_time_range(start_date, end_date, db)
        jobs_df['durration'] = jobs_df['end'] - jobs_df['start']
        target_job = jobs_df[
            (jobs_df['node_count'] <= max_nodes) & \
                (jobs_df['node_count'] >= min_nodes)
            ]
        if len(target_job) == 0:
            raise Exception('No jobs exist between time %s and %s with node_count between %s and %s' % \
                                (start_date, end_date, min_nodes, max_nodes))
        # target_job.sort_values('durration', inplace=True, ascending=False)
        target_job[target_job.index == target_job['durration'].idxmax()]
        target_jobid = target_job['jobid'].values[0]
        ccs_list = get_ccs_of_job(target_jobid, db)
    else:
        l = 2.0
        db = get_isc_db()
        jobs_df = get_job_in_time_range(start_date, end_date, db)
        jobs_df['durration'] = jobs_df['end'] - jobs_df['start']
        jobs_df['sig'] = \
            jobs_df['durration'].map(
            lambda x : pow(x, 1.0/l)) * \
            jobs_df['node_count'].map(
            lambda y: pow(y, 1.0 - 1.0/l))
        target_job = jobs_df[jobs_df.index == jobs_df['sig'].idxmax()]['jobid'].values[0]
        target_jobid = target_job['jobid'].values[0]
        ccs_list = get_ccs_of_job(target_jobid, db)
        
    return target_job, ccs_list

def graylog_query_of_job(job):
    '''
    :param pandas.DataFrame job: dataframe with only a single job record
    '''
    db = get_isc_db()
    ccs_list = get_ccs_of_job(job['jobid'].values[0], db)

    dt_start = dt.fromtimestamp(job['start'].values[0])
    str_start = dt_start.strftime("%Y-%m-%dT%H:%M:%S")
    
    dt_end = dt.fromtimestamp(job['end'].values[0])
    str_end = dt_end.strftime("%Y-%m-%dT%H:%M:%S")

    graylog_str_q = """_exists_:src_timestamp AND src_timestamp:[%(start)s  TO %(end)s] AND ( %(ccs_str_q)s )"""
    ccs_str_q = ' OR '.join(['"%s"' % ccs for ccs in ccs_list])
    return graylog_str_q % {"start" : str_start, 'end' : str_end, 'ccs_str_q' : ccs_str_q}
