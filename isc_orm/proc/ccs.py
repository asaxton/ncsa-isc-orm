'''
(c)ase (c)hassis (s)lot
'''

from isc_orm.core import get_job_in_time_range
from isc_orm.core import get_ccs_of_job
from isc_orm.core import get_isc_db
from datetime import datetime as dt

def get_ccs_list_of_good_condidate():
    l = 2.0
    db = get_isc_db()
    jobs_df = get_job_in_time_range(dt(2016, 5, 12), dt(2016, 5, 13), db)
    jobs_df['durration'] = jobs_df['end'] - jobs_df['start']
    jobs_df['sig'] = \
        jobs_df['durration'].map(
        lambda x : pow(x, 1.0/l)) * \
        jobs_df['node_count'].map(
        lambda y: pow(y, 1.0 - 1.0/l))
    target_jobid = jobs_df[jobs_df.index == jobs_df['sig'].idxmax()]['jobid'].values[0]
    ccs_list = get_ccs_of_job(target_jobid, db)
    return ccs_list

