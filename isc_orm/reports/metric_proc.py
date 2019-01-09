import numpy as np
from isc_orm.core import *

def metric_input(job_metrics):
    nidmap = get_nidmap()
    ret_dict = {}
    for id, job_m in job_metrics.items():
        job_nodes = set(job_m['CompId'])
        check_metric_lengths = [len(job_m[job_m['CompId'] == n]) for n in job_nodes]
        if len(set(check_metric_lengths)) > 1:
            raise Exception('Error in metric data, '
                            'length of time series not equal. '
                            'check_metric_lengths: {}'.format(check_metric_lengths))

        sample_nid = next(iter(job_nodes))
        data_shape = job_m[job_m['CompId'] == sample_nid].values.shape
        data_t_cube = np.zeros((24,24,24, *data_shape))

        for n in job_nodes:
            x,y,z = nidmap[nidmap['nid'] == n][['x', 'y', 'z']].values[0]
            np.copyto(data_t_cube[x,y,z], job_m[job_m['CompId'] == n].values)

            ret_dict[id] = data_t_cube

    return ret_dict
