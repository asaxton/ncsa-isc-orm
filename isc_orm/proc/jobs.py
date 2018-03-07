import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import timedelta
from datetime import datetime as dt
from math import ceil
from math import floor
from collections import OrderedDict
from itertools import product
from matplotlib import pyplot as plt

plt.rc('text', usetex=True)

def grouper(iterable, maxd):
    prev = None
    group = []
    for item in iterable:
        if not prev or item - prev <= maxd:
            group.append(item)
        else:
            yield group
            group = [item]
        prev = item
    if group:
        yield group

def scrub(jobs_df, **kwargs):
    '''
    This function filters out buggy, bad, or malformed
    records from a given jobs table.

    :kwargs queues str or list: specify which queue\s to include
    :kwargs node_counts int or list: which node_counts to include
    :return dict: {'jobs_df': pandas.DataFrame, 'Report': {}}
    '''
    tot_len_jobs_df = len(jobs_df)
    start = pd.to_datetime(jobs_df['start'], unit='s')
    end = pd.to_datetime(jobs_df['end'], unit='s')

    jobs_df['end_dt'] = end
    jobs_df['start_dt'] = start

    start_bound = dt(2010,1,1)
    end_bound = dt.now()

    good_start = start > start_bound
    good_end = end < end_bound

    consistent_start_end = start < end

    filter = consistent_start_end & good_start & good_end
    jobs_df = jobs_df[filter]

    report = {
        'global_scrub':{'removed_rec': len(filter) - filter.sum(),
                        'input_rec': tot_len_jobs_df,
                        'bad_start': len(good_start) - good_start.sum(),
                        'start_bound': start_bound,
                        'bad_end': len(good_end) - good_end.sum(),
                        'end_bound': end_bound,
                        'bad_start_end_pair': len(consistent_start_end) - consistent_start_end.sum()
                        }
        }
    
    queues = kwargs.get('queues', [])
    if not isinstance(queues, list):
        queues = [queues]

    if queues:
        queue_filter = jobs_df['queue'] == queues[0]
        for q in queues[1:]:
            queue_filter = queue_filter | (jobs_df['queue'] == q)
            
        jobs_df = jobs_df[queue_filter]
        report['queues'] = {'removed_rec': len(queue_filter) - queue_filter.sum()}
        
    node_counts = kwargs.get('node_counts', [])
    if not isinstance(node_counts, list):
        node_counts = [node_counts]

    if node_counts:
        node_counts_filter = jobs_df['node_count'] == node_counts[0]
        for nc in node_counts:
            node_counts_filter = node_counts_filter | jobs_df['node_count'] == nc

        jobs_df = jobs_df[node_counts_filter]
        report['node_counts'] = {'rec_rem': len(node_counts_filter) - node_counts_filter.sum()}

    jobs_df['runtime'] = jobs_df['end'] - jobs_df['start']
    jobs_df['runtime_dt'] = pd.to_datetime(jobs_df['runtime'], unit='s')

    report['common_info'] = {
        'min_start': min(start),
        'max_end': max(end)
        }

    return {'jobs_df': jobs_df, 'Report': report}
    

"""
1) Drop the remainder.
2) Combine the last group of two and last group of three to make one group of length five.
3) Assume that last two items are run in a job of 3 nodes, thus one node will be completely unused
4) Take the last two items, and place one item into two of the other groups. 
"""

def user_waste_4(df, min_grouping_size=2):

    N = len(df)
    if min_grouping_size > N:
        return [df['runtime'].max() * min_grouping_size - df['runtime'].sum()]
    else:
        grouping_mask = np.arange(N) // min_grouping_size
        grouping_mask = grouping_mask.tolist()

        if N%min_grouping_size:
            grouping_mask = grouping_mask[:-(N%min_grouping_size)]

        grouping_mask = sorted(range(N % min_grouping_size) + grouping_mask)

        job_groups = [p['runtime'].tolist()\
                         for _, p in df.groupby(np.array(grouping_mask))]

        return [len(g)*max(g) - sum(g) for g in tqdm(job_groups, desc='Groups')]

def user_waste_3(df, min_grouping_size=2):

    N = len(df)
    if min_grouping_size > N:
        return [df['runtime'].max() * min_grouping_size - df['runtime'].sum()]
    else:
        grouping_mask = np.arange(N) // min_grouping_size

        job_groups = [p['runtime'].tolist()\
                         for _, p in df.groupby(np.array(grouping_mask))]

        return [min_grouping_size*max(g) - sum(g) for g in tqdm(job_groups, desc='Groups')]

def user_waste_2(df, min_grouping_size=2):

    N = len(df)
    if min_grouping_size > N:
        return [df['runtime'].max() * min_grouping_size - df['runtime'].sum()]
    else:
        grouping_mask = np.arange(N) // min_grouping_size
        grouping_mask = grouping_mask[:-(N%min_grouping_size)]
        grouping_mask = grouping_mask + [max(grouping_mask)]*(N%min_grouping_size)
        
        job_groups = [p['runtime'].tolist()\
                         for _, p in df.groupby(np.array(grouping_mask))]

        return [len(g)*max(g) - sum(g) for g in tqdm(job_groups, desc='Groups')]

def user_waste_1(df, min_grouping_size=2):

    N = len(df)
    if min_grouping_size > N:
        return [df['runtime'].max() * min_grouping_size - df['runtime'].sum()]
    else:
        grouping_mask = np.arange(N) // min_grouping_size
        grouping_mask = grouping_mask[:-(N%min_grouping_size)]

        job_groups = [p['runtime'].tolist()\
                         for _, p in df.groupby(np.array(grouping_mask))]

        return [len(g)*max(g) - sum(g) for g in tqdm(job_groups, desc='Groups')]

def global_report(df):
    total_job_node_hours = (df['runtime']*df['node_count']).sum()/60./60.
    report = {
        'total_job_node_hours': total_job_node_hours,
        'earliest_record' : dt.fromtimestamp(min(df['start'])),
        'latest_record' : dt.fromtimestamp(max(df['end'])),
        }
    return report

def single_jobs(df, cluster_delta=10, node_count = 1):
    '''
    :param pandas.DataFrame df:
    :param int cluster_delta:
    :return dict:
    '''

    df_s_n = df[df['node_count'] == node_count]

    col_conv_datetime = ['start', 'end']
    for col in col_conv_datetime:
        df_s_n[col] = pd.to_datetime(df_s_n[col], unit='s')
    
    single_job_users = {k: {'jobs': d} for k, d in tqdm(df_s_n.groupby('user'), desc='Group by user')}

    for jobs in tqdm(single_job_users.values(), desc='Cluster Spliting: user'):
        jobs['jobs'].sort_values('jobid', inplace=True)

        jobs['jobs']['cluster'] =\
            [i for i, g in tqdm(enumerate(
                grouper(jobs['jobs']['jobid'].tolist(), cluster_delta)
                ), desc='Cluster Spliting: jobs') for k in g]
        jobs['clusters'] = [d.sort_values('jobid')\
                                for _, d in jobs['jobs'].groupby('cluster')]



    return single_job_users

def single_jobs_waste_report(single_job_users, **kwargs):
    uN = len(single_job_users)
    no_waste_clust = {u: [c['runtime'].sum()\
                              for c in tqdm(v['clusters'], desc='User {} Cluster'.format(u))]\
                          for u, v in tqdm(single_job_users.iteritems(), desc='Calc No Waste', total=uN)}

    no_waste = sum([sum(v) for v in no_waste_clust.values()])/60./60.
    
    min_grouping_size = kwargs.get('min_grouping_size', 2)

    waste4_clust = {u: [sum(user_waste_4(c, min_grouping_size))\
                            for c in tqdm(v['clusters'], desc='User {} Cluster'.format(u))]\
                        for u, v in tqdm(single_job_users.iteritems(), desc='Calc Waste 4', total=uN)}

    waste4 = sum([sum(i) for i in waste4_clust.values()])/60./60.

    for u, v in tqdm(single_job_users.iteritems(), desc='Packing Report for Each User', total=uN):
        if not 'Report' in single_job_users[u]:
            single_job_users[u]['Report'] = {}
            single_job_users[u]['Report'] = {
                'no_waste_seconds': no_waste_clust[u],
                'metric_4_waste_seconds': {
                    min_grouping_size: waste4_clust[u]
                    },
                }
        else:
            single_job_users[u]['Report']['metric_4_waste_seconds'].update({
                    min_grouping_size: waste4_clust[u]
                    })

    report = {
        'total_used_node_hours': no_waste,
        'metric4_waste_node_hours': waste4,
        }

    return report


def jobs_running(jobs_df, max_jobs_limit=[5, 100,200,500,1000], window_size=60, time_steps=None):

    def _jobs_running_at_time_stats(jr_dict, max_jobs_limit=[100,200,500,1000]):
        ts_slice = {u: v[['start_dt', 'end_dt', 'node_count']].sort_values('start_dt').reset_index()\
                        for u, v in jr_dict.iteritems()}
        res = {}
        for jl in max_jobs_limit:
            for u, v in ts_slice.iteritems():
                _x = v[v.index >= jl-1]
                num_nodes_dropped = _x['node_count'].sum()
                num_jobs_dropped = len(_x)
                if num_jobs_dropped:
                    if u in res:
                        res[u][jl] = {
                            'num_n_drop': num_nodes_dropped,
                            'num_j_drop': num_jobs_dropped
                            }
                        pass
                    else:
                        res[u] = {jl: {
                                'num_n_drop': num_nodes_dropped,
                                'num_j_drop': num_jobs_dropped
                                }}

        return res

    def _jobs_running_at_time(jobs_df, ts):
        '''
        :param pandas.DataFrame jobs_df: isc jobs datafream
        :param datetime.datetime ts: time stamp
        '''
        jobs_running_df = jobs_df[(jobs_df['start_dt'] < ts) &\
                                   (jobs_df['end_dt'] >= ts)]

        users = list(set(jobs_running_df['user']))
        return {u: jobs_running_df[jobs_running_df['user'] == u]\
                    for u in tqdm(users, desc='Spliting out users')}

    start_t = min(jobs_df['start_dt'])
    end_t = max(jobs_df['end_dt'])

    num_steps = int(ceil((end_t - start_t)/timedelta(seconds=window_size)))
    if time_steps:
        pass
    else:
        time_steps = [start_t + timedelta(seconds=n*window_size) for n in range(num_steps)]

    return {ts: {
            'users': _jobs_running_at_time_stats(
                _jobs_running_at_time(jobs_df, ts),
                max_jobs_limit),
            }\
                for ts in tqdm(time_steps, desc='Time Stepping')}


def user_eleg_jobs_report(jobs_in_queue_dict, max_jobs_limit=[100,200,500,1000]):
    '''
    jobs_in_queue_dict
    '''
    jiq_ts_sorted = OrderedDict(sorted(jobs_in_queue_dict.iteritems()))
    
    x = zip(*[(t, len(v['users'])) for t, v in jiq_ts_sorted.iteritems()])
    ts_df = pd.DataFrame({'time_slice':x[0], 'num_users':x[1]})

    for mjl in max_jobs_limit:
        x = zip(*[(t, len([1 for u in v['users'].values() if u > mjl ]))\
                      for t, v in jiq_ts_sorted.iteritems()])
        df = pd.DataFrame({'time_slice': x[0], 'users gt {} jobs'.format(mjl): x[1]})
        df.set_index('time_slice', inplace=True)
        ts_df = ts_df.join(df, how='outer')

    for mjl in max_jobs_limit:
        x = zip(*[(t, sum([max(0, u - mjl) for u in v['users'].values()]))\
                      for t, v in jiq_ts_sorted.iteritems()])
        df = pd.DataFrame({'time_slice': x[0], '{}: jobs bc eleg'.format(mjl): x[1]})
        df.set_index('time_slice', inplace=True)
        ts_df = ts_df.join(df, how='outer')

    return ts_df

def user_running_jobs_report(jobs_running_dict):
    limits = []
    for v in tqdm(jobs_running_dict.values(), desc='Jobs Running Dict'):
        if len(v['users']):
            tot_nodes = {}
            tot_jobs = {}
            tot_users_affected = {}
            for u in tqdm(v['users'].values(), desc='Users'):
                limits.extend(u.keys())
                limits = list(set(limits))
                for limit in u:
                    if u[limit]['num_j_drop']:
                        if limit in tot_nodes:
                            tot_nodes[limit] += u[limit]['num_n_drop']
                            tot_jobs[limit] += u[limit]['num_j_drop']
                            tot_users_affected[limit] += 1
                        else:
                            tot_nodes[limit] = u[limit]['num_n_drop']
                            tot_jobs[limit] = u[limit]['num_j_drop']
                            tot_users_affected[limit] = 1

            v['tot_nodes'] = tot_nodes
            v['tot_jobs'] = tot_jobs
            v['tot_users_affected'] = tot_users_affected
        else:
            v['tot_nodes'] = {l: 0 for l in limits}
            v['tot_jobs'] = {l: 0 for l in limits}
            v['tot_users_affected'] = {l: 0 for l in limits}
    
    jr_ts_sorted = OrderedDict(sorted(jobs_running_dict.iteritems()))
    x = zip(*[[t] + [v['tot_users_affected'].get(k,0) for k in limits] +\
                  [v['tot_nodes'].get(k,0) for k in limits] +\
                  [v['tot_jobs'].get(k,0) for k in limits] for t, v in jr_ts_sorted.iteritems()])
    names = ['time_slice']\
        + ['tot_users_affected_{}'.format(i) for i in limits]\
        + ['tot_nodes_drop_{}'.format(i) for i in limits]\
        + ['tot_jobs_drop_{}'.format(i) for i in limits]
    ts_df = pd.DataFrame({n: c for n,c in zip(names, x)})
    ts_df.set_index('time_slice', inplace=True)
    return ts_df

def plot_ts_df_jobs_running(ts_df):
    
    color_seq = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple', 'tab:brown', 'tab:pink', 'tab:gray', 'tab:olive', 'tab:cyan']
    jobs_names = sorted([n for n in ts_df.keys() if 'jobs_drop' in n],
                        key=lambda x: int(x.split('_')[-1]))
    nodes_names = sorted([n for n in ts_df.keys() if 'nodes_drop' in n],
                         key=lambda x: int(x.split('_')[-1]))
    users_names = sorted([n for n in ts_df.keys() if 'users_affected' in n],
                         key=lambda x: int(x.split('_')[-1]))

    fig, axes = plt.subplots(nrows=3, ncols=1)
    fig.suptitle('Max Running Jobs Policy Analysis\nIncluding 5 limit Policy', fontsize=24)
    # ts_df[jobs_names + nodes_names].plot(secondary_y=jobs_names,
    #                                     color=color_seq,
    #                                     ax=axes[0])
    ts_df[jobs_names].plot(color=color_seq,
                           ax=axes[0])
    
    ts_df[nodes_names].plot(color=color_seq,
                            ax=axes[1])

    ts_df[users_names].plot(color=color_seq,
                            ax=axes[2])

    fig, axes = plt.subplots(nrows=3, ncols=1)
    fig.suptitle('Max Running Jobs Policy Analysis\nNot Including 5 limit Policy', fontsize=24)
    ts_df[jobs_names[1:]].plot(color=color_seq[1:],
                           ax=axes[0])
    
    ts_df[nodes_names[1:]].plot(color=color_seq[1:],
                            ax=axes[1])

    ts_df[users_names[1:]].plot(color=color_seq[1:],
                            ax=axes[2])

def regularized_L2_norm(data, reg_window_size=3):
    ts_tot_df = pd.concat(data.values(), axis=1)
    window = np.ones(int(reg_window_size))/float(reg_window_size)
    clip_high = -int(ceil(reg_window_size/2.0))+1 if int(ceil(reg_window_size/2.0))-1 > 0 else None
    clip_low = int(floor(reg_window_size/2.0))
    for c in tqdm(ts_tot_df, desc="Preping Columns"):
        ts_tot_df[c].interpolate(inplace=True)
        ts_tot_df[c] = np.convolve(ts_tot_df[c], window)[clip_low:clip_high]
    L2_norms = {}
    base_col_names = list(set(['_'.join(k.split('_')[:-1]) for k in ts_tot_df.keys()])) 
    sub_sample = sorted(ts_df.keys()[1:])

    tot_prod = len([(i,j) for i,j in product(base_col_names, sub_sample)])
    for name, sample in tqdm(product(base_col_names, sub_sample), desc='L2_Nomrs', total=tot_prod):
        L2_norms['{}_{}'.format(name, sample)] = sqrt(np.linalg.norm(
                ts_tot_df['{}_60'.format(name)]\
                    - ts_tot_df['{}_{}'.format(name, sample)]
                ))/sqrt(np.linalg.norm(ts_tot_df['{}_60'.format(name)]))

    return L2_norms

def plot_L2_norm_data(L2_norms):
    fig, axes = plt.subplots(nrows=3, ncols=1)
    base_col_names, sub_sample = zip(*[('_'.join(k.split('_')[:-1]), k.split('_')[-1])\
                                           for k in L2_norms.keys()])
    fig.suptitle("Regularized, Nomalized, Root Squared Sum\nAgainst 60 Sec Sampling", fontsize=24)
    sub_sample = sorted(list(set(sub_sample)))
    base_col_names = sorted(list(set(base_col_names)))
    sample_plot_data = {name:[] for name in base_col_names}
    for name, sample in product(base_col_names, sub_sample):
        sample_plot_data[name].append(L2_norms['{}_{}'.format(name, sample)])

    node_drop = sorted([k for k in sample_plot_data if 'nodes_drop' in k],
                       key=lambda x : int(x.split('_')[-1]))
    jobs_drop = sorted([k for k in sample_plot_data if 'jobs_drop' in k],
                       key=lambda x : int(x.split('_')[-1]))
    users_affect = sorted([k for k in sample_plot_data if 'users_affected' in k],
                          key=lambda x : int(x.split('_')[-1]))

    for k in node_drop:
        d = sample_plot_data[k]
        axes[0].plot(range(4), d, label=k)
        axes[0].legend()
        axes[0].xaxis.set_ticks(range(4))
        axes[0].xaxis.set_ticklabels(sub_sample)
        axes[0].set_ylabel('\frac{\|f_60 - f_n\|}{\|f_60\|}')
    for k in jobs_drop:
        d = sample_plot_data[k]
        axes[1].plot(range(4), d, label=k)
        axes[1].legend()
        axes[1].xaxis.set_ticks(range(4))
        axes[1].xaxis.set_ticklabels(sub_sample)

    for k in users_affect:
        d = sample_plot_data[k]
        axes[2].plot(range(4), d, label=k)
        axes[2].legend()
        axes[2].xaxis.set_ticks(range(4))
        axes[2].xaxis.set_ticklabels(sub_sample)

    axes[2].set_xlabel('Sampling (Sec)')

def plot_raw_multiple_sampling_rates(data, policy_limit='200', start_mask=None, end_mask=None):
    '''
    dict with key of the sampling rate as keys
    '''
    fig, axes = plt.subplots(nrows=3, ncols=1)
    fig.suptitle('Comparing running Jobs sample rates', fontsize=24)

    for k, v in data.iteritems():
        if start_mask and end_mask:
            period_mask = (v.index < end_mask) & (v.index >= start_mask)
        elif start_mask:
            period_mask = (v.index >= start_mask)
        elif end_mask:
            period_mask = (v.index < end_mask)
        else:
            period_mask = [True]*len(v.index)
            
        c = [i for i in v.keys() if 'nodes_drop_{}'.format(policy_limit) in i][0]
        v[[c]][period_mask].plot(ax=axes[0])
        axes[0].set_title('Number of Nodes Dropped with a 200 max running jobs policy')
        for tl in axes[0].get_xticklabels():
            tl.set_visible(False)
        c = [i for i in v.keys() if 'jobs_drop_{}'.format(policy_limit) in i][0]
        v[[c]][period_mask].plot(ax=axes[1])
        axes[1].set_title('Number of jobs Dropped with a 200 max running jobs policy')
        for tl in axes[1].get_xticklabels():
            tl.set_visible(False)
        c = [i for i in v.keys() if 'users_affected_{}'.format(policy_limit) in i][0]
        v[[c]][period_mask].plot(ax=axes[2])
        axes[2].set_title('Number of Users affected with a 200 max running jobs policy')

def plot_ts_df(ts_df):
    fig, axes = plt.subplots(nrows=3, ncols=1)
    color_seq = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    color_seq = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple', 'tab:brown', 'tab:pink', 'tab:gray', 'tab:olive', 'tab:cyan']

    eleg_labels = [s for s in ts_df.keys() if 'jobs bc eleg' in s]
    users_labels = [s for s in ts_df.keys() if 'users' in s]

    ts_df[eleg_labels].plot(ax=axes[0], color=color_seq[1:])
    axes[0].set_title('Number of jobs in queue that get deomoted to elegable')

    ts_df[users_labels].plot(ax=axes[1], color=color_seq)
    axes[1].set_title('Useres affected: Absolute scale')

    ts_df[users_labels].plot(ax=axes[2], secondary_y=['num_users'], color=color_seq)
    axes[2].set_title('Useres affected: Adjusted scale')

def single_job_user_group_size_waste_report(single_job_users, max_grouping = 32):
    rep = {n: None for n in range(2, max_grouping+1)}
    for n in rep:
        print "Sarting group size of {}".format(n)
        rep[n] = single_jobs_waste_report(single_job_users, min_grouping_size=n)    

    return rep

def plot_single_job_user_group_size_waste(res):
    p = [(k, r['metric4_waste_node_hours']/r['total_used_node_hours']) for k, r in res.iteritems()]
    fig, axes = plt.subplots(nrows=1, ncols=1)
    axes.plot(zip(*p)[0], zip(*p)[1])
    axes.set_title('Users Wasted node hours vs. Group minimum size.\nUsing waste metric 4')
    xt = [2,6,10,14,18,22,26,30,32]
    axes.xaxis.set_ticks(xt)
    axes.set_ylabel(r'this needs better title')
    axes.set_xlabel('Min Nodes per Job')

def plot_num_users_with_single_node_jobs_hist(single_job_users):
    d = [len(v['jobs']) for v in single_job_users.values()]
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(24, 15))
    bins = map(lambda s: int(pow(2,s)), np.arange(1,log(max(d),2), .5))
    axes.tick_params(labelsize=18)
    axes.hist(d, bins=bins)
    axes.set_xscale('log', basex=2)
    axes.set_ylabel('Number of Useres', fontsize=20)
    axes.set_xlabel('Number of Single Node Jobs', fontsize=20)
    axes.set_title('Users That Launch Single Node Jobs', fontsize=30)
    fig.savefig('Users That Launch Single Node Jobs.png')

    d2 = [v['Report']['metric_4_waste_seconds'][2][0] for v in single_job_users.values()]
    d4 = [v['Report']['metric_4_waste_seconds'][4][0] for v in single_job_users.values()]
    d6 = [v['Report']['metric_4_waste_seconds'][6][0] for v in single_job_users.values()]
    users_outlier2 = [k for k, v in single_job_users.iteritems() \
                     if v['Report']['metric_4_waste_seconds'][2][0] > 1000000]
    uol2 = len(users_outlier2)
    users_outlier6 = [k for k, v in single_job_users.iteritems() \
                     if v['Report']['metric_4_waste_seconds'][6][0] > 1000000]
    u_grid = np.reshape(users_outlier2+['']*(3-uol2%3),(3,int(ceil(float(uol2)/3.))))
    annotation_text = ('Eleven users have single node jobs\n'
                       'that would waist more than\n'
                       '100k node hours if they were\n'
                       'to group their jobs into\n'
                       'pairs of nodes. They are,\n'
                       '{}').format('\n'.join([', '.join(i) for i in\
                                                   u_grid]))
    print annotation_text
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(24, 15))
    bins = map(lambda s: int(pow(10,s)), np.arange(1,7, .1))
    axes.tick_params(labelsize=18)
    axes.hist(d2, bins=bins, label='Groups of 2', alpha=.7)
    # axes.hist(d4, bins=bins, label='Groups of 4', alpha=.7)
    axes.hist(d6, bins=bins, label='Groups of 6', alpha=.7)
    axes.set_xscale('log', basex=10)
    axes.set_ylabel('Number of Useres', fontsize=20)
    axes.set_xlabel('Node Hours Waisted', fontsize=20)
    axes.set_title('Users That Have Waste', fontsize=30)
    axes.text(70000,50, annotation_text, ha="center", va="center", bbox={'boxstyle':'round',
                                                                      'fc':"white"}, size=15)
    axes.legend(fontsize=18)
    fig.savefig('Users That Have Waste.png')
    

'''
np.array(range(11))//2



waste4 = [[sum(user_waste_4(c))\
for c in tqdm(v['clusters'], desc='Cluster')]\
for v in tqdm(single_job_users.values(), desc='Users')]
'''
