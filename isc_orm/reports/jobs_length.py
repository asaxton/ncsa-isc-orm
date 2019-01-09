import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import axes3d, Axes3D
from isc_orm.core import get_isc_db
from isc_orm.core import get_jobs_df
from isc_orm.core import get_job_in_time_range

from sklearn.cluster import KMeans, MiniBatchKMeans, SpectralClustering, DBSCAN
from sklearn.metrics import silhouette_samples, silhouette_score
from sklearn.decomposition import PCA

import numbers
import numpy as np
import pandas as pd
from datetime import datetime as dt
import itertools
import time

def plot_job_length_hist():
    db = get_isc_db()
    db, om = get_isc_db()
    
    # jobs_df = get_jobs_df(db)
    start_dt = dt(2017, 11, 1)
    end_dt = dt(2018, 12, 10)

    jobs_df = get_job_in_time_range(start_dt, end_dt, db)
    jobs_df = jobs_df[jobs_df['status'] == "Completed"]
    col_conv_to_int = [
        'RLnodect',
        'RUcput',
        'resname'
        ]
    def hash_for_cluster(x):
        return hash(x) % 1000000000

    jobs_df = jobs_df.applymap(lambda x : -2 if  x is np.nan else -1 if x is None else x)

    def hash_everything_else(x):
        try:
            return int(x)
        except:
            return hash_for_cluster(x)

    for c in col_conv_to_int:
        jobs_df[c] = jobs_df[c].map(hash_everything_else)
        jobs_df[c].astype(np.int64)

    start_stop = jobs_df[['epilogue_start',
                          'epilogue_end',
                          'prologue_start',
                          'prologue_end',
                          'moab_start',
                          'moab_end',
                          'start', 'end']]
    
    jobs_df['length'] = start_stop['end'] - start_stop['start']
    jobs_df['moab_length'] = start_stop['moab_end'] - start_stop['moab_start']
    jobs_df['prologue_length'] = start_stop['prologue_end'] - start_stop['prologue_start']
    jobs_df['epilogue_length'] = start_stop['epilogue_end'] - start_stop['epilogue_start']


    categorical_columns = [c for c in jobs_df.columns if not isinstance(jobs_df[c].values[0], numbers.Number)]
    numeric_columns = list(set(jobs_df.columns).difference(set(categorical_columns)))

    jobs_cat_wip_df = jobs_df[categorical_columns].applymap(hash_for_cluster)

    jobs_cat_wip_df.columns = ["hash_{}".format(c) for c in jobs_cat_wip_df.columns]

    jobs_wip_df = pd.concat([jobs_df[numeric_columns], jobs_cat_wip_df], axis=1)

    for c in jobs_wip_df:
        jobs_wip_df[c].loc[~jobs_wip_df[c].notnull()] = -3


    if "PCA" in task:
        n_c = 20
        score_list = {}
        for n_c in range(3, 30):
            print('n_c {}'.format(n_c))
            pca = PCA(n_components=28)
            s = pca.fit_transform(jobs_wip_df)
            #cluster = SpectralClustering(n_clusters=n_c, random_state=0).fit(s)
            #cluster = DBSCAN()
            cluster = KMeans(n_clusters=n_c, random_state=1, n_jobs=2).fit(s)
            pred = cluster.predict(s)
            sample_mask = np.random.choice(range(len(pred)), size=10000, replace=False)
            _s = silhouette_samples(s[sample_mask], pred[sample_mask])

            s_score = [ _s[pred[sample_mask] == i] for i in range(n_c)]
            _ = [i.sort() for i in s_score]
            score_list[n_c] = s_score
            
            pca = PCA(n_components=3)
            s = pca.fit_transform(jobs_wip_df)
        plt_index = 1
        plt.suptitle('Sample silhouette scores for various class sizes')
        for k, s_score in score_list.items():
            if k in [3, 4, 5, 7, 8, 9, 15, 26, 27]:
                pass
            else:
                continue
            plt.subplot(3,3,plt_index)
            for j, i in enumerate(s_score):
                r = list(zip(*enumerate(i)))
                plt.plot(r[1], r[0] , label=j)
            plt.title('Num Classes {}'.format(k))
            if plt_index in [7,8,9]:
                plt.xlabel('s score')
            if plt_index in [1, 4, 7]:
                plt.ylabel('sample index')
            plt_index += 1
        plt.legend(loc='center left', bbox_to_anchor=(1, 2))
            # plt.figure()
        #for j, i in enumerate(s_score):
        #    plt.plot(*zip(*enumerate(i)), label=j)
        #plt.legend()

        plt.figure()
        plt_index = 1
        for k, i in list(score_list.items()):
            means = [l.mean() for l in i]
            if (k-2)%3 == 0:
                plt.legend()
                plt.subplot(3,3,plt_index)
                plt_index += 1
            plt.hist(means, bins, alpha=.3, label=k)

        plt.figure()
        means_tot = [np.mean([l.mean() for l in i]) for k, i in list(score_list.items())]
        median_tot = [np.median([np.median(l) for l in i]) for k, i in list(score_list.items())]
        plt.title('Scores agrigated for each class size')
        plt.plot(range(3,30), means_tot, label="Mean")
        plt.plot(range(3,30), median_tot, label="Median")
        plt.xlabel("Class size")
        plt.ylabel("S Score")
        plt.legend()

        for n_c in [8, 15]:
            print('n_c {}'.format(n_c))
            pca = PCA(n_components=28)
            s = pca.fit_transform(jobs_wip_df)
            #cluster = SpectralClustering(n_clusters=n_c, random_state=0).fit(s)
            #cluster = DBSCAN()
            cluster = KMeans(n_clusters=n_c, random_state=1, n_jobs=2).fit(s)
            pred = cluster.predict(s)
            sample_mask = np.random.choice(range(len(pred)), size=10000, replace=False)
            pca = PCA(n_components=3)
            s = pca.fit_transform(jobs_wip_df)

            fig = plt.figure()

            ax = Axes3D(fig)

            for i in range(n_c):
                ax.scatter(*list(zip(*(s[sample_mask][pred[sample_mask] == i]))), '.', label=i)

    if "kmean" in task:
        score_list = []
        rms_list = []
        for n_clusters in range(100, 120):
            tick = time.time()
            print('kmeans for n_clusers {}'.format(n_clusters))
            kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(jobs_wip_df.values)

            pred = kmeans.predict(jobs_wip_df.values)
            sample_mask = np.random.choice(range(len(pred)), size=10000, replace=False)

            _s = silhouette_samples(jobs_wip_df.values[sample_mask], pred[sample_mask])

            score_list.append(_s)

            labeled = list(sorted(zip(pred, jobs_wip_df.values), key=lambda x : x[0]))
            grouped_classes = itertools.groupby(labeled, key=lambda x : x[0] )
            _rSq = 0
            for k_label, k_list in grouped_classes:
                k_mean_displace = kmeans.cluster_centers_[k_label] - np.mean(list(zip(*k_list))[1], axis=0)
                _rSq += sum(k_mean_displace*k_mean_displace)
            rms_list.append(np.sqrt(_rSq))
            print('_rSq {} time {:.4f} min'.format(rms_list[-1], (time.time() - tick)/60))
        
    fig, ax = plt.subplots()
    
    ax.set_yscale('log')

    start_stop['length'].hist(bins=range(0, 200000, 120))


