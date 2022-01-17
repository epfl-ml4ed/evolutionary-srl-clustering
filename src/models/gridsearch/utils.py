import pandas as pd
import numpy as np
import itertools as it

from etl.postgres_utils import get_select
from sklearn.metrics import  f1_score

from project_settings import PREFIX, CLUSTER_METRIC, TEST_ID, FEATURE_GROUPS, FEATURE_GROUPS_GENEVA

def get_factor(canton, factor):
    if canton=="ticino":
        thresh = np.ceil(139/factor)
    else:
        thresh = np.ceil(55/factor)
    return int(thresh)



def reorder_labels(cluster_labels, experiment_name, previous_labels = [], df_curr= None):
    year = int(experiment_name.split('_')[-1])

    if year== 1: # order greater to smaller
        new_labels =  simple_reordering(cluster_labels)
    else:
        if len(previous_labels)==0:
            # get previous year
            previous_exp = '_'.join(experiment_name.split('_')[:-1] + [str(year-1)])
            query = 'select student, {0} as label_prev from labels.{0}'.format(previous_exp)
            df_prev = get_select(query)
            if len(df_prev) > 0:
                previous_labels = df_prev.label_prev.tolist()
                if df_curr is not None:
                    df_merged = df_prev[['student', 'label_prev']].merge(df_curr[['student','label']],on = 'student',  how = 'right')
                    df_merged = df_merged.fillna(8)
                    #cluster_labels = pd.Series(list(map(int, df_merged.label.tolist())))
                    #previous_labels =  pd.Series(list(map(int, df_merged.label_prev.tolist())))

                new_labels = find_permutations(cluster_labels, previous_labels)
            else:
                new_labels =  simple_reordering(cluster_labels)
        else:
            new_labels = find_permutations(cluster_labels, previous_labels)

    return new_labels.values


def simple_reordering(cluster_labels):
    clusters = np.unique(cluster_labels).shape[0]
    old = list(cluster_labels.value_counts().index)
    new = list(np.unique(cluster_labels))
    dict_transform = dict(zip(old, new))
    new_labels = cluster_labels.map(dict_transform)
    return new_labels

def find_permutations(cluster_labels, previous_labels):
    clusters = np.unique(cluster_labels).tolist()
    perm_clusters = [list(x) for x in list(it.permutations(clusters)) if list(x) != clusters]

    max_f1 = f1_score(cluster_labels,previous_labels, average = 'weighted')
    best_perm = clusters

    for comb in perm_clusters:
        dict_transform = dict(zip(clusters, comb))
        new_labels = pd.Series(cluster_labels).map(dict_transform)
        current_f1  = f1_score(new_labels,previous_labels, average = 'weighted')
        if current_f1 > max_f1:
            max_f1 = current_f1
            best_perm = comb

    dict_transform = dict(zip(clusters, best_perm))
    new_labels = pd.Series(cluster_labels).map(dict_transform)
    return new_labels






def get_optimal_params(experiment_name, canton = 'ticino'):


    query_generic = """
        with latest as (
            select distinct on (experiment_name, clusters, window_size, gamma) *
            from results.gridsearch
            where experiment_name = '{experiment_name}'
            order by experiment_name, clusters, window_size, gamma, experiment_date desc
        )
        select *
        from latest
        order by s_sil_km desc
        limit 1
        """
    query = query_generic.format(experiment_name = experiment_name)
    params = get_select(query)

    if len(params) == 0:
        print(query)
        print("Error: This shouldn't happen. Make sure the table is not empty.")
        print("This is the missing table", experiment_name)
        print("Set search parameter to True")
        gamma = 1
        window = 1
        exp_id = None
        model = 'spectral'
        optimal_clusters =  2
    else:
        gamma = params.gamma[0]
        window = params.window_size[0]
        optimal_clusters =  params.clusters[0]
        exp_id = params.experiment_date[0]
        model = params.model[0]
        print("Clusters: ", optimal_clusters, gamma, window)

    return optimal_clusters, gamma, window, exp_id, model
