import itertools as it
import numpy as np
import pandas as pd
from tqdm import tqdm

import sys
sys.path.append('./../../src/')

from project_settings import PREFIX, TEST_ID,FEATURE_GROUPS, FEATURE_GROUPS_GENEVA
from etl.postgres_utils import get_select
from features.load_data import get_data
from features.preprocess import format_feature, get_distance_matrix, \
get_affinity_matrix, normalize
from models.clustering import spectral_clustering
from models.gridsearch.utils import reorder_labels, get_optimal_params
from models.cluster_analysis import get_scores, get_group_composition
from models.results import save_metadata, save_labels, save_labels_metadata, save_group_metadata


def create_feature_groups(search= True, feature_groups = FEATURE_GROUPS,
canton = 'ticino', time_unit = 'biweek'):
    if canton == 'geneva':
        time_unit = 'woy'
        if feature_groups == FEATURE_GROUPS:
            feature_groups = FEATURE_GROUPS_GENEVA

    years = [1,2,3,4,5,6]
    period_list = ['semester']

    for period, group, year in tqdm(it.product(period_list, feature_groups.keys(), years)):
        metric = feature_groups[group]['metric']
        features = feature_groups[group]['features']
        try:

            optimal_pipeline_groups(period, group, year, metric, features,
                                    time_unit, search, canton= canton)
        except  Exception as e:
            print(e)



def optimal_pipeline_groups(period, group, year, metric, features,
                            time_unit, search = True, save = True, canton = 'ticino'):

    if period in ['complete', 'semester']:
        time_unit = "{0}_{1}".format(period, time_unit)
    df = get_data(time_unit = time_unit, year = year, canton = canton,period = period)

    experiment_name = '{}_{}_{}_{}_{}'.format(TEST_ID,
                                           group,
                                           time_unit, canton,
                                           year).replace(" ", "_")

    if len(df) > 0:
        kernel_matrix, gamma_list, window_list = get_best_kernels(df, year, features, metric, time_unit,canton)
        if search:
            gridsearch_spectral_clusters(df, kernel_matrix, experiment_name,
                      time_unit,  period, features, metric, year, canton = canton)


        optimal_clusters, _, _, exp_id, model = get_optimal_params(experiment_name,
                                                                    canton = canton)


        if save: # Runs spectral clustering with optimal parameters and
            save_group_labels(df, kernel_matrix, experiment_name, optimal_clusters, time_unit)

            save_group_metadata(canton, experiment_name, exp_id, features, metric,
                                 optimal_clusters, gamma_list, window_list,
                                 time_unit, group)




def save_group_labels(df, kernel_matrix, experiment_name, optimal_clusters, time_unit):

    kmeans, _, _ = spectral_clustering(kernel_matrix,
                                      optimal_clusters)
    cluster_labels =   kmeans.labels_
    df['label'] = reorder_labels(pd.Series(cluster_labels), experiment_name)

    save_labels(df, experiment_name, time_unit)




def gridsearch_spectral_clusters(df, kernel_matrix, experiment_name,
          time_unit,  period, features, metric, year, CLUSTER_LIST = [2,3,4,5],
          table_name = 'results.gridsearch', canton = 'ticino'):

    for clusters in CLUSTER_LIST:
        kmeans, proj_X, eigenvals_sorted = spectral_clustering(kernel_matrix, clusters)

        cluster_labels =   kmeans.labels_
        df['label'] = cluster_labels

        s_distortion, s_bic, s_sil_km = get_scores(kmeans, kernel_matrix, proj_X)
        s_eigen = eigenvals_sorted[:10].tolist()

        save_metadata(canton,
                  experiment_name,
                  time_unit,
                  period,
                  features,
                  'spectral_group',
                  metric,
                  year,
                  -1,
                  -1,
                  clusters,
                  s_distortion, s_bic, s_sil_km,
                  s_eigen,
                  table_name = table_name
                 )


def get_best_kernels(df, year, features, metric, time_unit = 'biweek', canton = 'ticino'):
        len_features = len(features)
        students = len(df)
        kernel_affinities = np.zeros((len_features,students,students))
        gamma_list = []
        window_list = []

        for i in range(len_features):
            feature = features[i]
            X_formatted =  format_feature(df, feature, metric, time_unit)
            gamma, window, _ = get_gamma_window(feature, year, time_unit, canton)
            gamma_list.append(gamma)
            window_list.append(window)
            D = get_distance_matrix(X_formatted, metric, window, feature)
            Dn = normalize(D)
            kernel_affinities[i] = get_affinity_matrix(Dn, gamma)

        combined_affinity = np.sum(kernel_affinities, axis = 0)

        return combined_affinity, gamma_list, window_list


def get_gamma_window(feature, year, time_unit, canton = 'ticino'):
    experiment_name = '{}_{}_{}_{}_{}'.format(TEST_ID,
                                           feature,
                                           time_unit,canton,
                                           year)

    query = """
            select * from results.labels_metadata
            where experiment_name = '{experiment_name}'
            order by experiment_date desc
            """.format(experiment_name = experiment_name)
    params = get_select(query)

    if len(params)==0:
        print("Error: This should not happen. No data in metadata for this experiment: ", experiment_name)
        gamma = 1
        window = 2
        clusters = 3
    else:
        gamma = params.gamma[0]
        window = params.window_size[0]
        clusters = params.optimal_clusters[0]
    return gamma, window, clusters
