"""
Tunes features within feature groups
"""

import itertools as it
import numpy as np
import pandas as pd
from tqdm import tqdm

import sys
sys.path.append('./../../src/')

from project_settings import PREFIX, FEATURE_GROUPS
from etl.postgres_utils import get_select
from features.load_data import get_data
from features.preprocess import format_feature, get_distance_matrix, get_affinity_matrix
from models.clustering import spectral_clustering
from models.gridsearch.utils import reorder_labels
from models.cluster_analysis import get_scores, get_group_composition
from models.results import save_metadata, save_labels, save_labels_metadata, save_group_metadata
from models.gridsearch.groups import gridsearch_spectral_clusters


def optimize_feature_groups(search= True, feature_groups = FEATURE_GROUPS):

    years = [1,2,3]# [1,2,3,4,5,6]
    period_list = ['yearly'] #'semester'
    time_unit = 'biweek'

    for period, group, year in tqdm(it.product(period_list, feature_groups.keys(), years)):
        metric = feature_groups[group]['metric']
        features = feature_groups[group]['features']

        try:
            optimal_pipeline_feature_groups(period, group, year, metric, features, time_unit)
        except  Exception as e:
            print(e)
            print("error", features, year)





def optimal_pipeline_feature_groups(period, group, year, metric, features,
                            time_unit, search = True, save = True):

    if period in ['complete', 'semester']:
        time_unit = "{0}_{1}".format(period, time_unit)
    df = get_data(time_unit = time_unit, year = year)

    experiment_name = '{}_{}_{}_{}'.format(PREFIX,
                                           group,
                                           time_unit,
                                           year).replace(" ", "_")
    table_name =  'results.gridsearch_fg'
    clusters_list = [2,3,4]
    if len(df) > 0:
        if search:
            gammas, windows = get_combinations_features(features)
            for gamma_list, window_list in it.product(gammas, windows):


                kernel_matrix = get_kernels_feature_groups(df, year, features, metric, time_unit,
                                                            gamma_list, window_list)
                for clusters in clusters_list:
                    spectral_feature_groups(df, kernel_matrix, experiment_name,
                              time_unit,  period, features, metric, year, clusters, gamma_list, window_list,
                              table_name = table_name)


        optimal_clusters, exp_id, gamma_list, window_list = get_optimal_params(experiment_name, table_name)


        if save: # Runs spectral clustering with optimal parameters and
            save_feature_group_labels(df, year, metric, features, kernel_matrix, experiment_name, optimal_clusters,
                                            gamma_list, window_list, time_unit)

            save_group_metadata(experiment_name, exp_id, features, metric,
                                 optimal_clusters, gamma_list, window_list,
                                 time_unit, group)


def spectral_feature_groups(df, kernel_matrix, experiment_name,
          time_unit,  period, features, metric, year, clusters, gamma_list, window_list,
          table_name = 'results.gridsearch'):

        kmeans, proj_X, eigenvals_sorted = spectral_clustering(kernel_matrix, clusters)

        cluster_labels =   kmeans.labels_
        df['label'] = cluster_labels

        s_distortion, s_bic, s_sil_km = get_scores(kmeans, kernel_matrix, proj_X)
        s_eigen = eigenvals_sorted[:10].tolist()

        save_metadata(experiment_name,
                  time_unit,
                  period,
                  features,
                  'spectral_group',
                  metric,
                  year,
                  gamma_list,
                  window_list,
                  clusters,
                  s_distortion, s_bic, s_sil_km,
                  s_eigen,
                  table_name = table_name
                 )



def get_combinations_features(features):
    r = len(features)
    gammas_search = [0.7,0.9, 1, 1.1, 1.3]
    window_search = [0, 1, 2, 3, 4]

    if r == 2:
        gammas = [list(x) for x in list(it.product(gammas_search, gammas_search ))]
        windows = [list(x) for x in list(it.product(window_search, window_search ))]
    elif r == 3:
        gammas = [list(x) for x in list(it.product(gammas_search, gammas_search ,gammas_search))]
        windows = [list(x) for x in list(it.product(window_search, window_search ,window_search))]

    return gammas, windows


def save_feature_group_labels(df, year, metric, features, kernel_matrix, experiment_name, optimal_clusters, \
                                gamma_list, window_list, time_unit):

    kernel_matrix = get_kernels_feature_groups(df, year, features, metric, time_unit,
                                                gamma_list, window_list)

    kmeans, _, _ = spectral_clustering(kernel_matrix,
                                      optimal_clusters)
    cluster_labels =   kmeans.labels_
    df['label'] = reorder_labels(pd.Series(cluster_labels), experiment_name)

    save_labels(df, experiment_name, time_unit)




def get_kernels_feature_groups(df, year, features, metric, time_unit, gamma_list, window_list):
    len_features = len(features)
    students = len(df)
    kernel_affinities = np.zeros((len_features,students,students))

    for i in range(len_features):
        feature = features[i]
        X_formatted =  format_feature(df, feature, metric, time_unit)
        gamma = gamma_list[i]
        window = window_list[i]
        D = get_distance_matrix(X_formatted, metric, window)
        kernel_affinities[i] = get_affinity_matrix(D, gamma)

    combined_affinity = np.sum(kernel_affinities, axis = 0)

    return combined_affinity
