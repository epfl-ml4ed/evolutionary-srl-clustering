import pandas as pd
import numpy as np
import itertools as it
from sklearn.cluster import DBSCAN

import sys
sys.path.append('./../../src/')

from project_settings import PREFIX, TEST_ID, KERNEL_METRIC, FEATURE_GROUPS, FEATURE_GROUPS_GENEVA, CLUSTER_METRIC
from etl.postgres_utils import get_select
from features.preprocess import get_feature_kernels,format_feature
from features.load_data import get_data
from models.clustering import spectral_clustering
from models.cluster_analysis import get_scores, get_group_composition
from models.results import save_metadata, save_labels, save_labels_metadata
from models.gridsearch.utils import reorder_labels, get_optimal_params

from visualization.evaluation import plot_mean



def run_gridsearch_features(search=True, feature_groups = FEATURE_GROUPS,
                            image =False, fast= False, save = True, canton = 'ticino',
                            time_unit =  'biweek'):
    if canton == 'geneva':
        time_unit = 'woy'
        if feature_groups == FEATURE_GROUPS:
            feature_groups = FEATURE_GROUPS_GENEVA

    years = [1,2,3,4,5,6]
    period_list = ['semester']

    for period, group, year  in it.product(period_list, feature_groups.keys(),years):
        metric = feature_groups[group]['metric']
        features = feature_groups[group]['features']
        try:
            for feature in features:
                optimal_pipeline([feature], period = period, time_unit = time_unit,
                                   year = year, metric = metric, search = search,
                                   image = image, fast = fast, save =save, canton = canton)
        except  Exception as e:
            print("error", features, year, e)



def optimal_pipeline(features, period = 'yearly', time_unit = 'biweek',
                       year = 4,  metric = 'dtw', search = True, image = False,
                       fast = False, save = True, canton = 'ticino'):

    if period in ['complete', 'semester']:
        time_unit = "{0}_{1}".format(period, time_unit)

    df= get_data(time_unit = time_unit, year = year, canton = canton,period = period)

    # gridsearch of all features
    experiment_name = '{0}_{1}_{2}_{3}_{4}'.format(PREFIX,
                                           '_'.join(features),
                                           time_unit,canton,
                                           year).replace(" ", "_")

    test_name = '{0}_{1}_{2}_{3}_{4}'.format(TEST_ID,
                                           '_'.join(features),
                                           time_unit,canton,
                                           year).replace(" ", "_")
    print(test_name)
    if len(df)>0:

        # Tunes the window, gamma and number of clusters
        if search:
            gridsearch_spectral(experiment_name, df, features, metric, time_unit, period,
                                school_year = year, image = image, fast = fast,
                                canton = canton)

        # Retrivies optimal parameters from gridsearch
        optimal_clusters, gamma, window, exp_id, model = get_optimal_params(experiment_name, canton=canton,
                                                        heuristic =KERNEL_METRIC)

        # Runs spectral clustering with optimal parameters and
        if save:
            save_model_labels(model, test_name, exp_id, df, features, metric,
                          optimal_clusters, gamma, window, time_unit, canton=canton)
        save_labels_metadata(canton, test_name, exp_id, features, model, metric,
                             optimal_clusters, gamma, window, time_unit)


def gridsearch_spectral(experiment_name, df, features, metric,
                        time_unit, period = 'yearly', school_year = 4,
                        CLUSTER_LIST = [2, 3, 4, 5],
                        GAMMA_LIST = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8 , 0.9, \
                        1, 1.1 ,1.2, 1.3, 1.4,1.5, 1.6, 1.7, 1.8],
                        WINDOW_LIST = [0,1,2,3,4,5,6,7,8,9,10,11, 12],
                        image = False, fast = False, canton = 'ticino'):

    if metric == 'euclidean':
        WINDOW_LIST = [1]

    if fast:
        CLUSTER_LIST = [3]
        GAMMA_LIST = [1]
        WINDOW_LIST = [1]

    for gamma, window in it.product(GAMMA_LIST,WINDOW_LIST):
        # affity distane
        kernel_matrix, _ = get_feature_kernels(df,features,metric,gamma,window,time_unit)

        for clusters in CLUSTER_LIST:
            kmeans, proj_X, eigenvals_sorted = spectral_clustering(kernel_matrix,clusters)

            cluster_labels =   kmeans.labels_
            df['label'] = cluster_labels

            s_distortion, s_bic, s_sil_km = get_scores(kmeans, kernel_matrix, proj_X)
            s_eigen = eigenvals_sorted[:10].tolist()

            save_metadata(
                      canton,
                      experiment_name,
                      time_unit,
                      period,
                      features,
                      'spectral',
                      metric,
                      school_year,
                      gamma,
                      window,
                      clusters,
                      s_distortion, s_bic, s_sil_km,
                      s_eigen)





def save_model_labels(model, experiment_name, exp_id, df, features, metric,
                        optimal_clusters, gamma, window, time_unit, canton = 'ticino'):

    kernel_matrix, distance_matrix = get_feature_kernels(df,features,metric,
                                                        gamma,window,time_unit)
    if model == 'spectral':
        kmeans, _, _ = spectral_clustering(kernel_matrix,
                                          optimal_clusters)
        cluster_labels =   kmeans.labels_
    else:
        dbscan = DBSCAN(metric = 'precomputed', eps = gamma)
        cluster_labels = dbscan.fit_predict(distance_matrix) + 1

    df['label'] = reorder_labels(pd.Series(cluster_labels), experiment_name)

    save_labels(df, experiment_name, time_unit)
