import pandas as pd
import numpy as np
import itertools as it

import sys
sys.path.append('./../../src/')


from models.clustering import spectral_clustering
from models.gridsearch.features import get_optimal_params
from models.gridsearch.groups import get_gamma_window, get_best_kernels
from models.results import save_labels, save_labels_metadata
from features.preprocess import get_feature_kernels
from features.load_data import get_data
from project_settings import PREFIX, FEATURE_GROUPS, FEATURE_GROUPS_GENEVA, TEST_ID
from models.affect.adaptive import batch_affect_spectral
from etl.postgres_utils import get_select


def get_evolutionary_clusters(feature_groups = FEATURE_GROUPS, time_unit = 'biweek',
                            individual = True, canton = 'ticino'):

    if canton == 'geneva':
        time_unit = 'woy'
        if feature_groups == FEATURE_GROUPS:
            feature_groups = FEATURE_GROUPS_GENEVA


    period_list = [ 'semester']


    # DROP results.group_labels_metadata
    for period, group in it.product(period_list, feature_groups.keys()):
        metric = feature_groups[group]['metric']
        features = feature_groups[group]['features']
        try:
            evolutionary_clustering(period, group, time_unit, metric = metric,
                                    canton = canton,
                                    feature_groups = list(feature_groups.keys()))
            if individual:
                for feature in features:
                    evolutionary_clustering(period, feature, time_unit,
                                            metric = metric, canton = canton,
                                            feature_groups = list(feature_groups.keys()))
        except  Exception as e:
            print(e)



def get_kernel_year(feature, metric, time_unit, year, canton = 'ticino', period = 'yearly'):

    gamma, window, clusters = get_gamma_window(feature, year, time_unit, canton = canton)

    df= get_data(time_unit = time_unit, year = year,  canton = canton,period = period)
    kernel_matrix, _ = get_feature_kernels(df,
                                    [feature],
                                    metric,
                                    gamma,
                                    window,
                                    time_unit)

    return kernel_matrix, clusters


def get_kernel_group(feature, metric, time_unit, year, canton = 'ticino', period = 'yearly'):

    clusters, list_features = get_group_clusters(feature, year, time_unit,canton = canton)

    df= get_data(time_unit = time_unit, year = year,  canton = canton, period = period)
    kernel_matrix,_ ,_ = get_best_kernels(df, year, list_features,
                            metric, time_unit,  canton = canton)

    return kernel_matrix, clusters



def get_group_clusters(feature, year, time_unit, canton ='ticino'):
    experiment_name = '{}_{}_{}_{}_{}'.format(TEST_ID, feature, time_unit, canton, year)
    query = """select * from results.group_labels_metadata
            where experiment_name = '{}'
            order by experiment_date desc""".format(experiment_name)
    df = get_select(query)
    clusters = df.optimal_clusters[0]
    list_features = list(df.features[0])
    return clusters,list_features


def evolutionary_clustering(period, feature, time_unit,
                            metric = 'dtw', search = True, canton = 'ticino',
                            feature_groups = list(FEATURE_GROUPS.keys()) ):

    if period in ['complete', 'semester']:
        time_unit = "{0}_{1}".format(period, time_unit)

    if period == 'semester':
        years = [1,2,3,4,5,6]
    else:
        years = [1,2,3]

    df= get_data(time_unit = time_unit, year = 1, canton = canton, period = period)
    kernel_matrix = np.zeros((len(years), len(df), len(df)))
    optimal_clusters = []

    for year in years:
        if feature in feature_groups:
            kernel_matrix[year-1], clusters =  get_kernel_group(feature, metric,
                                                time_unit, year, canton = canton, period = period)
        else:
            kernel_matrix[year-1], clusters =  get_kernel_year(feature, metric,
                                                time_unit, year, canton = canton, period = period)
        optimal_clusters.append(clusters)

    experiment_name = '{}_{}_{}_{}'.format(TEST_ID, feature, time_unit, canton)
    clu, W_bar,alpha_est = batch_affect_spectral(kernel_matrix, optimal_clusters,
                                                experiment_name, search,period = period)



