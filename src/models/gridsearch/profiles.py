from kmodes.kmodes import KModes
from sklearn.metrics import silhouette_score
from datetime import datetime
import pickle
from pathlib import Path

import random
import numpy as np
import seaborn as sns
import pandas as pd
import itertools as it

import sys
sys.path.append('./../../src/')

from project_settings import PREFIX, FEATURE_GROUPS, FEATURE_GROUPS_GENEVA, TEST_ID
from etl.postgres_utils import get_select, execute_query
from features.load_data import get_data
from models.cluster_analysis import get_scores,  get_pairwise_wilcoxon, get_group_composition
from models.results import save_kmodes, save_metadata, save_labels, save_labels_metadata, save_group_metadata
from models.gridsearch.utils import reorder_labels, get_optimal_params
from visualization.evaluation import plot_boxplot

SEED = 123


def run_all_profiles(canton = 'ticino', search = True,time_unit = 'biweek' ):
    if canton == 'geneva':
        time_unit = 'woy'
    period_list = ['semester']#
    years = [1,2,3,4,5,6]
    adaptive_list = ['', 'adaptive_']

    for period, adaptive, year in it.product(period_list, adaptive_list, years):
        try:
            print(period, adaptive, year)
            optimal_pipeline_profiles(period, year, time_unit, adaptive,
             search = search, canton = canton)

        except  Exception as e:
            print(e)
            print("error", year)



def rename_table_profiles(experiment_name, new_experiment_name):
    query = """
    drop table if exists labels.{new_experiment_name};
    create table labels.{new_experiment_name} as (
    select
    student,
    calendar_year,
    {experiment_name} as {new_experiment_name}
    from labels.{experiment_name});
    """.format(experiment_name  = experiment_name,
    new_experiment_name = new_experiment_name)

    execute_query(query)

def optimal_pipeline_profiles(period, year, time_unit, adaptive, search = True,
                                canton = 'ticino'):

    if period in ['complete', 'semester']:
        time_unit = "{0}_{1}".format(period, time_unit)
    experiment_name = '{}{}_{}_{}_{}_{}'.format(adaptive, TEST_ID, 'profile',
                                           time_unit, canton, year)


    if year == 1 and adaptive == 'adaptive_':
        print(adaptive)
        old_experiment_name = '_'.join(experiment_name.split('_')[1:])
        print(old_experiment_name,experiment_name)
        rename_table_profiles(experiment_name = old_experiment_name, new_experiment_name = experiment_name)
    else:
        if canton == 'ticino':
            feature_groups = list(FEATURE_GROUPS.keys())
        else:
            feature_groups = list(FEATURE_GROUPS_GENEVA.keys())

        df = get_data(year = year,  metric = 'labels',
         time_unit = time_unit, adaptive = adaptive, canton = canton, period = period)


        if df is not None:

            df_feat = df[feature_groups].fillna(5)


            if search:
                gridsearch_kmodes(df, df_feat, experiment_name, period, year,
                                                        time_unit, canton= canton)

            optimal_clusters, _, _, _, _ = get_optimal_params(experiment_name, canton = canton)

            # Runs spectral clustering with optimal parameters and
            save_profile_labels(df, df_feat, experiment_name, optimal_clusters,
                                time_unit, period, adaptive, year,canton = canton)



def run_single(period , year, optimal_clusters, adaptive,
                time_unit = 'biweek', save= True, canton =  'ticino', plot = False):

    if canton == 'ticino':
        feature_groups = list(FEATURE_GROUPS.keys())
    else:
        feature_groups = list(FEATURE_GROUPS_GENEVA.keys())

    if period in ['complete', 'semester']:
        time_unit = "{0}_{1}".format(period, time_unit)
    df = get_data(year = year,  metric = 'labels', time_unit = time_unit,
                    adaptive = adaptive, canton = canton)

    if len(df) > 0:

        experiment_name = '{}{}_{}_{}_{}_{}'.format(adaptive, TEST_ID, 'profile',
                                               time_unit, canton,  year)

        df_feat = df[feature_groups].fillna(5)

        km = KModes(n_clusters=optimal_clusters, init='Huang', n_init=550,
                    max_iter = 1000, random_state = SEED)

        cluster_labels= km.fit_predict(df_feat)
        df['label'] = cluster_labels
        df['label'] = reorder_labels(pd.Series(cluster_labels), experiment_name)
        cluster_labels = df['label'].tolist()

        if plot:
            save_data(experiment_name, cluster_labels,km.cluster_centroids_)
        if save:
            save_labels(df, experiment_name, time_unit)

    return df, km, cluster_labels


def load_data(experiment_name):
    folder_dir = """./../results/{prefix}/temp_data/{experiment_name}
    """.format(prefix = TEST_ID,experiment_name=experiment_name )

    data_dir = "{0}/cluster_labels.txt".format(folder_dir)
    with open(data_dir, "rb") as fp:   #Pickling
        cluster_labels = pickle.load(fp)

    data_dir = "{0}/centroids.npy".format(folder_dir)
    centroids =np.load(data_dir)
    return cluster_labels,centroids



def save_data(experiment_name, cluster_labels,centroids):
    folder_dir = """./../results/{prefix}/temp_data/{experiment_name}
    """.format(prefix = TEST_ID,experiment_name=experiment_name )
    Path(folder_dir).mkdir(parents=True, exist_ok=True)

    data_dir = "{0}/cluster_labels.txt".format(folder_dir)

    with open(data_dir, "wb") as fp:   #Pickling
        pickle.dump(cluster_labels, fp)

    data_dir = "{0}/centroids".format(folder_dir)
    np.save(data_dir, centroids)




def save_profile_labels(df, df_feat, experiment_name, optimal_clusters,
                        time_unit, period, adaptive, year,canton = 'ticino'):

    km = KModes(n_clusters=optimal_clusters, init='Huang', n_init=150,
                max_iter = 500, random_state = 123)

    cluster_labels= km.fit_predict(df_feat)
    df['label'] = cluster_labels
    df['label'] = reorder_labels(pd.Series(cluster_labels), experiment_name)

    if canton == 'ticino':
        plot_boxplot(df,  time_unit, feature = 'sum_medias', year = year, period = period,
        adaptive = adaptive,group = 'profiles', canton = 'ticino' )

    save_labels(df, experiment_name, time_unit)

    save_group_metadata(canton, experiment_name, datetime.now() , [], 'labels',
                         optimal_clusters, [], [],time_unit, 'profiles')





def gridsearch_kmodes(df, df_feat, experiment_name, period, year, time_unit,
                        CLUSTER_LIST = [2,3,4,5,6], canton = 'ticino'):
                        #CLUSTER_LIST = [2,3,4,5,6]):
    for clusters in CLUSTER_LIST:
            km = KModes(n_clusters=clusters, init='Huang',
                        n_init=150,  max_iter = 100, random_state = SEED)

            cluster_labels= km.fit_predict(df_feat)
            df['label'] = cluster_labels

            if len(np.unique(cluster_labels)) == 1:
                print("Clusters = 1 :( )")
                print(df_feat.head())
                print(clusters)
                s_sil_km = 0
            else:
                s_sil_km = silhouette_score(df_feat,  cluster_labels)

            save_metadata(canton,
                      experiment_name,
                      time_unit,
                      period,
                      [],
                      'kmodes',
                      'labels',
                      year,
                      -1,-1,
                      clusters,
                     -1,  -1,
                     s_sil_km,
                      [] )



