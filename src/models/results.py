import sys
sys.path.append('./../src/')
from etl.postgres_utils import execute_query, copy_df, insert_query
from datetime import datetime


def save_kmodes(experiment_name, PREFIX, canton, time_unit, adaptive,
               year, feats,optimal_clusters, students_group,
               students_group_min, s_sil_km ,
               table_name = 'results.gridsearch_kmodes'):


    timestamp = datetime.now()

    values = (timestamp, experiment_name, PREFIX, canton, time_unit, adaptive,
                   year, feats,optimal_clusters, students_group,
                   students_group_min, s_sil_km)

    num_cols = len(values)
    query = """insert into {table_name}
    values (%s {new_cols}) """.format(table_name = table_name,
                                     new_cols = ', %s'*(num_cols -1) )

    insert_query(query, values)

def save_metadata(canton, experiment_name, time_unit, period,features, model,
                metric, school_year, gamma,\
                window, clusters, s_distortion, s_bic, s_sil_km, \
                s_eigen,  table_name ='results.gridsearch' ):

    timestamp = datetime.now()

    values = (timestamp, canton, experiment_name, time_unit, period, features, model, metric, school_year, gamma,
            window, clusters, s_distortion, s_bic, s_sil_km,
            s_eigen)

    num_cols = len(values)
    query = """insert into {table_name}
    values (%s {new_cols}) """.format(table_name = table_name,
                                     new_cols = ', %s'*(num_cols -1) )

    insert_query(query, values)


def save_labels_metadata(canton, experiment_name, exp_id, features, model, metric,
                         optimal_clusters, gamma, window, time_unit):

    timestamp = datetime.now()

    values = (timestamp, canton, experiment_name, exp_id, features, model, metric,
             optimal_clusters, gamma, window, time_unit)

    num_cols = len(values)
    table_name = 'results.labels_metadata'
    query = """insert into {table_name}
    values (%s {new_cols}) """.format(table_name = table_name,
                                     new_cols = ', %s'*(num_cols -1) )

    insert_query(query, values)


def save_group_metadata(canton, experiment_name, exp_id, features, metric,
                     optimal_clusters, gamma_list, window_list, time_unit, group):

    timestamp = datetime.now()

    values = (timestamp, canton, experiment_name, exp_id, features, metric,
            optimal_clusters, gamma_list, window_list, time_unit, group)

    num_cols = len(values)
    table_name = 'results.group_labels_metadata'
    query = """insert into {table_name}
    values (%s {new_cols}) """.format(table_name = table_name,
                                     new_cols = ', %s'*(num_cols -1) )

    insert_query(query, values)



def create_results_table(experiment_name,features,plot=False ):
    if plot:
        dtype = 'text'
    else:
        dtype = 'int'

    vars = " "
    for feature in features[2:]:
        new_var = ", {0} int".format(feature)
        vars = vars + new_var


    query = """
        drop table if exists labels.{experiment_name};
        create table  if not exists labels.{experiment_name}(
          student int,
          {experiment_name} {dtype}
          {vars}
        );
        """.format(experiment_name = experiment_name, dtype = dtype, vars = vars)
    execute_query(query)


def save_labels(df, experiment_name, time_unit = 'biweek', plot = False):

    if 'complete' in time_unit:
        extra = []
    else:
        extra = ['calendar_year']

    features = ['student','label'] + extra
    results = df[features]

    create_results_table(experiment_name, features, plot)
    table_name = "labels.{}".format(experiment_name)
    copy_df(results, table_name)
    print(str(table_name) + " created")
