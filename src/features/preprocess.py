
import numpy as np
from scipy.spatial import distance
from tslearn.metrics import cdist_dtw


def get_max_duration(time_unit):
    TIME_UNITS = {'semester': 2,
                  'moy': 12,
                  'biweek': 27,
                  'woy': 53,
                  'doy':366,

                  'complete_semester': 2*3,
                    'complete_moy': 12*3,

                    'complete_biweek': 27*3,
                    'complete_woy': 52*3,
                    'complete_doy':365*3,

                    'semester_moy': 12//2 ,
                    'semester_biweek': 27//2,
                    'semester_woy': 52//2 ,
                    'semester_doy': 365//2
                  }
    max_duration = TIME_UNITS[time_unit]
    return max_duration

features_to_normalize = [
 'total_duration_norm',
 'writing_events_norm',
]



def format_year(student, value, time_unit = 'biweek'):
    max_duration = get_max_duration(time_unit)
    processed_year = np.zeros(max_duration)
    time = "{0}_{1}".format(value, time_unit)

    if student[time] is not None:
        index = np.array(student[time], dtype =np.int)
        values = np.array(student[value], dtype = np.float)
        processed_year[index] = values

    return processed_year


def format_time_series(df, value, time_unit = 'biweek'):
    max_duration = get_max_duration(time_unit)
    number_students = len(df)
    all_students = np.zeros([number_students, max_duration], dtype=float)

    time = "{0}_{1}".format(value, time_unit)

    for i in range(number_students):
        if df.iloc[i][time] is not None:
            if df.iloc[i][time][-1] >= max_duration:
                print(time)
                print(df.iloc[i][time])
                print(df.iloc[i])
                print(df.student.iloc[i])
        all_students[i] = format_year(df.iloc[i], value, time_unit)
    return all_students


def format_feature(df, current_feature, metric, time_unit = 'biweek'):
    X_formatted = None
    if metric == 'euclidean':
        feature = df[current_feature].fillna(0.1)
        X_formatted = np.array(feature).reshape(-1,1)
    elif metric == 'dtw':
        X_formatted =  format_time_series(df, current_feature, time_unit)
    return X_formatted


def get_distance_matrix(X, metric='euclidean', window=2, feature_name = ''):
    """
    calculates distance matrix given a metric
    :param X: np.array with students' time-series
    :param metric: str distance metric to compute
    :param window: int for DTW
    :return: np.array with distance matrix
    """
    if metric == 'dtw':
        if feature_name in features_to_normalize :
            norms = np.linalg.norm(X, axis=1)
            norms[norms==0] = 1 # to avoid dividing my zero
            data_normalized = X / norms[:, np.newaxis]
        else:
            data_normalized = X
        distance_matrix = cdist_dtw(data_normalized,
                                    global_constraint='sakoe_chiba',
                                    sakoe_chiba_radius=window)
    else:
        distance_vector = distance.pdist(X, metric)
        distance_matrix = distance.squareform(distance_vector)
    return distance_matrix


def get_affinity_matrix(D, gamma=1):
    """
    calculates affinity matrix from distance matrix
    :param D: np.array distance matrix
    :param gamma: float coefficient for Gaussian Kernel
    :return:
    """
    S = np.exp(-gamma * D ** 2)
    return S


def get_feature_kernels(df, features,
                        metric = 'euclidean',
                        gamma = 1,
                        window = 2,
                        time_unit= 'biweek'):
    len_features = len(features)
    students = len(df)
    kernel_distances = np.zeros((len_features,students,students))
    kernel_affinities = np.zeros((len_features,students,students))

    for i in range(len_features):
        X_formatted =  format_feature(df, features[i], metric, time_unit)
        D = get_distance_matrix(X_formatted, metric, window,features[i])
        # to prevent overflow
        Dn = normalize(D)
        kernel_distances[i] = Dn
        kernel_affinities[i] = get_affinity_matrix(Dn, gamma)

    combined_affinity = np.sum(kernel_affinities, axis = 0)
    combined_distances = np.sum(kernel_distances, axis = 0)

    return combined_affinity, combined_distances


def normalize(distance_matrix):
    # robust standarizatoin
    flat_nan = distance_matrix[~np.isnan(distance_matrix)]
    median = np.median(flat_nan)
    q1 = np.percentile(flat_nan, 10)
    q3 = np.percentile(flat_nan, 90)
    IQR = q3 - q1
    if IQR==0:
        IQR = 1
    S =  (distance_matrix - median) / IQR

    # then normalization
    range_matrix = np.max(S) - np.min(S)
    normalized = (S - np.min(S)) / range_matrix
    return normalized
