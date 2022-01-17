import numpy as np
import pandas as pd
import sys
sys.path.append('./../../src/')

from models.gridsearch.utils import reorder_labels, get_optimal_params
from models.clustering import spectral_clustering
from models.cluster_analysis import get_scores, get_group_composition
from models.results import save_metadata, save_labels, save_labels_metadata
from features.load_data import get_data
from project_settings import PREFIX
from etl.postgres_utils import get_select, execute_query
from datetime import datetime


def check_code():
    import scipy.io
    mat = scipy.io.loadmat('W.mat')
    W_temp = mat['W'][0]

    real_shape = W_temp[0].shape[0]
    number_of_matrix = W_temp.shape[0]
    W = np.zeros((number_of_matrix, real_shape,real_shape))

    for i in range(number_of_matrix):
        W[i] = W_temp[i]

def rename_table(experiment_name, new_experiment_name, year):
    query = """
    drop table if exists labels.{new_experiment_name};
    create table labels.{new_experiment_name} as (
    select
    student,
    calendar_year,
    {experiment_name}_{year} as {new_experiment_name}
    from labels.{experiment_name}_{year});
    """.format(experiment_name  = experiment_name,
    new_experiment_name = new_experiment_name, year = year)

    execute_query(query)


def batch_affect_spectral(W, num_clust, experiment_name, search = True, period = 'yearly'):
    # params
    num_iter = 3

    t_max = W.shape[0] #Number of years/semesters
    num_students = W.shape[1]
    alpha_est = np.zeros((num_iter, t_max))
    clu = np.zeros((t_max,num_students))
    W_bar = np.zeros(W.shape) # smoothen matrix

    for t, clusters in zip(range(t_max), num_clust): #for every year/semester
        # Gets data
        year = t+1
        new_experiment_name = "adaptive_{}_{}".format(experiment_name, year)
        time_unit, feature, canton = get_experiment_info(new_experiment_name)
        df= get_data(time_unit = time_unit, year = year, canton = canton, period = period)

        if t == 0: # for t = 0, we use the same clusters as for the static one
            W_bar_current = W[t]
            W_bar[t] = W_bar_current
            kmeans, proj_X, eigenvals_sorted = spectral_clustering(W_bar[t],clusters)
            cluster_labels = kmeans.labels_
            optimal_clusters = clusters
            rename_table(experiment_name, new_experiment_name, year)
        # Only do temporal smoothing if not the first time step
        else:
            W_prev = W_bar[t-1]
            W_current = W[t]

            clu_prev = clu[t-1]

            # TODO: function
            if search:
                gridsearch_clusters_adaptive(df, year, W_current, W_prev,new_experiment_name, num_iter)

            # Find optimal clusters
            optimal_clusters, _, _,_,_ = get_optimal_params(new_experiment_name, canton = canton)
            if(optimal_clusters!=clusters):
                print("Different number, static: {}, evol: {}".format(clusters, optimal_clusters))

            # RUN WITH OPTIMAL CLUSTERS -Estimate alpha
            kmeans, _, _ = spectral_clustering( W_current,optimal_clusters)
            cluster_labels = kmeans.labels_
            # Estimate alpha iteratively
            for i_iter in range(num_iter):
                alpha = estimate_alpha(W_current, W_prev, cluster_labels)
                W_bar_current = alpha*W_prev + (1-alpha)*W_current

                kmeans, proj_X, eigenvals_sorted = spectral_clustering(W_bar_current,optimal_clusters)
                cluster_labels = kmeans.labels_

                alpha_est[i_iter, t] = alpha #9 values
            W_bar[t] = W_bar_current

            # save labels and metadata, compute metrics
            cluster_labels = reorder_labels(pd.Series(cluster_labels), new_experiment_name, clu[t-1])

            df['label'] = cluster_labels
            save_labels(df, new_experiment_name, time_unit)

            save_labels_metadata(canton, new_experiment_name,datetime.now() , [feature],
                                'adaptive', str(list(alpha_est.flatten())),
                                 optimal_clusters, -1, -1, time_unit)

        # Match clusters (using greedy method if more than 4 clusters)
        clu[t] = cluster_labels#permute_clusters_greedy(cluster_labels, clu_prev)

    return clu, W_bar,alpha_est




def iterate_alpha(num_iter, W_current, W_prev, cluster_labels, curr_clust_num):
    alpha_est = []
    for i_iter in range(num_iter):
        alpha = estimate_alpha(W_current, W_prev, cluster_labels)
        W_bar_current = alpha*W_prev + (1-alpha)*W_current

        kmeans, proj_X, eigenvals_sorted = spectral_clustering(W_bar_current,curr_clust_num)
        cluster_labels = kmeans.labels_
        alpha_est.append(alpha)
    return  alpha_est, W_bar_current, kmeans, proj_X, eigenvals_sorted, cluster_labels



def gridsearch_clusters_adaptive(df, year, W_current, W_prev, new_experiment_name, num_iter = 3):
    time_unit, feature, canton = get_experiment_info(new_experiment_name)
    cluster_list = [2,3,4,5]
    for curr_clust_num in cluster_list:
        kmeans, _, _ = spectral_clustering( W_current,curr_clust_num)
        cluster_labels = kmeans.labels_

        # Estimate alpha iteratively
        alphas_list, W_bar_current, kmeans, proj_X, eigenvals_sorted, \
        cluster_labels = iterate_alpha(num_iter, W_current, W_prev, cluster_labels, curr_clust_num)

        # Finishes estimating alpha
        #Compute cluster info
        df['label'] = cluster_labels

        s_distortion, s_bic, s_sil_km = get_scores(kmeans, W_bar_current, proj_X)
        s_eigen = eigenvals_sorted[:10].tolist()

        save_metadata(
                 canton,
                  new_experiment_name,
                  time_unit,
                  "None",
                  [feature], #features
                  'adaptive',
                  "select_cluster", #metric
                  year,
                  -1, #gamma,
                  -1, #window,
                  curr_clust_num,
                  s_distortion, s_bic, s_sil_km,
                  s_eigen
                 )


def get_experiment_info(new_experiment_name):
    b = new_experiment_name.split('_')
    if b[-4]=='semester':
        time_unit = '_'.join(b[-4:-2])
        feature = '_'.join(b[2:-4])
        canton = b[-2]
    else:
        time_unit = b[-3]
        feature = '_'.join(b[2:-3])
        canton = b[-2]
    return time_unit, feature, canton


def estimate_alpha(W_curr, W_prev, clu):
    """
    % estimate_alpha(W_curr,W_prev,clu) returns an estimate of the optimal
    % forgetting factor alpha given the current proximity matrix W_curr, the
    % shrinkage estimate at the previous time step W_prev, and the cluster
    % membership vector clu.
    %
    % [alpha,mean_curr,var_curr] = estimate_alpha(W_curr,W_prev,clu) also
    % returns the matrices of sample means and sample variances used to
    % calculate the forgetting factor alpha.
    %
    % Author: Kevin Xu
    """
    mean_curr, var_curr = clu_sample_stats(W_curr, clu)
    num = np.sum(var_curr)
    den = num + np.sum((W_prev - mean_curr)**2)
    alpha = num / den
    return alpha



def clu_sample_stats(W_current, cluster_labels):
    """
    % [sm,sv] = clu_sample_stats(W,clu) calculates the sample means and
    % variances entries of W by sampling over the clusters specified by the
    % cluster vector clu. The sample variance is the unbiased (corrected) type.
    %
    % Author: Kevin Xu
    """

     # Matrix of sample means across all points in cluster
    sm = get_sample_means(W_current,cluster_labels)

    # Matrix of unbiased sample variances across all points in cluster
    sv = get_sample_variances(W_current, cluster_labels,  sm)
    return sm, sv


def get_sample_means(W_current, cluster_labels):
    n = W_current.shape[0] # Number of realizations
    clu_names = np.unique(cluster_labels)
    k = clu_names.shape[0] #number of clusters

    sm = np.zeros((n,n)) # Matrix of sample means across all points in cluster

    # First calculate sample means for all combinations (i,j) by iterating
    # across the clusters
    for c1 in range(k):
        c1_obj = np.array(np.where(cluster_labels == c1))[0]
        c1_length = c1_obj.shape[0]

        cluster_elems = W_current[c1_obj[:,None], c1_obj]
        # Calculate sample mean for i = j (diagonals)
        diag_sm = np.trace(cluster_elems)/c1_length # 1 (?)

        #Calculate sample mean for i ~= j (off-diagonals)
        offdiag_sm = (np.sum(cluster_elems) - diag_sm*c1_length) / (c1_length*(c1_length-1))

        sm_mat = offdiag_sm * np.ones((c1_length,c1_length))
        sm_mat[np.diag_indices_from(sm_mat)] = diag_sm

        sm[c1_obj[:,None], c1_obj] = sm_mat

        for c2 in range(c1+1,k):
            c2_obj = np.array(np.where(cluster_labels == c2))[0]
            c2_length = c2_obj.shape[0]

            cross_sm = np.sum(W_current[c1_obj[:,None], c2_obj]) / (c1_length*c2_length)
            sm[c1_obj[:,None], c2_obj] = cross_sm
            sm[c2_obj[:,None], c1_obj] = cross_sm
    return sm


def get_sample_variances(W_current, cluster_labels, sm):
    n = W_current.shape[0] # Number of realizations
    clu_names = np.unique(cluster_labels)
    k = clu_names.shape[0] #number of clusters
    sv = np.zeros((n,n)) # Matrix of sample means across all points in cluster

    # Now calculate sample variances
    for c1 in range(k):
        c1_obj = np.array(np.where(cluster_labels == c1))[0]
        c1_length = c1_obj.shape[0]

        cluster_elems = W_current[c1_obj[:,None], c1_obj]
        cluster_means = sm[c1_obj[:,None], c1_obj]

        # Calculate sample variance for i = j (diagonals)
        diag_sv = np.trace((cluster_elems - cluster_means)**2) / (c1_length - 1) #0 (?)

        # Calculate sample variance for i ~= j (off-diagonals)
        offdiag_sv = (np.sum((cluster_elems - cluster_means)**2) - diag_sv*(c1_length-1)) / (c1_length*(c1_length-1)-2)


        # If only one node is in this component, then W_currente cannot
        # calculate variance so set it to 0. If there are tW_currento nodes, W_currente can
        # calculate variance along the diagonal but not on the off-diagonal so
        # set the off-diagonal variance to 0.
        if c1_length == 1:
            sv[c1_obj[:,None], c1_obj] = 0
        elif c1_length == 2:
            node_1 = c1_obj[0]
            node_2 = c1_obj[1]
            sv[node_1,node_1] = diag_sv
            sv[node_2,node_2] = diag_sv
            sv[node_1,node_2] = 0
            sv[node_2,node_1] = 0
        else:
            sv_mat = offdiag_sv* np.ones((c1_length,c1_length))
            sv_mat[np.diag_indices_from(sv_mat)] = diag_sv
            sv[c1_obj[:,None], c1_obj] = sv_mat


        for c2 in range(c1+1,k):
            c2_obj = np.array(np.where(cluster_labels == c2))[0]
            c2_length = c2_obj.shape[0]

            cross_elems = W_current[c1_obj[:,None], c2_obj]
            cross_means = sm[c1_obj[:,None], c2_obj]

            # Calculate sample variance
            cross_sv = (np.sum((cross_elems - cross_means)**2))  / (c1_length*c2_length -1)


            if (c1_length*c2_length) == 1:
                sv[c1_obj[:,None], c2_obj] = 0
            else:
                sv[c1_obj[:,None], c2_obj] = cross_sv
                sv[c2_obj[:,None], c1_obj] = cross_sv
    return sv
