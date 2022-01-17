import pandas as pd
import numpy as np
import itertools as it
from scipy.stats import ranksums
from sklearn.metrics import silhouette_score

import sys
sys.path.append('./../src/')
from models.clustering import compute_bic


def get_scores(kmeans, kernel_matrix, proj_X):
    y_pred = kmeans.labels_

    s_distortion = kmeans.inertia_
    s_bic = compute_bic(kmeans, proj_X)
    if len(np.unique(y_pred)) == 1:
        print("Clustering error. Can't compute Silhouette correctly")
        s_sil_km = 0
    else:
        s_sil_km = silhouette_score(proj_X, y_pred)


    return s_distortion, s_bic, s_sil_km

