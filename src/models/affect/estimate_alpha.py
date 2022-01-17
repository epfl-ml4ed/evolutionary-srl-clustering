# Generated with SMOP  0.41-beta
from libsmop import *
# test.m


@function
def estimate_alpha(W_curr=None,W_prev=None,clu=None,*args,**kwargs):
    varargin = estimate_alpha.varargin
    nargin = estimate_alpha.nargin

    # estimate_alpha(W_curr,W_prev,clu) returns an estimate of the optimal
# forgetting factor alpha given the current proximity matrix W_curr, the
# shrinkage estimate at the previous time step W_prev, and the cluster
# membership vector clu.

    # [alpha,mean_curr,var_curr] = estimate_alpha(W_curr,W_prev,clu) also
# returns the matrices of sample means and sample variances used to
# calculate the forgetting factor alpha.

    # Author: Kevin Xu

    mean_curr,var_curr=clu_sample_stats(W_curr,clu,nargout=2)
    num=sum(sum(var_curr))

    den=num + sum(sum((W_prev - mean_curr) ** 2))

    alpha=num / den

    assert_(logical_not(isnan(alpha)),'alpha is NaN. Numerator = %f. Denominator = %f',num,den)


# Generated with SMOP  0.41-beta
from libsmop import *
# test.m


@function
def clu_sample_stats(W=None,clu=None,*args,**kwargs):
    varargin = clu_sample_stats.varargin
    nargin = clu_sample_stats.nargin

    # [sm,sv] = clu_sample_stats(W,clu) calculates the sample means and
# variances entries of W by sampling over the clusters specified by the
# cluster vector clu. The sample variance is the unbiased (corrected) type.

    # Author: Kevin Xu

    n=size(W,1)
# test.m:8

    clu_names=unique(clu)
# test.m:9
    k=length(clu_names)
# test.m:10
    # Matrix of sample means across all points in cluster
    sm=zeros(n,n)
# test.m:13
    # Matrix of unbiased sample variances across all points in cluster
    sv=zeros(n,n)
# test.m:15
    # First calculate sample means for all combinations (i,j) by iterating
# across the clusters
    for c1 in arange(1,k).reshape(-1):
        c1_obj=find(clu == clu_names(c1))
# test.m:20
        c1_length=length(c1_obj)
# test.m:21
        diag_sm=trace(W(c1_obj,c1_obj)) / c1_length
# test.m:23
        offdiag_sm=(sum(sum(W(c1_obj,c1_obj))) - dot(diag_sm,c1_length)) / (dot(c1_length,(c1_length - 1)))
# test.m:25
        # 	for i = c1_obj'
# 		for j = c1_obj'
# 			if (i == j)
# 				sm(i,i) = diag_sm;
# 			else
# 				sm(i,j) = offdiag_sm;
# 			end
# 		end
# 	end
        sm_mat=dot(offdiag_sm,ones(c1_length))
# test.m:36
        sm_mat[diag(true(c1_length,1))]=diag_sm
# test.m:37
        sm[c1_obj,c1_obj]=sm_mat
# test.m:38
        for c2 in arange(c1 + 1,k).reshape(-1):
            c2_obj=find(clu == clu_names(c2))
# test.m:41
            c2_length=length(c2_obj)
# test.m:42
            cross_sm=sum(sum(W(c1_obj,c2_obj))) / (dot(c1_length,c2_length))
# test.m:44
            # 		for i = c1_obj'
# 			for j = c2_obj'
# 				sm(i,j) = cross_sm;
# 				sm(j,i) = cross_sm;
# 			end
# 		end
            sm[c1_obj,c2_obj]=cross_sm
# test.m:51
            sm[c2_obj,c1_obj]=cross_sm
# test.m:52

    # Now calculate sample variances
    for c1 in arange(1,k).reshape(-1):
        c1_obj=find(clu == clu_names(c1))
# test.m:58
        c1_length=length(c1_obj)
# test.m:59
        diag_sv=trace((W(c1_obj,c1_obj) - sm(c1_obj,c1_obj)) ** 2) / (c1_length - 1)
# test.m:61
        offdiag_sv=(sum(sum((W(c1_obj,c1_obj) - sm(c1_obj,c1_obj)) ** 2)) - dot(diag_sv,(c1_length - 1))) / (dot(c1_length,(c1_length - 1)) - 2)
# test.m:64
        # calculate variance so set it to 0. If there are two nodes, we can
	# calculate variance along the diagonal but not on the off-diagonal so
	# set the off-diagonal variance to 0.
        if c1_length == 1:
            sv[c1_obj,c1_obj]=0
# test.m:72
        else:
            if c1_length == 2:
                node_1=c1_obj(1)
# test.m:74
                node_2=c1_obj(2)
# test.m:75
                sv[node_1,node_1]=diag_sv
# test.m:76
                sv[node_2,node_2]=diag_sv
# test.m:77
                sv[node_1,node_2]=0
# test.m:78
                sv[node_2,node_1]=0
# test.m:79
            else:
                # 		for i = c1_obj'
# 			for j = c1_obj'
# 				if (i == j)
# 					sv(i,i) = diag_sv;
# 				else
# 					sv(i,j) = offdiag_sv;
# 				end
# 			end
# 		end
                sv_mat=dot(offdiag_sv,ones(c1_length))
# test.m:90
                sv_mat[diag(true(c1_length,1))]=diag_sv
# test.m:91
                sv[c1_obj,c1_obj]=sv_mat
# test.m:92
        for c2 in arange(c1 + 1,k).reshape(-1):
            c2_obj=find(clu == clu_names(c2))
# test.m:96
            c2_length=length(c2_obj)
# test.m:97
            cross_sv=sum(sum((W(c1_obj,c2_obj) - sm(c1_obj,c2_obj)) ** 2)) / (dot(c1_length,c2_length) - 1)
# test.m:99
            # 		for i = c1_obj'
# 			for j = c2_obj
# 				# If only one node is in each component then we cannot
# 				# calculate variance so set it to 0.
# 				if c1_length*c2_length == 1
# 					sv(i,j) = 0;
# 				else
# 					sv(i,j) = cross_sv;
# 					sv(j,i) = cross_sv;
# 				end
# 			end
# 		end
            if dot(c1_length,c2_length) == 1:
                sv[c1_obj,c2_obj]=0
# test.m:114
            else:
                sv[c1_obj,c2_obj]=cross_sv
# test.m:116
                sv[c2_obj,c1_obj]=cross_sv
# test.m:117
