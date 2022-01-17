
CREATE OR REPLACE FUNCTION array_avg(double precision[])
RETURNS double precision AS $$
SELECT avg(v) FROM unnest($1) g(v)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION array_min(double precision[])
RETURNS double precision AS $$
SELECT min(v) FROM unnest($1) g(v)
$$ LANGUAGE sql;


create schema if not exists results;

  drop table if exists results.group_labels_metadata;
  create table if not exists results.group_labels_metadata (
    experiment_date timestamp,
    canton text,
    experiment_name text,
    experiment_id timestamp,
    features  text[],
    metric text,
    optimal_clusters int,
    gamma_list text[],
    window_list text[],
    time_unit text,
    group_name text
  );


  drop table if exists results.labels_metadata;
  create table if not exists results.labels_metadata (
    experiment_date timestamp,
    canton text,
    experiment_name text,
    experiment_id timestamp,
    features  text[],
    model text,
    metric text,
    optimal_clusters int,
    gamma float,
    window_size int,
    time_unit text
  );

  drop table if exists results.gridsearch_kmodes;
  create table if not exists results.gridsearch_kmodes (
    experiment_date timestamp,
    experiment_name text,
    prefix text,
    canton text,
    time_unit text,
    adaptive text,
    year int,
    feats text[],
    optimal_clusters int,
    students_group text[],
    students_group_min int,
    s_sil_km decimal,
    media1_list text[],
    media2_list text[],
    sum_medias_list text[],
    median1 decimal,
    median2 decimal,
    median_sum decimal,
    count_media1 decimal,
    count_media2 decimal,
    count_media_sum decimal
  );

    drop table if exists results.gridsearch_fg;
    create table if not exists results.gridsearch_fg (
      experiment_date timestamp,
      experiment_name text,
      time_unit text,
      time_period text,
      features  text[],
      model text,
      metric text,
      school_year int,
      gamma_list  text[],
      window_list  text[],
      clusters int,
      s_distortion float,
      s_bic float,
      s_sil_km float,
      s_eigen text[],
      students_group text[],
      median_groups int,
      media1_list text[],
      media2_list text[],
      final_list text[],
      sum_medias_list text[],
      sexratio_list text[]
    );


  drop table if exists results.gridsearch;
  create table if not exists results.gridsearch (
    experiment_date timestamp,
    canton text,
    experiment_name text,
    time_unit text,
    time_period text,
    features  text[],
    model text,
    metric text,
    school_year int,
    gamma  float,
    window_size  int,
    clusters int,
    s_distortion float,
    s_bic float,
    s_sil_km float,
    s_eigen text[],
    students_group text[],
    median_groups int,
    media1_list text[],
    media2_list text[],
    final_list text[],
    sum_medias_list text[],
    sexratio_list text[],
    median_sum_medias float,
    mean_sum_medias float
  );

