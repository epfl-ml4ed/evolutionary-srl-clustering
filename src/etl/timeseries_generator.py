import numpy as np
import statistics

from pandas.core.common import flatten
from matplotlib import pyplot as plt
from pathlib import Path

import sys
sys.path.append('./../src/')
from etl.postgres_utils import get_generic_query, execute_query, get_select
from features.preprocess import format_year, get_max_duration




def create_times_series(period, time_unit):
    table_name = "time_series_complete"
    sql_query = get_generic_query(table_name)

    if period == "yearly":
        timespan = ", calendar_year, school_year"
    elif period == "semester":
        timespan = ", calendar_year, school_year, semester"
        time_unit = "semester_{0}".format(time_unit)
    elif period == "complete":
        timespan = " "
        time_unit = "complete_{0}".format(time_unit)

    formatted = sql_query.format(period =  period,
                                timespan = timespan,
                                time_unit = time_unit,
                                timespan_short = timespan[1:])

    execute_query(formatted)
    print("table created: features.dtw_per_{time_unit}".format(time_unit = time_unit))


def generate_combinations():
    periods = [ "semester", "yearly"] # [ "semester", "yearly", "complete"]
    time_units =  ['biweek', 'woy']#["semester", "biweek", "woy", "doy", "moy"]

    for period in periods:
        for time_unit in time_units:
            if period!=time_unit:
                create_times_series(period, time_unit)


def visualize_series():
    periods = ["semester","yearly", "complete"]
    time_units = ["semester", "biweek", "woy", "doy", "moy"]

    features = ['response_request_ratio', 'request_feedback_ratio',
                'average_duration_session', 'total_duration',
                'average_reflection', 'average_description',
                'writing_events', 'recipe_image_ratio',
                'recipe_experience_ratio', 'distinct_events',
                'count_just_log_in', 'ratio_just_log_in',
                'total_events', 'experience_events',
                'recipe_events', 'reflection_events',
                'new_events', 'editing_events']

    features = ['average_duration_session',
                 'total_duration', 'distinct_events',
                'count_just_log_in', 'ratio_just_log_in']
    for period in periods:
        for time_unit in time_units:
            if period!=time_unit:
                if period in ['complete', 'semester']:
                    time_unit = "{0}_{1}".format(period, time_unit)
                query =  """select * from features.dtw_per_{0}
                           where student in
                           (select distinct student
                           from cohort.report
                           where number_events > 70
                           group by student
                           having count(*)=3)
                           """.format(time_unit)
                df = get_select(query)

                max_duration = get_max_duration(time_unit)
                for feature in features:
                    print(time_unit, feature)
                    time = "{0}_{1}".format(feature, time_unit)
                    flat = np.array(list(flatten(df[feature])), dtype = np.float)
                    flat_nan = flat[~np.isnan(flat)]
                    upper = np.percentile(flat_nan, 90)

                    if upper == 0:
                        upper = 1

                    #save image
                    plot_dir = "./../results/timelines/{0}/{1}".format(feature, time_unit)
                    Path(plot_dir).mkdir(parents=True, exist_ok=True)

                    total_students = len(df)
                    for student in range(total_students):
                        student_data = df.iloc[student]
                        test = format_year(student_data, feature, time_unit)
                        if np.sum(test) > 0.01:
                            plt.bar(range(max_duration), test)
                            plt.xlim(0-0.5, max_duration+0.5)
                            plt.ylim(0, upper)
                            student_id = student_data.student

                            if period == 'yearly':
                                school_year = student_data.school_year
                                student_id = "sy_{0}_{1}".format(school_year, student_id)

                            plt.title("{0} | {1} | {2}".format(feature, time_unit, student_id))
                            img_dir = "{0}/{1}.png".format(plot_dir, student_id)
                            plt.savefig(img_dir)
                            plt.close()
