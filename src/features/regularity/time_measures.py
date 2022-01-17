import numpy as np
import pandas as pd
from scipy.special import entr
from scipy.spatial.distance import jensenshannon

from etl.postgres_utils import get_select,insert_query

def weekly_frequencies(student, year, biweek = '', timespan = 'school_year'):
    query = """
            with all_days as ( -- to have all days of the week
                select distinct
                dow from
                features.{biweek}regularity_dow
            ),
            relative_dow as (
                select *
                from features.{biweek}regularity_dow
                where {timespan} = {year}
                and student = {student}
            )
            select
            dow::int as dow,
            case when absolute is null then 0 else absolute
                end as absolute,
            case when relative is null then 0 else relative
                end as relative
            from all_days
            left join relative_dow
                using(dow)
            order by dow
            """.format(year = year,
                       student = student,
                       biweek = biweek,
                       timespan = timespan)

    df = get_select(query)
    return df.absolute.tolist(), df.relative.tolist()


def hourly_frequencies(student, year, biweek = '',timespan = 'school_year'):
    query = """
            with all_hours as ( -- to have all hours of the day
                select distinct
                hod from
                features.regularity_hod
            ),
            relative_hod as (
                select *
                from features.{biweek}regularity_hod
                where {timespan} = {year}
                and student = {student}
            )
            select
            hod::int as hod,
            case when absolute is null then 0 else absolute
                end as absolute,
            case when relative is null then 0 else relative
                end as relative
            from all_hours
            left join relative_hod
                using(hod)
            order by hod
            """.format(biweek = biweek,
                        year = year,
                       student = student,
                       timespan = timespan)

    df = get_select(query)
    return df.absolute.tolist(), df.relative.tolist()


def PWD(student, year=3, biweek = '',timespan = 'school_year'):
    activity, normalized_activity = weekly_frequencies(student, year, biweek, timespan)
    entropy = entr(normalized_activity).sum()
    return (np.log2(7) - entropy) * np.max(activity)


def PHD(student, year=3 ,biweek = '',timespan = 'school_year'):
    activity, normalized_activity = hourly_frequencies(student, year, biweek, timespan)
    entropy = entr(normalized_activity).sum()
    return (np.log2(24) - entropy) * np.max(activity)

def PWD_biweekly(student, year=3, biweek = 'biweek_',timespan = 'school_year'):
    activity, normalized_activity = weekly_frequencies(student, year, biweek, timespan)
    entropy = entr(normalized_activity).sum()
    return (np.log2(14) - entropy) * np.max(activity)
