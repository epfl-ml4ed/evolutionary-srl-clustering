import numpy as np
import pandas as pd
from scipy.special import entr
from scipy.spatial.distance import jensenshannon

from etl.postgres_utils import get_select

HOUR_TO_SECOND = 60 * 60
DAY_TO_SECOND = 24 * HOUR_TO_SECOND
WEEK_TO_SECOND = 7 * DAY_TO_SECOND

def frequency_hod(student, year, biweek = '', timespan = 'school_year'):
    if 'semester' in biweek:
        timespan = 'semester'
    query = """
        select
            number_of_weeks,
            hours_day
        from features.{biweek}regularity_frequency_hod
        where {timespan} = {year}
        and student = {student}
        """.format(year = year,
                  student = student,
                  biweek = biweek,
                  timespan = timespan)

    df = get_select(query)
    return df


def fourier_transform(Xi, f, n):
    M = np.exp(-2j * np.pi * f * n)
    return np.real(np.dot(M, Xi))


def FDH(student, year, biweek = '', timespan = 'school_year'):
    df = frequency_hod(student, year, biweek,timespan)
    if len(df) > 0:
        if 'semester' in biweek:
            Lw = 27
        else:
            Lw = 53
        duration = Lw * WEEK_TO_SECOND // HOUR_TO_SECOND
        n = np.arange(duration)

        Xi = np.zeros(duration)
        mask = np.array(df.hours_day[0], dtype =np.int)
        Xi[mask]=1
        fourier = abs(fourier_transform(Xi, 1 / 24, n))
    else:
        fourier = 0

    return fourier


def FWH(student, year,biweek = '', timespan = 'school_year'):
    df = frequency_hod(student, year, biweek,timespan)
    if 'semester' in biweek:
        Lw = 27
    else:
        Lw = 53
    duration = Lw * WEEK_TO_SECOND // HOUR_TO_SECOND
    n = np.arange(duration)

    Xi = np.zeros(duration)
    mask = np.array(df.hours_day[0], dtype =np.int)
    Xi[mask]=1
    return abs(fourier_transform(Xi, 1 / 24*7, n))


def FWH_biweekly(student, year, biweek = '', timespan = 'school_year'):
    df = frequency_hod(student, year, biweek,timespan)
    if len(df) > 0:
        if 'semester' in biweek:
            Lw = 27
        else:
            Lw = 53
        duration = Lw * WEEK_TO_SECOND // HOUR_TO_SECOND
        n = np.arange(duration)

        Xi = np.zeros(duration)
        mask = np.array(df.hours_day[0], dtype =np.int)
        Xi[mask]=1
        fourier =  abs(fourier_transform(Xi, 1 / 24*14, n))
    else:
        fourier = 0
    return fourier


def frequency_days(student, year, biweek = '', timespan = 'school_year'):
    if 'semester' in biweek:
        timespan = 'semester'

    query = """
        select
            number_of_weeks,
            days_year
        from features.{biweek}regularity_frequency_days
        where {timespan} = {year}
        and student = {student}
        """.format(year = year,
                  student = student,
                  biweek = biweek,
                  timespan = timespan)

    df = get_select(query)
    return df


def FWD(student, year,  biweek , timespan = 'school_year'):
    df = frequency_days(student, year,  biweek,timespan)
    if len(df) > 0:
        if 'semester' in biweek:
            Lw = 27
        else:
            Lw = 53
        duration = Lw * WEEK_TO_SECOND // DAY_TO_SECOND
        n = np.arange(duration)

        Xi = np.zeros(duration)
        mask = np.array(df.days_year[0], dtype =np.int)
        Xi[mask]=1
        fourier = abs(fourier_transform(Xi, 1 / 7, n))
    else:
        fourier = 0
    return fourier
