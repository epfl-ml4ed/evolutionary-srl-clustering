import numpy as np
import pandas as pd
from scipy.special import entr
from scipy.spatial.distance import jensenshannon

from etl.postgres_utils import get_select,insert_query


def hours_per_day(student, year, biweek = '', timespan = 'school_year'):
    query = """
        select
        woy,
        dow,
        hours_dow,
        hours_bool
        from features.{biweek}regularity_hours_per_day
        where {timespan} = {year}
        and student = {student}
        """.format(year = year,
                  student = student,
                  biweek = biweek,
                  timespan = timespan)

    df = get_select(query)
    return df

def WS1(student, year=3, biweek='', timespan = 'school_year'):
    if len(biweek)== 2:
        week = 7
    else:
        week = 14

    hist = hours_per_day(student, year, biweek, timespan)
    Lw = len(hist)
    res = []
    for i in range(Lw):
        for j in range(i + 1, Lw):
            current_week = format_week_ws1(hist.loc[i],week)
            next_week = format_week_ws1(hist.loc[j],week)
            res.append(similarity_days(current_week, next_week))
    return np.mean(res)

def format_week_ws1(current_week,week):
    index = np.array(current_week.dow, dtype =np.int)
    values = np.array(current_week.hours_bool, dtype = np.int)

    processed_week = np.zeros(week)
    processed_week[index] = values
    return processed_week

def similarity_days(wi, wj):
    m1 = np.where(wi == 1)[0]
    m2 = np.where(wj == 1)[0]
    if len(m1) == 0 or len(m2) == 0:
        return 0
    return len(np.intersect1d(m1, m2)) / max(len(m1), len(m2))



def WS2(student, year=3, biweek='', timespan = 'school_year'):
    """
    The value of Sim2 is bounded in [0, 1] and high value of this measure
    reflects similar shapes of activity profiles inthe weeks of comparison
    """
    if len(biweek)== 2:
        week = 7
    else:
        week = 14

    profile = hours_per_day(student, year, biweek, timespan)
    Lw = len(profile)
    res = []
    for i in range(Lw):
        for j in range(i + 1, Lw):
            current_week = format_week_ws2(profile.loc[i], week)
            next_week = format_week_ws2(profile.loc[j], week)
            if not current_week.any() or not next_week.any(): continue
            res.append(1 - jensenshannon(current_week, next_week, 2.0))
    if len(res) == 0: return np.nan
    return np.mean(res)


def normalize(v):
    s = v.sum()
    if s == 0: return v
    return v / s


def format_week_ws2(current_week, week):
    index = np.array(current_week.dow, dtype =np.int)
    values = np.array(current_week.hours_dow, dtype = np.float)

    processed_week = np.zeros(week)
    processed_week[index] = normalize(values)
    return processed_week



def WS3(student, year=3,biweek='',timespan = 'school_year'):
    if len(biweek)== 2:
        week = 7
    else:
        week = 14

    profile = hours_per_day(student, year, biweek, timespan)
    Lw = len(profile)
    res = []
    for i in range(Lw):
        for j in range(i + 1, Lw):
            current_week = format_week_ws2(profile.loc[i],week)
            next_week = format_week_ws2(profile.loc[j],week)

            current_active = format_week_ws1(profile.loc[i],week)
            next_active = format_week_ws1(profile.loc[j],week)

            if not current_week.any() or not next_week.any(): continue
            res.append(chi2_divergence(current_week, next_week, current_active, next_active))
    if len(res) == 0: return np.nan
    return np.mean(res)

def chi2_divergence(p1, p2, a1, a2):
    a = p1 - p2
    b = p1 + p2
    frac = np.divide(a, b, out=np.zeros(a.shape, dtype=float), where=b!=0)
    m1 = np.where(a1 == 1)[0]
    m2 = np.where(a2 == 1)[0]
    union = np.union1d(m1, m2)
    if (len(union) == 0): return np.nan
    return 1 - (1 / len(union)) * np.sum(np.square(frac))
