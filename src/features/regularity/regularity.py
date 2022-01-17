from etl.postgres_utils import get_select,insert_query,copy_df, \
                                insert_df, execute_query

from features.regularity.time_measures import PWD, PHD, PWD_biweekly
from features.regularity.profile import WS1, WS2, WS3
from features.regularity.frequency import FDH, FWH, FWD, FWH_biweekly

def get_data():
    query = """
        select distinct
            student,
            calendar_year,
            school_year
        from semantic.students_events
            left join features.school_years
                using(calendar_year, student)
        where school_year is not null
        """

    df = get_select(query)
    return df

def create_raw_table_regularity():
    query = """
    drop table if exists features.regularity;
    create table if not exists  features.regularity (
      student int,
      calendar_year int,
      school_year int,
      PWD decimal,
      PHD decimal,
      WS1 decimal,
      WS2 decimal,
      WS3 decimal,
      FDH decimal,
      FWH decimal,
      FWD decimal
    );
    """
    execute_query(query)



def regularity_weekly():
    df = get_data()

    df['PWD'] = [ PWD(student, year) for student, year in zip(df.student, df.school_year)]
    df['PHD'] = [ PHD(student, year) for student, year in zip(df.student, df.school_year)]

    df['WS1'] = [ WS1(student, year) for student, year in zip(df.student, df.school_year)]
    df['WS2'] = [ WS2(student, year) for student, year in zip(df.student, df.school_year)]
    df['WS3'] = [ WS3(student, year) for student, year in zip(df.student, df.school_year)]

    df['FDH'] = [ FDH(student, year) for student, year in zip(df.student, df.school_year)]
    df['FWH'] = [ FWH(student, year) for student, year in zip(df.student, df.school_year)]
    df['FWD'] = [ FWD(student, year) for student, year in zip(df.student, df.school_year)]

    df.fillna(0)
    create_raw_table_regularity()
    insert_df(df, 'features.regularity')


def create_raw_table_regularity_biweek():
    query = """
    drop table if exists features.biweek_regularity;
    create table if not exists  features.biweek_regularity (
      student int,
      calendar_year int,
      school_year int,
      PWD decimal,
      PHD decimal,
      WS1 decimal,
      WS2 decimal,
      WS3 decimal,
      FDH decimal,
      FWH decimal,
      FWD decimal
    );
    """
    execute_query(query)

def regularity_biweekly():
    df = get_data()
    df['PWD'] = [ PWD_biweekly(student, year, 'biweek_') for student, year in zip(df.student, df.school_year)]
    df['PHD'] = [ PHD(student, year) for student, year in zip(df.student, df.school_year)]

    df['WS1'] = [ WS1(student, year, 'biweek_') for student, year in zip(df.student, df.school_year)]
    df['WS2'] = [ WS2(student, year, 'biweek_') for student, year in zip(df.student, df.school_year)]
    df['WS3'] = [ WS3(student, year, 'biweek_') for student, year in zip(df.student, df.school_year)]

    df['FDH'] = [ FDH(student, year) for student, year in zip(df.student, df.school_year)]
    df['FWH'] = [ FWH_biweekly(student, year) for student, year in zip(df.student, df.school_year)]
    df['FWD'] = [ FWD(student, year) for student, year in zip(df.student, df.school_year)]

    df.fillna(0)
    create_raw_table_regularity_biweek()
    insert_df(df, 'features.biweek_regularity')


def create_raw_table_regularity_complete():
    query = """
    drop table if exists features.biweek_regularity_complete;
    create table if not exists  features.biweek_regularity_complete (
      student int,
      calendar_year int,
      school_year int,
      PWD decimal,
      PHD decimal,
      WS1 decimal,
      WS2 decimal,
      WS3 decimal,
      FDH decimal,
      FWH decimal,
      FWD decimal,

    PWD_biweek decimal,

    WS1_biweek decimal,
    WS2_biweek decimal,
    WS3_biweek decimal,

    FWH_biweek decimal
    );
    """
    execute_query(query)

def regularity_biweekly_complete():
    df = get_data()

    df['PWD'] = [ PWD(student, year) for student, year in zip(df.student, df.school_year)]
    df['PHD'] = [ PHD(student, year) for student, year in zip(df.student, df.school_year)]

    df['WS1'] = [ WS1(student, year) for student, year in zip(df.student, df.school_year)]
    df['WS2'] = [ WS2(student, year) for student, year in zip(df.student, df.school_year)]
    df['WS3'] = [ WS3(student, year) for student, year in zip(df.student, df.school_year)]

    df['FDH'] = [ FDH(student, year) for student, year in zip(df.student, df.school_year)]
    df['FWH'] = [ FWH(student, year) for student, year in zip(df.student, df.school_year)]
    df['FWD'] = [ FWD(student, year, '') for student, year in zip(df.student, df.school_year)]

    # biweekly
    df['PWD_biweek'] = [ PWD_biweekly(student, year, 'biweek_') for student, year in zip(df.student, df.school_year)]

    df['WS1_biweek'] = [ WS1(student, year, 'biweek_') for student, year in zip(df.student, df.school_year)]
    df['WS2_biweek'] = [ WS2(student, year, 'biweek_') for student, year in zip(df.student, df.school_year)]
    df['WS3_biweek'] = [ WS3(student, year, 'biweek_') for student, year in zip(df.student, df.school_year)]

    df['FWH_biweek'] = [ FWH_biweekly(student, year) for student, year in zip(df.student, df.school_year)]

    df.fillna(0)
    create_raw_table_regularity_complete()
    insert_df(df, 'features.biweek_regularity_complete')
