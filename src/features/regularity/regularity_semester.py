from etl.postgres_utils import get_select,insert_query,copy_df, \
                                insert_df, execute_query

from features.regularity.time_measures import PWD, PHD, PWD_biweekly
from features.regularity.profile import WS1, WS2, WS3
from features.regularity.frequency import FDH, FWH, FWD, FWH_biweekly

def get_data_semesters():
    query = """
        select distinct
            student,
            calendar_year,
            school_year,
        --    semester as semester_aux,
            school_year*2 + semester -1 as semester
        from semantic.students_events
            left join features.school_years
                using(calendar_year, student)
        where school_year is not null
        --fast track
        and student in (
        select student from cohort.paper_ticino
        union
        select student from cohort.paper_geneva
        )
        """

    df = get_select(query)
    return df

def create_raw_table():
    query = """
    drop table if exists features.biweek_semester_regularity;
    create table if not exists  features.biweek_semester_regularity (
      student int,
      calendar_year int,
      school_year int,
      semester_id int,
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


# all biweek, semester
def regularity_biweekly_semester():
    df = get_data_semesters()
    df['PWD'] = [ PWD_biweekly(student, year, 'biweek_semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]
    df['PHD'] = [ PHD(student, year, 'biweek_semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]

    df['WS1'] = [ WS1(student, year, 'biweek_semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]
    df['WS2'] = [ WS2(student, year, 'biweek_semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]
    df['WS3'] = [ WS3(student, year, 'biweek_semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]

    df['FDH'] = [ FDH(student, year, biweek = 'biweek_semester_') for student, year in zip(df.student, df.semester)]
    df['FWH'] = [ FWH_biweekly(student, year,  biweek ='biweek_semester_') for student, year in zip(df.student, df.semester)]
    df['FWD'] = [ FWD(student, year,  biweek = 'biweek_semester_') for student, year in zip(df.student, df.semester)]

    df.fillna(0)
    create_raw_table()
    insert_df(df, 'features.biweek_semester_regularity')



def create_raw_table_regularity_complete_semester():
    query = """
    drop table if exists features.biweek_regularity_complete_semester;
    create table if not exists  features.biweek_regularity_complete_semester (
      student int,
      calendar_year int,
      school_year int,
      semester_id int,
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

def regularity_biweekly_complete_semester():
    df = get_data_semesters()

    df['PWD'] = [ PWD_biweekly(student, year, 'semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]
    df['PHD'] = [ PHD(student, year, 'semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]

    df['WS1'] = [ WS1(student, year, 'semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]
    df['WS2'] = [ WS2(student, year, 'semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]
    df['WS3'] = [ WS3(student, year, 'semester_', timespan = 'semester') for student, year in zip(df.student, df.semester)]

    df['FDH'] = [ FDH(student, year,  'semester_') for student, year in zip(df.student, df.school_year)]
    df['FWH'] = [ FWH(student, year,  'semester_') for student, year in zip(df.student, df.school_year)]
    df['FWD'] = [ FWD(student, year,  'semester_') for student, year in zip(df.student, df.school_year)]


    # biweekly
    df['PWD_biweek'] = [ PWD_biweekly(student, year, 'biweek_semester_', timespan = 'semester') for student, year in zip(df.student, df.school_year)]

    df['WS1_biweek'] = [ WS1(student, year, 'biweek_semester_' ,timespan = 'semester') for student, year in zip(df.student, df.school_year)]
    df['WS2_biweek'] = [ WS2(student, year, 'biweek_semester_' ,timespan = 'semester') for student, year in zip(df.student, df.school_year)]
    df['WS3_biweek'] = [ WS3(student, year, 'biweek_semester_' ,timespan = 'semester') for student, year in zip(df.student, df.school_year)]

    df['FWH_biweek'] = [ FWH_biweekly(student, year, timespan = 'biweek_semester_') for student, year in zip(df.student, df.school_year)]



    df.fillna(0)
    create_raw_table_regularity_complete_semester()
    insert_df(df, 'features.biweek_regularity_complete_semester')
