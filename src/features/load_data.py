import sys
sys.path.append('./../src/')
from etl.postgres_utils import get_select
from project_settings import PREFIX, FEATURE_GROUPS, FEATURE_GROUPS_GENEVA, TEST_ID

def get_data(year = 3, metric = 'dtw', time_unit = None,
            adaptive = '', canton = 'ticino', period = 'yearly'):
    data = None
    if time_unit and metric !='labels':
        data =  get_time_unit_data(time_unit, year, canton, period)
    elif metric == 'labels':
        data = get_feature_labels(adaptive, time_unit, year, canton)
    return data

def get_time_unit_data(time_unit, year, canton, period):
    if 'complete' in time_unit:
        query = """
            with grades as (
                select
                student,
                avg(media1) as media1,
                avg(media2) as media2,
                avg(grade1) as grade1,
                avg(grade2) as grade2,
                avg(final_grade) as final_grade
                from clean.all_grades
                group by student
            )
            select distinct on (student, school_year) *,
            media1 + media2 as sum_medias
            from features.dtw_per_{time_unit} as feat
            left join clean.users as users
    			on users.user_id = feat.student
            left join clean.gender as gender
                on gender.user_id = feat.student
    		left join grades
    			using(student)
            where student in
                (select student from cohort.paper_{canton})
            order by student, school_year, calendar_year desc
            """.format(time_unit = time_unit, canton = canton )
    elif 'semester' in time_unit:

        query = """
        with semesters_ids as(
            select
            (school_year::int*2) + semester::int - 1 as semester_id, *
            from features.dtw_per_{time_unit}
         ),
        cohort as(
            select distinct on (student, semester_id) *,
            media1 + media2 as sum_medias
            from semesters_ids as feat
            left join features.school_years
                using(student,calendar_year, school_year)
            left join clean.gender as gender
                on gender.user_id = feat.student
    		left join clean.all_grades
    			using(student,calendar_year, school_year)
            where  student in
            (select student from cohort.paper_{canton})
            order by student, semester_id, calendar_year desc
        )
        select *
        from cohort
        -- this is not a problem, we just use the features that don't have the _biweek
        left join features.biweek_regularity_complete_semester
            using(student, semester_id, calendar_year)
        left join features.out_of_group_{time_unit}_{canton}
            using(student, semester_id, calendar_year)
        where semester_id = {year}
        order by student
            """.format(time_unit = time_unit, year = year,
                        canton = canton, period = period)
    else: # yearly
        query = """
            select distinct on (student, school_year) *,
            media1 + media2 as sum_medias
            from features.school_years
            left join features.dtw_per_{time_unit} as feat
                using(student,calendar_year, school_year)
            left join clean.gender as gender
                on gender.user_id = feat.student
            -- left join features.biweek_regularity
            -- ===== CHANGE HERE !!!! ======
            left join features.biweek_regularity_complete
                using(student,calendar_year, school_year)
            left join features.out_of_group_{time_unit}_{period}_{canton}
                using(student,calendar_year, school_year)
    		left join clean.all_grades
    			using(student,calendar_year, school_year)
            where student in
                (select student from cohort.paper_{canton})
            and school_year = {year}
            order by student
            """.format(time_unit = time_unit, year = year,
            canton = canton, period = period )
    df = get_select(query)
    return df



def get_feature_labels(adaptive, time_unit, year, canton = 'ticino'):
    if 'semester' in time_unit:
        school_period = 'semester_id'
    else:
        school_period = 'school_year'

    if canton == 'ticino':
        feature_groups = list(FEATURE_GROUPS.keys())

    else:
        feature_groups = list(FEATURE_GROUPS_GENEVA.keys())

    part1 = """
    with semesters as (
        select distinct
        calendar_year,
        semester
        from semantic.students_events
    ),
    extended as (
        select
        (school_year*2) + semester -1 as semester_id,
        * from cohort.paper_{canton}
        left join semesters
            using(calendar_year)
    ),
    cohort as (
        select distinct on (student, {school_period})
        * from extended
        where {school_period} = {year}
        order by student, {school_period}, calendar_year
    )
    select *
    """

    part2 = " "
    for feature in feature_groups:
        new_feat =  """ , {adaptive}{prefix}_%s_{time_unit}_{canton}_{year} as %s
        """ % (feature, feature)
        part2 = part2 + new_feat


    part3 = "from cohort  "
    for feature in feature_groups:
        new_feat =  """
        left join labels.{adaptive}{prefix}_%s_{time_unit}_{canton}_{year}
            using(student,calendar_year)
        """ % (feature)
        part3 = part3 + new_feat

    part4 = """ where student in (select student from cohort.paper_{canton})"""

    query = part1 + part2 + part3 + part4

    query = query.format(prefix = TEST_ID, adaptive = adaptive,
              time_unit = time_unit, year = year,
              school_period = school_period, canton = canton)

    df = get_select(query)
    return df




def get_data_out_of_group(calendar_year, semester, subject = 'ingredients',
                            canton = 'ticino', period = 'yearly'):
    if period  == 'semester':
        school_period = 'semester_id'
    else:
        school_period = 'school_year'

    part1 = """
    with semesters_list  as (
        select distinct
        student,
        calendar_year,
        semester,
        school_year*2 + semester - 1 as semester_id,
        school_year,
        canton
        from semantic.students_events
        left join features.school_years
            using (student, calendar_year)
    ),
    students_list as ( --so that we can use semester_id
        select distinct on(student, calendar_year, semester)*
        from semesters_list
        where {school_period} = '{semester}'
        and canton = '{canton}'
        and calendar_year = '{calendar_year}'
    ),
    events_students as(
      select
      attributes->>'activity' as activity,
      student,
      calendar_year,
      event_student,
      event_type,
      event_date,
      attributes,
      semester_id,
      school_year,
      canton,
      semester
      from semantic.students_events as events -- keeps only the ones we are interested
      inner join students_list using (student, calendar_year, semester)
      where event_type in ('write new recipe', 'edit recipe')
    ),
  joined_table as (
     select
         e.activity::int as activity,
         student,
         semester,
         semester_id,
         school_year,
         calendar_year,
         event_student,
         canton,
         tags,
         (attributes->'title')::jsonb as title,
         (attributes->'description')::jsonb as description,
         attributes->>'observation' as observation,
         ingredients,
         event_date
     from events_students e
     left join semantic.activities as a
     on e.activity = a.activity::text
    ),
    """

    if subject in ['ingredients', 'tags']:
        part2= """
        filtered_table as (
        select cardinality({subject}) as num_objects,
        * from joined_table
        where cardinality({subject}) >1
        )
        select distinct on (activity)
        * from filtered_table
        order by activity, event_date desc
        """
    elif subject in ['title', 'description']:
        part2 = """
          filtered_table as (
          select
            {subject}->0 as {subject}_text,
            length(({subject}->0)::text) as num_objects,
            * from joined_table
            where length(({subject}->0)::text) >=3
            and student in
                (select student from cohort.paper_{canton})
          )
          select distinct on (activity)
          * from filtered_table
          order by activity, event_date desc
        """
    else: #all
        part2 = """
          filtered_table as (
          select
            * from joined_table
            where student in
                (select student from cohort.paper_{canton})
          )
          select distinct on (activity)
          * from filtered_table
          order by activity, event_date desc
        """

    together = part1 + part2
    query = together.format(
    calendar_year = calendar_year,
    semester = semester,
    canton = canton,
    subject = subject,
    school_period = school_period
    )

    df = get_select(query)
    return df
