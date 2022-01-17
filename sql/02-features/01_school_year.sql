
drop table if exists features.school_years;
create table if not exists  features.school_years as (
with calendar_years as(
  select distinct
  student,
  calendar_year
  from semantic.students_events
  where event_type in (  'write new recipe',
                          'edit recipe',
                          'write new experience',
                          'edit experience',
                          'write new reflection',
                          'edit reflection')
),
school_years as (
    select
    student,
    calendar_year,
    row_number() over (partition by student
                      order by calendar_year)
                      as proxy_school_year
    from calendar_years
),
corrected_years as (
  select
    student,
    calendar_year,
    proxy_school_year,
    case when school_year is null then proxy_school_year
    else school_year end as school_year,
    case when school_year is null then 1 else 0 end as student_with_grades
  from school_years
  left join clean.all_grades
  using(student, calendar_year)
),
cantons as (
    select
    user_id as student,
    case when canton = 'ti' then 'ticino'
        when canton = 'ge' then 'geneva'
        else 'unknown' end as canton
    from clean.users
)
select * from corrected_years
 left join cantons
  using(student)
 where school_year <= 3
);


create index if not exists school_years_student on
features.school_years(student);
create index if not exists school_years_student on
features.school_years(calendar_year);
