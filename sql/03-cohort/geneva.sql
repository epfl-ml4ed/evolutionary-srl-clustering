


drop table if exists cohort.report_geneva;
create table if not exists cohort.report_geneva as (
  with events_count  as(
  select
  student,
  calendar_year,
  count(*) filter (where event_type in
  ('write new recipe','write new experience','write new reflection',
  'edit recipe','edit experience','edit reflection','add image' ))
  as number_events
  from  semantic.students_events
  group by student,calendar_year
  ),
  cohort as (
  select
  student,
  school_year,
  calendar_year,
  number_events,
  gender,
  canton,
  language
  from features.school_years as feat
  left join clean.users as users
  on users.user_id = feat.student
  left join clean.gender as gender
    on gender.user_id = feat.student
  left join events_count
  using(student,calendar_year)
  where number_events >= 1
  and canton = 'ge'
  and users.user_id not in -- test users
    ('511', '627', '500842', '83','500982','501057',
      '415','508','614','500846','500898', '206',
    '501027','11', '14', '15','16', '17', '18', '621', '2')
)
select distinct on (student, school_year)
* from cohort
order by student, school_year, calendar_year desc
);

drop table if exists cohort.paper_geneva;
create table if not exists cohort.paper_geneva as (
  with semesters_ids as(
      select distinct
      student,
      (school_year*2) + semester - 1 as semester_id,
      school_year
      from features.dtw_per_semester_biweek
    ),
    first_filter as (
      select distinct student,
      count(distinct school_year) as count_sy
      from cohort.report_geneva
      group by student
      having count(distinct school_year)=3
    )
    select * from cohort.report_geneva
    where student  in
    (
      select student from first_filter
    )

);
