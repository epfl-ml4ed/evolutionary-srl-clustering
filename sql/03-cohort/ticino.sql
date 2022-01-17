

create schema if not exists cohort;

drop table if exists cohort.report_ticino;
create table if not exists cohort.report_ticino as (
  with events_count  as(
  select
  student,
  calendar_year,
  count(*) filter (where event_type in
  ('write new recipe','write new experience','write new reflection',
  'edit recipe','edit experience','edit reflection'))
  as number_events
  from  semantic.students_events
  group by student,calendar_year
  ),
  cohort as (
  select
  student,
  school_year,
  calendar_year,
  school_group,
  number_events,
  case when school_group is null then 'no_group' else school_group end
    as group_info,
  media1,
  media2,
  grade1,
  grade2,
  final_grade,
  gender,
  media1 + media2 as sum_medias
  from features.school_years as feat
  left join clean.users as users
  on users.user_id = feat.student
  left join clean.gender as gender
    on gender.user_id = feat.student
  left join events_count
  using(student,calendar_year)
  left join clean.all_grades
  using(student,calendar_year, school_year)
  where number_events >= 1
  and (school_group != 'block' and final_grade is not null)
  and users.user_id not in
    ('511', '627', '500842', '83','500982','501057',
      '415','508','614','500846','500898', '206',
    '501027','11', '14', '15','16', '17', '18', '621')
)
select distinct on (student, school_year)
* from cohort
order by student, school_year, calendar_year desc
);

drop table if exists cohort.paper_ticino;
create table if not exists cohort.paper_ticino as (
  with semesters_ids as(
      select
      (school_year*2) + semester - 1 as semester_id, *
      from features.dtw_per_semester_biweek
    ),
    first_filter as(
      select
      student
      from
      semesters_ids
      group by student
      having count(distinct school_year)=3
    )
    select * from cohort.report_ticino
    where student  in
    (
      select student from first_filter
    )
);
