-- events:
--
create schema if not exists semantic;

drop type if exists semantic.student_event cascade;
create type semantic.student_event  as enum (
  'write new recipe',
  'edit recipe',

  'write new experience',
  'edit experience',

  'write new reflection', --from reflection log
  'edit reflection',

  'receive feedback',
  'request feedback',

  'log in',
  'be inactive',

  'add image'
);



drop table if exists semantic.students_events;
create table if not exists semantic.students_events(
  event_student serial,
  student int,
  event_type semantic.student_event,
  event_date timestamp,
  attributes jsonb,
  calendar_year int,
  semester int --0 August. 1 january
);

insert into semantic.students_events(student, event_type,
                                      event_date, attributes,
                                      calendar_year, semester)
  select
  id_user as student,
  event_type,
  event_date,
  attributes,
  case when event_date > '2013-08-01' and event_date <= '2014-08-01' then 1314
      when event_date > '2014-08-01' and event_date <= '2015-08-01' then 1415
      when event_date > '2015-08-01' and event_date <= '2016-08-01' then 1516
      when event_date > '2016-08-01' and event_date <= '2017-08-01' then 1617
      when event_date > '2017-08-01' and event_date <= '2018-08-01' then 1718
      when event_date > '2018-08-01' and event_date <= '2019-08-01' then 1819
      when event_date > '2019-08-01' and event_date <= '2020-08-01' then 1920
      when event_date > '2020-08-01' and event_date <= '2021-08-01' then 2021
      end as calendar_year,
  case when (extract(doy from event_date) >= 214 or extract(doy from event_date)  < 25)
  then 0 else 1 end as semester
  from
  (
    (
      select
      user_id as student,
      'write new recipe'::semantic.student_event as event_type,
      end_date as event_date,
      jsonb_build_object('activity', activity,
        'session_id', session_id,
        'title', title,
        'description',  description,
        'start_date', start_date,
        'end_date', end_date,
        'duration', duration,
        'au_evaluation',au_evaluation,
        'number_of_updates', number_of_updates,
        'image', image)::jsonb as attributes
      from
        clean.log_activities_duration
        left join clean.activities_users
          using(activity)
        where session_id = 1
        and activity_type ='{recipe}'
    )
    union
    (
      select
      user_id as student,
      'edit recipe'::semantic.student_event,
      end_date as event_date,
      jsonb_build_object('activity', activity,
        'session_id', session_id,
        'title', title,
        'description',  description,
        'start_date', start_date,
        'end_date', end_date,
        'duration', duration,
        'au_evaluation',au_evaluation,
        'number_of_updates', number_of_updates,
        'image', image)::jsonb
      from
        clean.log_activities_duration
        left join clean.activities_users
          using(activity)
        where session_id > 1
        and activity_type ='{recipe}'
    )
    union
    (
      select
      user_id as student,
      'write new experience'::semantic.student_event,
      end_date as event_date,
      jsonb_build_object('activity', activity,
        'session_id', session_id,
        'title', title,
        'description',  description,
        'start_date', start_date,
        'end_date', end_date,
        'duration', duration,
        'au_evaluation',au_evaluation,
        'number_of_updates', number_of_updates,
        'image', image)::jsonb
      from
        clean.log_activities_duration
        left join clean.activities_users
          using(activity)
        where session_id = 1
        and activity_type ='{experience}'
    )
    union
    (
      select
      user_id as student,
      'edit experience'::semantic.student_event,
      end_date as event_date,
      jsonb_build_object('activity', activity,
        'session_id', session_id,
        'title', title,
        'description',  description,
        'start_date', start_date,
        'end_date', end_date,
        'duration', duration,
        'au_evaluation',au_evaluation,
        'number_of_updates', number_of_updates,
        'image', image)::jsonb
      from
        clean.log_activities_duration
        left join clean.activities_users
          using(activity)
        where session_id > 1
        and activity_type ='{experience}'
    )
    union
    (
      select
      user_id::int,
      'log in'::semantic.student_event,
      event_date,
      jsonb_build_object('field','empty')::jsonb
      from clean.logins
    )
    union
    (
      select
      student,
      'write new reflection'::semantic.student_event,
      event_date,
      jsonb_build_object('duration', duration,
                        'activity',activity,
                        'session_id', session_id,
                        'number_updates',number_updates,
                        'operations', operations,
                        'insert_operation', insert_operation,
                        'fields', fields,
                        'evaluations', evaluations,
                        'improvement',improvement,
                        'critical_point',critical_point,
                        'skill_documentation',skill_documentation,
                        'learning_budget',learning_budget)::jsonb
      from clean.log_reflections_duration
      where session_id = 1
    )
    union
    (
      select
      student,
      'edit reflection'::semantic.student_event,
      event_date,
      jsonb_build_object('duration', duration,
                        'activity',activity,
                        'session_id', session_id,
                        'number_updates',number_updates,
                        'operations', operations,
                        'insert_operation', insert_operation,
                        'fields', fields,
                        'evaluations', evaluations,
                        'improvement',improvement,
                        'critical_point',critical_point,
                        'skill_documentation',skill_documentation,
                        'learning_budget',learning_budget)::jsonb
      from clean.log_reflections_duration
      where session_id > 1
    )
    union
    (
      select
        student,
        'receive feedback'::semantic.student_event,
        event_date as event_date,
        jsonb_build_object(
          'activity', activity,
          'end_date', end_date,
          'duration', duration,
          'teachers',teachers,
          'supervisors',supervisors,
          'sender', sender,
          'improvement',improvement,
          'critical_point',critical_point,
          'skill_documentation',skill_documentation,
          'learning_budget',learning_budget)::jsonb
        from clean.log_feedback
    )
    union
    (
      select
      sender::int as student,
      'request feedback'::semantic.student_event,
      end_date as event_date,
      jsonb_build_object(
        'start_date', start_date,
        'end_date', end_date,
        'duration', end_date-start_date,
        'number_updates',number_of_updates,
        'activity',activity,
        'recipient', recipient,
        'sender', sender,
        'url', url,
        'is_read', is_read)::jsonb
      from clean.feedback_request_duration
    )
    union
    (
      select distinct on (activity, image_name)
      user_id::int as student,
      'add image'::semantic.student_event,
      event_date,
      jsonb_build_object('activity', activity,
        'activity_type', activity_type,
        'title', title,
        'description',  description,
        'image_name', image_name,
        'au_evaluation',au_evaluation,
        'operation', operation
      )::jsonb
      from clean.log_activities
      left join clean.activities_users
        using(activity)
      where image = 1
      and user_id > 0
      and event_date::date != '2020-07-02'
    )
  ) as events
left join raw.users_no_dup as raw
  on events.student = raw.us_user
where events.student in (
  select distinct
    student::int
  from
    semantic.students
)
and event_date::date != '2020-07-02'

;


create index if not exists students_events_student on
semantic.students_events(student);
create index if not exists students_events_event_date on
semantic.students_events(event_date);


insert into semantic.students_events(student, event_type,
                                      event_date, attributes, calendar_year, semester)
    with events_durations as(
      select
      student,
      event_date,
      calendar_year,
      semester,
      event_type,
       (event_date - lag(event_date) over
              (order by student,event_date))::interval/60
                          as duration,
        extract(epoch from
        ((event_date - lag(event_date) over
              (order by student,event_date))::interval)
        )/60 as minutes,
        event_date - (event_date - lag(event_date) over
              (order by student,event_date))::interval
                          as start_date
     from semantic.students_events
    ),
    percentiles as(
      select
      student,
      event_type,
      percentile_disc(0.75) within group (order by minutes) +
      1.5*(
        percentile_disc(0.75) within group (order by minutes) -
        percentile_disc(0.25) within group (order by minutes)

      ) as maximum
      from events_durations
      group by student, event_type
    )
    select
    student,
    'be inactive'::semantic.student_event,
    (start_date + '3 seconds'::interval) as event_date,
    jsonb_build_object('event_type', event_type,
                        'maximum', maximum,
                        'end_date',event_date,
                        'start_date', start_date,
                        'duration', duration,
                        'minutes',minutes
    )::jsonb,
    calendar_year,
    semester
    from events_durations
    left join percentiles
    using(student, event_type)
  where (minutes > maximum and maximum > 20)
  or
  (event_type = 'log in' and minutes > 60)
  or
  (minutes > 120)
;


create index if not exists students_events_event_type on
semantic.students_events(event_type);
create index if not exists students_events_event_date on
semantic.students_events(event_date);
create index if not exists students_events_student on
semantic.students_events(student);
create index if not exists students_events_activity on
semantic.students_events((attributes->>'activity'));
create index if not exists students_events_duration on
semantic.students_events((attributes->>'duration'));
create index if not exists students_events_improvement on
semantic.students_events((attributes->>'improvement'));
create index if not exists students_events_learning_budget on
semantic.students_events((attributes->>'learning_budget'));
create index if not exists students_events_skill_documentation on
semantic.students_events((attributes->>'skill_documentation'));
create index if not exists students_events_critical_point on
semantic.students_events((attributes->>'critical_point'));

select count(*), event_type from semantic.students_events group by event_type ;