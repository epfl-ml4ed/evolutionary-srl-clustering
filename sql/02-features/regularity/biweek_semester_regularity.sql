


drop table if exists features.biweek_semester_regularity_dow;
create table if not exists  features.biweek_semester_regularity_dow as (
  with dow_raw as (
    select distinct
    student,
   (mod(extract(week from event_date)::int,2)::int)*7 + extract(dow from event_date)::int as dow,
    case when extract(doy from event_date)::int >= 213 --August 1rst
      then extract(doy from event_date)::int - 213
      else extract(doy from event_date)::int + 152 end as doy,
    calendar_year,
    semester,
    school_year
    from semantic.students_events
    left join features.school_years
      using(student, calendar_year)
    where event_type not in ('be inactive', 'receive feedback')
  ),
  normalize as(
    select
    student,
    calendar_year,
    semester,
    school_year,
    count(*) as total_events
    from dow_raw
    group by student,calendar_year, school_year, semester
  ),
  dow_aggregated as (
    select
    student,
    school_year,
    calendar_year,
    semester,
    count(*) as events_dow,
    dow
    from dow_raw
    group by dow, student, school_year,calendar_year, semester
    order by dow
  )
  select
    student,
    school_year,
    calendar_year,
    semester as semester_aux,
    school_year*2 + semester -1 as semester,
    dow,
    events_dow as absolute,
    events_dow::decimal/total_events as relative,
    total_events
    from dow_aggregated
    left join normalize
      using(student, school_year,calendar_year, semester)
);




--hours per day
drop table if exists features.biweek_semester_regularity_hours_per_day;
create table if not exists  features.biweek_semester_regularity_hours_per_day as (
  with events_durations as(
    select
    student,
    attributes->>'activity' as activity,
    event_date,
    event_type,
    calendar_year,
    semester,
    school_year,
    case when (attributes->>'duration' is null
       or extract(epoch from (attributes->>'duration')::interval)/60 < 2) then 2
       else extract(epoch from (attributes->>'duration')::interval)/60 end as duration,
    sum(bool(event_type ='be inactive')::int)
        over (partition by student order by student, event_date) as session,
    case when extract(doy from event_date)::int >= 213 -- August first
      then extract(doy from event_date)::int - 213
      else extract(doy from event_date)::int + 152 end as doy,
    case when extract(week from event_date)::int >= 31-- August first
      then extract(week from event_date)::int - 31
      else extract(week from event_date)::int + 21 end as woy,
   (mod(extract(week from event_date)::int,2)::int)*7 + extract(dow from event_date)::int as dow
   from semantic.students_events
   left join features.school_years
     using(student, calendar_year)
  ),
  sessions_durations as(
    select
    student,
    session,
    doy,
    dow,
    woy,
    calendar_year,
    semester,
    school_year,
    max(event_date) - min(event_date) as session_duration
    from events_durations
    where event_type != 'be inactive'
    group by student, session, doy, woy,calendar_year, semester, school_year, dow
  ),
  day_duration as (
    select
    student,
    doy,
    woy,
    dow,
    calendar_year, semester,
    school_year,
    sum(session_duration),
    extract(epoch from sum(session_duration))/3600 as hours,
    case when extract(epoch from sum(session_duration)) > 0 then 1 else 0 end as hours_bool
    from sessions_durations
    group by student, doy, woy, calendar_year, semester, school_year,dow
    order by calendar_year, semester, woy, dow
  )
  select
    student,
    calendar_year,
    semester as semester_aux,
    school_year*2 + semester -1 as semester,
    school_year,
    woy,
    array_agg(hours order by dow) as hours_dow,
    array_agg(ceil(hours) order by dow) as hours_dow_int,
    array_agg(ceil(hours_bool) order by dow) as hours_bool,
    array_agg(dow order by dow) as dow
    from day_duration
    group by student, calendar_year, semester, school_year, woy
);


drop table if exists features.biweek_semester_regularity_frequency_hod;
create table if not exists  features.biweek_semester_regularity_frequency_hod as (
  with hod_raw as (
    select distinct
    student,
    extract(hour from event_date) as hod,
    case when extract(doy from event_date)::int >= 213 --August 1rst
      then extract(doy from event_date)::int - 213
      else extract(doy from event_date)::int + 152 end as doy,
    calendar_year,
    semester,
    school_year,
    case when extract(week from event_date)::int >= 31-- August first
      then extract(week from event_date)::int - 31
      else extract(week from event_date)::int + 21 end as woy,
    (mod(extract(week from event_date)::int,2)::int)*7 + extract(dow from event_date)::int as dow
    from semantic.students_events
    left join features.school_years
      using(student, calendar_year)
  )
  select
    student,
    calendar_year,
    semester as semester_aux,
    school_year*2 + semester -1 as semester,
    school_year,
    count(distinct woy) as number_of_weeks,
    array_agg(hod+(doy*24) order by hod,doy) as hours_day
    from hod_raw
      group by student, calendar_year, semester, school_year

);





drop table if exists features.biweek_semester_regularity_hod;
create table if not exists  features.biweek_semester_regularity_hod as (
  with hod_raw as (
    select distinct
    student,
    extract(hour from event_date) as hod,
    case when extract(doy from event_date)::int >= 213 --August 1rst
      then extract(doy from event_date)::int - 213
      else extract(doy from event_date)::int + 152 end as doy,
    calendar_year, semester,
    school_year
    from semantic.students_events
    left join features.school_years
      using(student, calendar_year)
    where event_type not in ('be inactive', 'receive feedback')
  ),
  normalize as(
    select
    student,
    school_year,
    semester,
    count(*) as total_events
    from hod_raw
    group by student, school_year, semester
  ),
  hod_aggregated as (
    select
    student,
    school_year,
    semester,
    count(*) as events_hod,
    hod
    from hod_raw
    group by hod, student, school_year, semester
    order by hod
  )
  select
    student,
    school_year,
    semester as semester_aux,
    school_year*2 + semester -1 as semester,
    hod,
    events_hod as absolute,
    events_hod::decimal/total_events as relative
    from hod_aggregated
    left join normalize
      using(student, school_year, semester)
);



drop table if exists features.biweek_semester_regularity_frequency_days;
create table if not exists  features.biweek_semester_regularity_frequency_days as (
  with hod_raw as (
    select distinct
    student,
    case when extract(doy from event_date)::int >= 213 --August 1rst
      then extract(doy from event_date)::int - 213
      else extract(doy from event_date)::int + 152 end as doy,
    calendar_year,
    semester,
    school_year,
    case when extract(week from event_date)::int >= 31-- August first
      then extract(week from event_date)::int - 31
      else extract(week from event_date)::int + 21 end as woy
    from semantic.students_events
    left join features.school_years
      using(student, calendar_year)
  )
  -- correct semester
  --correct_semester as(  select case when semester = 1 then doy_aux   )
  select
    student,
    calendar_year,
    semester as semester_aux,
    school_year*2 + semester -1 as semester,
    school_year,
    count(distinct woy) as number_of_weeks,
    array_agg(doy order by doy) as days_year
    from hod_raw
      group by student, calendar_year,school_year, semester
);
