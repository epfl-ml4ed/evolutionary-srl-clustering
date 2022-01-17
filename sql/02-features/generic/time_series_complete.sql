

drop table if exists features.counts_per_{time_unit};
create table if not exists  features.counts_per_{time_unit} as (

  with events_durations as(
     select
     student,
     attributes,
     attributes->>'activity' as activity,
     case when (attributes->>'image')::bool is true then 1
     else 0 end as image,
     case when cardinality(tags) > 1 then true else false end as bool_tags,
     case when cardinality(ingredients) > 1 then true else false end as bool_ingredients,

     event_date,
     event_type,
     calendar_year,
     school_year,
     sum(bool(event_type ='be inactive')::int)
      over (partition by student
            order by student, event_date) as session,

   case when (attributes->>'duration' is null
      or extract(epoch from (attributes->>'duration')::interval)/60 < 2) then 2
      when extract(epoch from (attributes->>'duration')::interval)/60 > 120 then 120
      else extract(epoch from (attributes->>'duration')::interval)/60 end as duration,

   case when extract(week from event_date)::int >= 31-- August first
     then extract(week from event_date)::int - 31
     else extract(week from event_date)::int + 21 end as woy,

   case when extract(year from event_date) in ('2012','2016', '2020')
   then true else false end as leap_year,

  extract(doy from event_date) as doy_aux,

   case when extract(month from event_date)::int >= 8 --August 1rst
     then extract(month from event_date)::int - 8
     else extract(month from event_date)::int + 4 end as moy_aux

    from semantic.students_events e
    left join features.school_years
      using(student, calendar_year)
    left join semantic.activities as a
      on (a.activity::text = e.attributes->>'activity')
   ),
   biweekly_aux as (
     select *,
     woy::int/2 as biweek,

     case when leap_year then
      (case when doy_aux >= 214 then doy_aux - 214 else doy_aux + 152 end)
    else
      (case when doy_aux>= 213 then doy_aux - 213  else doy_aux + 152 end)
     end as doy_leap,

     case when leap_year then
      (case when doy_aux >= 214 or doy_aux < 25 then 0 else 1 end)
    else
      (case when doy_aux>= 213 or doy_aux < 25 then 0 else 1 end)
     end as semester_aux

     from events_durations
   ),
   fix_periods as (
     select *,
     case when semester_aux = 1 and woy = 0 then 0  else semester_aux end as semester,
     case when semester_aux = 1 and woy = 0 and moy_aux = 11 then 0  else moy_aux end as moy,
     case when semester_aux = 1 and woy = 0 and doy_leap > 350 then 0  else doy_leap end as doy
     from biweekly_aux
   ),
   complete_periods as (
     select
     (school_year-1)*365 + doy as complete_doy,
     (school_year-1)*52 + woy as complete_woy,
     (school_year-1)*26 + biweek as complete_biweek,
     (school_year-1)*12 + moy as complete_moy,
     (school_year-1)*2 + semester as complete_semester,

     doy - (semester)*177 as semester_doy,
     woy - (semester)*25  as semester_woy,
     biweek - (semester)*12  as semester_biweek,
     moy - (semester)*5   as semester_moy,
     *
     from fix_periods
     where school_year >0
   ),
  session_stats as(
    select
     student,
     session,
     count(*) as session_events,
     max({time_unit}) as {time_unit},
     max(calendar_year) as calendar_year,
     max(school_year) as school_year,
     max(semester) as semester,
     case when (extract(epoch from (max(event_date) - min(event_date))::interval)/60) > 180
     then 180 else
     (extract(epoch from (max(event_date) - min(event_date))::interval)/60)
     end as session_duration
    -- sum(duration) as session_duration
    from complete_periods
    where event_type not in ('be inactive')
    group by student, session
  ),
  counts_per_session_biw as (
    select
    student {timespan}, {time_unit},
    count(*) filter (where session_events = 1)
      as count_just_log_in,
    (count(*) filter (where session_events = 1))::decimal/count(*)
      as ratio_just_log_in,
    sum(coalesce(session_duration,0))::decimal/count(*) as average_duration_session,
    sum(coalesce(session_duration,0))::decimal as total_duration
    from session_stats
    group by student {timespan}, {time_unit}
  ),
  counts_per_{time_unit} as (
    select

    (sum(bool_tags::int)  filter (where event_type in
                                ('write new recipe',
                                'write new experience',
                                'edit recipe',
                                'edit experience'))::decimal)/
       nullif((count(*) filter (where event_type in
                                ('write new recipe',
                                'write new experience',
                                'edit recipe',
                                'edit experience'))),0) as tags_ratio,

    (sum(bool_ingredients::int)  filter (where event_type in
                                ('write new recipe',
                                'edit recipe')) ::decimal)/
       nullif((count(*) filter (where event_type in
                                ('write new recipe',
                                'edit recipe'))),0) as ingredients_ratio,

    student {timespan}, {time_unit},
    count(*) filter (where event_type ='add image')  as image_events,

    count(*) filter (where event_type ='add image')::decimal/
    (case when (
      count(distinct activity) filter (where event_type in
        ('write new recipe','write new experience','edit recipe','edit experience'))) = 0
    then null else (
      count(distinct activity) filter (where event_type in
        ('write new recipe','write new experience','edit recipe','edit experience'))
    ) end)   as image_ratio,

    count(*) filter (where event_type in
    ('write new recipe','write new experience',
      'edit recipe','edit experience' ))
    as writing_events,

    count(*) filter (where event_type in
    ('write new recipe','write new experience','write new reflection',
      'edit recipe','edit experience','edit reflection' ))
    as writing_events_with_reflection,

    count(*)  as total_events,
    count(*) filter (where event_type in  ('write new experience','edit experience'))
    as experience_events,
    count(*) filter (where event_type in  ('write new recipe','edit recipe'))
    as recipe_events,
    count(*) filter (where event_type in  ('write new reflection','edit reflection'))
    as reflection_events,
    count(*) filter (where event_type in
    ('write new recipe','write new experience','write new reflection'))
    as new_events,
    count(*) filter (where event_type in
    ('edit recipe','edit experience','edit reflection' ))
    as editing_events,

    case when (count(*) filter (where event_type in ('edit recipe','edit experience','edit reflection',
                                'write new recipe','write new experience','write new reflection'))) < 1
    then null else
    (count(*) filter (where event_type  in ('edit recipe','edit experience','edit reflection')))::decimal /
    (count(*) filter (where event_type in ('edit recipe','edit experience','edit reflection',
                                          'write new recipe','write new experience','write new reflection')))
    end as editing_ratio,

    count(distinct event_type) filter (where event_type not in
      ('log in', 'be inactive'))  as distinct_events,
    sum(coalesce(image,0)) filter (where event_type in
      ('write new recipe','write new experience','edit recipe','edit experience')) ::decimal/
      count(*) filter (where event_type in
      ('write new recipe','write new experience','edit recipe','edit experience'))
      as recipe_image_ratio,
    sum(coalesce(duration,0)) filter (where event_type in ('write new recipe'))
    as new_recipe_duration,
    count(*) filter (where event_type in ('write new reflection','edit reflection' ))
    as count_reflection,
    count(*) filter (where event_type  in ('receive feedback', 'request feedback'))
    as count_feedback,
    count(*) filter (where event_type  in ('request feedback'))
    as count_feedback_requests,
    count(*) filter (where event_type  in ('receive feedback'))
    as count_feedback_responses,


    case when (count(*) filter (where event_type in ('write new recipe','write new experience'))) = 0
    then null else
    (count(*) filter (where event_type  in ('request feedback')))::decimal /
    (count(*) filter (where event_type in ('write new recipe','write new experience')))
    end as request_feedback_ratio,

    case when (count(*) filter (where event_type in ('request feedback'))) = 0
    then null else
    (count(*) filter (where event_type  in ('receive feedback')))::decimal /
    (count(*) filter (where event_type in ('request feedback')))
    end as response_request_ratio,

    sum(coalesce(length(attributes->>'improvement'),0) +
        coalesce(length(attributes->>'critical_point'),0) +
        coalesce(length(attributes->>'skill_documentation'),0) +
        coalesce(length(attributes->>'learning_budget'),0)
      ) as quality_reflection,

    case when (count(*) filter (where event_type in ('write new reflection'))) = 0
    then null else
    sum(coalesce(length(attributes->>'improvement'),0) +
        coalesce(length(attributes->>'critical_point'),0) +
        coalesce(length(attributes->>'skill_documentation'),0) +
        coalesce(length(attributes->>'learning_budget'),0)
      )::decimal/count(*) filter (where event_type in ('write new reflection'))
       end as average_reflection,

     case when (count(*) filter (where event_type in ('write new recipe'))) = 0
     then null else
     sum(coalesce(length(attributes->>'description'),0)
       )::decimal/count(*) filter (where event_type in ('write new recipe'))
        end as average_description,

    (count(*) filter (where event_type in ('edit recipe','write new recipe'))::decimal)/
    (case when (count(*) filter
    (where event_type in('edit experience','write new experience',
                          'edit recipe','write new recipe'))::decimal) = 0
    then null else (count(*) filter
    (where event_type in('edit experience','write new experience',
                          'edit recipe','write new recipe'))::decimal)  end)
    as recipe_experience_ratio,

    (count(*) filter (where event_type in ('edit experience','write new experience'))::decimal)/
    (case when (count(*) filter
    (where event_type in('edit experience','write new experience',
                          'edit recipe','write new recipe'))::decimal) = 0
    then null else (count(*) filter
    (where event_type in('edit experience','write new experience',
                          'edit recipe','write new recipe'))::decimal)  end)
    as experience_recipe_ratio
    from complete_periods
    group by student {timespan}, {time_unit}
  )
  select * from
  counts_per_{time_unit}
  full outer join counts_per_session_biw
  using(student {timespan}, {time_unit})

);

drop table if exists features.aggregated_per_{time_unit};
create table if not exists  features.aggregated_per_{time_unit} as (
  with first_step as (
    select
    *,
    case when (count_feedback_requests>0) then 1 else 0 end as bool_feed
    from features.counts_per_{time_unit}
  ),
  max_time as (
    select
    {timespan_short},
    max({time_unit}) as max_number
    from first_step
    group by {timespan_short}
  ),
  second_step as (
    select
    student,
    {timespan_short},
    sum(bool_feed) as sum_bool_feed,
    sum(count_feedback_requests) as sum_count_feedback_requests,
    sum(count_feedback_responses) as sum_count_feedback_responses,
    sum(count_feedback) as sum_count_feedback,

    sum(writing_events) as sum_writing_events,
    sum(total_duration) as sum_total_duration
    from first_step
    group by student, {timespan_short}
  ),
  third_step as (
    select
    student,
    {timespan_short},
    sum_bool_feed,
    sum_bool_feed::decimal/max_number as feedback_ratio_bool,

    sum_count_feedback,
    sum_count_feedback_requests,
    sum_count_feedback_responses,
    sum_writing_events,
    sum_total_duration,

    case when sum_count_feedback = 0 then 0 else log(sum_count_feedback) end as log_count_feedback,
    log(sum_count_feedback_requests+0.0000001)  as log_count_feedback_requests,
    log(sum_count_feedback_responses+0.0000001) as log_count_feedback_responses,
    log(sum_writing_events+0.0000001) as log_writing_events,
    log(sum_total_duration+0.0000001) as log_total_duration,

    sum_total_duration::decimal/max_number as average_total_duration,
    sum_writing_events::decimal/max_number as average_writing_events

    from second_step
    left join max_time using({timespan_short})
  )
  select * from third_step

);

drop table if exists features.dtw_per_{time_unit};
create table if not exists  features.dtw_per_{time_unit} as (
with editing_ratio_aggregate as (
select
  student {timespan},
  array_agg(editing_ratio order by {time_unit}) as editing_ratio,
  array_agg({time_unit} order by {time_unit}) as editing_ratio_{time_unit}
  from features.counts_per_{time_unit}
    where editing_ratio is not null
  group by student {timespan}
),
writing_events_aggregate as (
select
  student {timespan},
  array_agg(writing_events order by {time_unit}) as writing_events,
  array_agg({time_unit} order by {time_unit}) as writing_events_{time_unit}
  from features.counts_per_{time_unit}
    where writing_events is not null
  group by student {timespan}
),
total_events_aggregate as (
select
student {timespan},
array_agg(total_events order by {time_unit}) as total_events,
array_agg({time_unit} order by {time_unit}) as total_events_{time_unit}
from features.counts_per_{time_unit}
  where total_events is not null
group by student {timespan}
),

tags_ratio_aggregate as (
select
  student {timespan},
  array_agg(tags_ratio order by {time_unit}) as tags_ratio,
  array_agg({time_unit} order by {time_unit}) as tags_ratio_{time_unit}
  from features.counts_per_{time_unit}
    where tags_ratio is not null
  group by student {timespan}
),
ingredients_ratio_aggregate as (
select
  student {timespan},
  array_agg(ingredients_ratio order by {time_unit}) as ingredients_ratio,
  array_agg({time_unit} order by {time_unit}) as ingredients_ratio_{time_unit}
  from features.counts_per_{time_unit}
    where ingredients_ratio is not null
  group by student {timespan}
),

experience_events_aggregate as (
select
student {timespan},
array_agg(experience_events order by {time_unit}) as experience_events,
array_agg({time_unit} order by {time_unit}) as experience_events_{time_unit}
from features.counts_per_{time_unit}
  where experience_events is not null
group by student {timespan}
),
recipe_events_aggregate as (
select
student {timespan},
array_agg(recipe_events order by {time_unit}) as recipe_events,
array_agg({time_unit} order by {time_unit}) as recipe_events_{time_unit}
from features.counts_per_{time_unit}
  where recipe_events is not null
group by student {timespan}
),
reflection_events_aggregate as (
select
student {timespan},
array_agg(reflection_events order by {time_unit}) as reflection_events,
array_agg({time_unit} order by {time_unit}) as reflection_events_{time_unit}
from features.counts_per_{time_unit}
  where reflection_events is not null
group by student {timespan}
),
new_events_aggregate as (
select
student {timespan},
array_agg(new_events order by {time_unit}) as new_events,
array_agg({time_unit} order by {time_unit}) as new_events_{time_unit}
from features.counts_per_{time_unit}
  where new_events is not null
group by student {timespan}
),
editing_events_aggregate as (
select
student {timespan},
array_agg(editing_events order by {time_unit}) as editing_events,
array_agg({time_unit} order by {time_unit}) as editing_events_{time_unit}
from features.counts_per_{time_unit}
  where editing_events is not null
group by student {timespan}
),
reflection_aggregate as (
select
  student {timespan},
  array_agg(count_reflection order by {time_unit}) as count_reflection,
  array_agg({time_unit} order by {time_unit}) as count_reflection_{time_unit}
  from features.counts_per_{time_unit}
    where count_reflection is not null
  group by student {timespan}
),
average_reflection_aggregate as (
select
  student {timespan},
  array_agg(average_reflection order by {time_unit}) as average_reflection,
  array_agg({time_unit} order by {time_unit}) as average_reflection_{time_unit}
  from features.counts_per_{time_unit}
    where average_reflection is not null
  group by student {timespan}
),
average_description_aggregate as (
select
  student {timespan},
  array_agg(average_description order by {time_unit}) as average_description,
  array_agg({time_unit} order by {time_unit}) as average_description_{time_unit}
  from features.counts_per_{time_unit}
    where average_description is not null
  group by student {timespan}
),
feedback_aggregate as (
select
  student {timespan},
  array_agg(count_feedback order by {time_unit}) as count_feedback,
  array_agg({time_unit} order by {time_unit}) as count_feedback_{time_unit}
  from features.counts_per_{time_unit}
    where count_feedback  is not null
  group by student {timespan}
),
request_feedback_ratio_aggregate as (
select
  student {timespan},
  array_agg(request_feedback_ratio order by {time_unit}) as request_feedback_ratio,
  array_agg({time_unit} order by {time_unit}) as request_feedback_ratio_{time_unit}
  from features.counts_per_{time_unit}
    where request_feedback_ratio  is not null
  group by student {timespan}
),
response_request_ratio_aggregate as (
select
  student {timespan},
  array_agg(response_request_ratio order by {time_unit}) as response_request_ratio,
  array_agg({time_unit} order by {time_unit}) as response_request_ratio_{time_unit}
  from features.counts_per_{time_unit}
    where response_request_ratio  is not null
  group by student {timespan}
),
quality_aggregate as (
select
  student {timespan},
  array_agg(quality_reflection order by {time_unit}) as quality_reflection,
  array_agg({time_unit} order by {time_unit}) as quality_reflection_{time_unit}
  from features.counts_per_{time_unit}
    where quality_reflection is not null
  group by student {timespan}
),
recipe_aggregate as (
select
  student {timespan},
  array_agg(new_recipe_duration order by {time_unit}) as new_recipe_duration,
  array_agg({time_unit} order by {time_unit}) as new_recipe_duration_{time_unit}
  from features.counts_per_{time_unit}
    where new_recipe_duration is not null
  group by student {timespan}
),
recipe_image_ratio_aggregate as (
select
  student {timespan},
  array_agg(recipe_image_ratio order by {time_unit}) as recipe_image_ratio,
  array_agg({time_unit} order by {time_unit}) as recipe_image_ratio_{time_unit}
  from features.counts_per_{time_unit}
    where recipe_image_ratio is not null
  group by student {timespan}
),
recipe_experience_ratio_aggregate as (
select
  student {timespan},
  array_agg(recipe_experience_ratio - experience_recipe_ratio order by {time_unit}) as recipe_experience_ratio,
  array_agg({time_unit} order by {time_unit}) as recipe_experience_ratio_{time_unit}
  from features.counts_per_{time_unit}
    where recipe_experience_ratio is not null
  group by student {timespan}
),
distinct_events_aggregate as (
select
  student {timespan},
  array_agg(distinct_events order by {time_unit}) as distinct_events,
  array_agg({time_unit} order by {time_unit}) as distinct_events_{time_unit}
  from features.counts_per_{time_unit}
    where distinct_events is not null
  group by student {timespan}
),
count_just_log_in_aggregate as (
select
  student {timespan},
  array_agg(count_just_log_in order by {time_unit}) as count_just_log_in,
  array_agg({time_unit} order by {time_unit}) as count_just_log_in_{time_unit}
  from features.counts_per_{time_unit}
    where count_just_log_in is not null
  group by student {timespan}
),
ratio_just_log_in_aggregate as (
select
  student {timespan},
  array_agg(ratio_just_log_in order by {time_unit}) as ratio_just_log_in,
  array_agg({time_unit} order by {time_unit}) as ratio_just_log_in_{time_unit}
  from features.counts_per_{time_unit}
    where ratio_just_log_in is not null
  group by student {timespan}
),
average_duration_session_aggregate as (
select
  student {timespan},
  array_agg(average_duration_session order by {time_unit}) as average_duration_session,
  array_agg({time_unit} order by {time_unit}) as average_duration_session_{time_unit}
  from features.counts_per_{time_unit}
    where average_duration_session is not null
  group by student {timespan}
),
total_duration_aggregate as (
select
  student {timespan},
  array_agg(total_duration order by {time_unit}) as total_duration,
  array_agg({time_unit} order by {time_unit}) as total_duration_{time_unit}
  from features.counts_per_{time_unit}
    where total_duration is not null
  group by student {timespan}
),
image_ratio_aggregate as (
select
  student {timespan},
  array_agg(image_ratio order by {time_unit}) as image_ratio,
  array_agg({time_unit} order by {time_unit}) as image_ratio_{time_unit}
  from features.counts_per_{time_unit}
    where image_ratio is not null
  group by student {timespan}
),
image_events_aggregate as (
select
  student {timespan},
  array_agg(image_events order by {time_unit}) as image_events,
  array_agg({time_unit} order by {time_unit}) as image_events_{time_unit}
  from features.counts_per_{time_unit}
    where image_events is not null
  group by student {timespan}
),
aggregated_all as (
  select
  total_duration as total_duration_norm,
  total_duration_{time_unit} total_duration_norm_{time_unit},

  writing_events as writing_events_norm,
  writing_events_{time_unit} writing_events_norm_{time_unit},
  * from reflection_aggregate

  left join tags_ratio_aggregate
    using(student {timespan})
  left join ingredients_ratio_aggregate
    using(student {timespan})

  left join image_events_aggregate
    using(student {timespan})
  left join image_ratio_aggregate
    using(student {timespan})
  left join editing_ratio_aggregate
      using(student {timespan})

  left join response_request_ratio_aggregate
    using(student {timespan})
  left join request_feedback_ratio_aggregate
    using(student {timespan})
  left join average_duration_session_aggregate
    using(student {timespan})
  left join total_duration_aggregate
    using(student {timespan})
  left join average_reflection_aggregate
    using(student {timespan})
  left join average_description_aggregate
    using(student {timespan})
  left join writing_events_aggregate
    using(student {timespan})
  left join feedback_aggregate
    using(student {timespan})
  left join quality_aggregate
    using(student {timespan})
  left join recipe_aggregate
    using(student {timespan})
  left join recipe_image_ratio_aggregate
    using(student {timespan})
  left join recipe_experience_ratio_aggregate
    using(student {timespan})
  left join distinct_events_aggregate
    using(student {timespan})
  left join count_just_log_in_aggregate
    using(student {timespan})
  left join ratio_just_log_in_aggregate
    using(student {timespan})

  left join total_events_aggregate
    using(student {timespan})
  left join experience_events_aggregate
    using(student {timespan})
  left join recipe_events_aggregate
    using(student {timespan})
  left join reflection_events_aggregate
    using(student {timespan})
  left join new_events_aggregate
    using(student {timespan})
  left join editing_events_aggregate
    using(student {timespan})
  )
  select *
  from aggregated_all
  left join features.aggregated_per_{time_unit}
  using(student {timespan})
);
