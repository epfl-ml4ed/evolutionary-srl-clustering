drop table if exists semantic.students;
create table if not exists  semantic.students as (
  select
    user_id as student,
    case when language = 'ita' then 1 else 0 end as language_ita,
    case when canton = 'ti' then 'ticino' else 'geneva' end as canton,
    users.archived,
    number_of_classes,
    number_of_communications,
    total_reads,
    gender
  from clean.users_types as users_types
  left join clean.users
    using(user_id)
  left join clean.users_classes
    using(user_id)
  left join clean.communication_users
    using(user_id)
  left join clean.gender  as gender_me
    using(user_id)
  where users_types.user_type = 'studente'
  or user_id in
    (select distinct user_id from clean.all_grades)
  and user_id not in
  ('511', '627', '500842', '83','500982','501057',
    '415','508','614','500846','500898', '206',
  '501027','11', '14', '15','16', '17', '18', '621')
  --pietro and formattores and test accounts



);
