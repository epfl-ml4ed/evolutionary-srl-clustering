drop table if exists semantic.teachers;
create table if not exists  semantic.teachers as (
  select
    user_id as teacher,
    language,
    canton,
    archived
  from clean.users_types
  left join clean.users
    using(user_id)
  where user_type = 'docente'
);
