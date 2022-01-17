drop table if exists semantic.supervisors;
create table if not exists  semantic.supervisors as (
  select
    user_id as supervisor,
    language,
    canton,
    archived
    from clean.users_types
    left join clean.users
      using(user_id)
    where user_type =  'formatore'
);
