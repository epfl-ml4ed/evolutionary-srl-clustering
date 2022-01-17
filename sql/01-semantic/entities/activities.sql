drop table if exists  semantic.activities;
create table if not exists semantic.activities as (
  select
  *
 from clean.activities_users
  left join clean.activities_responsables
    using(activity)
  left join clean.activities_companies
    using(activity)
  left join clean.activities_versions
    using(activity)
  left join clean.activities_ingredients
    using(activity)
  left join clean.activities_files
    using(activity)
  left join clean.activities_tags
    using(activity)
);
