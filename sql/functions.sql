

-- https://wiki.postgresql.org/wiki/Aggregate_Median-
create or replace function _final_median(numeric[])
   returns numeric as
$$
   select avg(val)
   from (
     select val
     from unnest($1) val
     order by 1
     limit  2 - mod(array_upper($1, 1), 2)
     offset ceil(array_upper($1, 1) / 2.0) - 1
   ) sub;
$$
language 'sql' immutable;

create or replace aggregate median(numeric) (
  sfunc=array_append,
  stype=numeric[],
  finalfunc=_final_median,
  initcond='{}'
);

-- https://stackoverflow.com/questions/18962605/apply-aggregate-functions-on-array-fields-in-postgres
create function array_avg(_data anyarray)
returns numeric
as
$$
    select avg(a)
    from unnest(_data) as a
$$ language sql;

create function array_median(_data anyarray)
returns numeric
as
$$
    select median(a)
    from unnest(_data) as a
$$ language sql;
