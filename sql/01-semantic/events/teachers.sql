-- events:
--
drop type if exists semantic.teacher_event cascade;
create type semantic.teacher_event  as enum (
  'receive feedback request',
  'send feedback response',
  'communicate',
  'add class file', -- classFile added
  'create new recipe',
  'create new experience',
  'log in'
);


drop table if exists semantic.teachers_events;
create table if not exists semantic.teachers_events(
  teacher int,
  event_type semantic.teacher_event,
  event_date date,
  attributes jsonb
);

insert into semantic.teachers_events(teacher, event_type, event_date, attributes)
  select * from
  (
    (
      select
      sender::int as teacher,
      'send feedback response'::semantic.teacher_event,
      event_date,
      jsonb_build_object('notification', notification,
                  'recipient', recipient,
                  'sender', sender,
                  'url', url,
                  'is_read', is_read,
                  'activity', n.activity,
                  'description', description)::jsonb
      from clean.notifications as n
      left join clean.activities_users as au
        on (n.activity::text = au.activity::text
        and n.recipient = au.user_id)
      left join clean.activities_responsables as ar
        on (n.activity::text = ar.activity::text
        and n.sender = ar.responsable)
      where type = 'feedback response'
    )
    union
    (
      select
      sender::int as teacher,
      'communicate'::semantic.teacher_event,
      event_date,
      jsonb_build_object('notification', notification,
                  'recipient', recipient,
                  'sender', sender,
                  'url', url,
                  'is_read', is_read,
                  'activity', n.activity,
                  'description', description)::jsonb
      from clean.notifications as n
      left join clean.activities_users as au
        on (n.activity::text = au.activity::text
        and n.recipient = au.user_id)
      left join clean.activities_responsables as ar
        on (n.activity::text = ar.activity::text
        and n.sender = ar.responsable)
      where type = 'communication'
    )
    union
    (
      select
      recipient::int as teacher,
      'receive feedback request'::semantic.teacher_event,
      event_date,
      jsonb_build_object('notification', notification,
                  'recipient', recipient,
                  'sender', sender,
                  'url', url,
                  'is_read', is_read,
                  'activity', n.activity,
                  'description', description)::jsonb
      from clean.notifications as n
      left join clean.activities_users as au
        on (n.activity::text = au.activity::text
        and n.recipient = au.user_id)
      left join clean.activities_responsables as ar
        on (n.activity::text = ar.activity::text
        and n.sender = ar.responsable)
     where type in ('send feedback request','feedback request')
    )
    union
    (
      select
      user_id::int as teacher,
      'add class file'::semantic.teacher_event,
      event_date,
      jsonb_build_object('notification', notification,
                  'recipient', recipient,
                  'sender', sender,
                  'url', url,
                  'is_read', is_read,
                  'activity', n.activity,
                  'description', description)::jsonb
      from clean.notifications as n
      left join clean.activities_users as au
        on (n.activity::text = au.activity::text
        and n.recipient = au.user_id)
      left join clean.activities_responsables as ar
        on (n.activity::text = ar.activity::text
        and n.sender = ar.responsable)
      where type = 'classfile added'
    )
    union
    (
      select
      user_id::int,
      'create new experience'::semantic.teacher_event,
      event_date,
      jsonb_build_object('notification', notification,
                  'recipient', recipient,
                  'sender', sender,
                  'url', url,
                  'is_read', is_read,
                  'activity', n.activity,
                  'description', description)::jsonb
      from clean.notifications as n
      left join clean.activities_users as au
        on (n.activity::text = au.activity::text
        and n.recipient = au.user_id)
      left join clean.activities_responsables as ar
        on (n.activity::text = ar.activity::text
        and n.sender = ar.responsable)
      where type = 'new experience'
    )
    union
    (
      select
      user_id::int,
      'create new recipe'::semantic.teacher_event,
      event_date,
      jsonb_build_object('notification', notification,
                  'recipient', recipient,
                  'sender', sender,
                  'url', url,
                  'is_read', is_read,
                  'activity', n.activity,
                  'description', description)::jsonb
      from clean.notifications as n
      left join clean.activities_users as au
        on (n.activity::text = au.activity::text
        and n.recipient = au.user_id)
      left join clean.activities_responsables as ar
        on (n.activity::text = ar.activity::text
        and n.sender = ar.responsable)
      where type = 'new recipe'
    )
    union
    (
      select
      user_id::int,
      'log in'::semantic.teacher_event,
      event_date,
      jsonb_build_object('field','empty')::jsonb
      from clean.logins
    )
  ) as events
where events.teacher in (
  select distinct
  teacher
  from
    semantic.teachers
);


create index if not exists semantic_teachers_events on
semantic.teachers_events(teacher);
create index if not exists semantic_teachers_events on
semantic.teachers_events(event_type);

select count(*), event_type from semantic.teachers_events group by event_type ;
