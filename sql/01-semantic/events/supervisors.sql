-- events:
--
drop type if exists semantic.supervisor_event cascade;
create type semantic.supervisor_event  as enum (
  'receive feedback request',
  'send feedback response',
  'communicate',
  'add class file',
  'create new recipe',
  'create new experience',
  'log in'
);


drop table if exists semantic.supervisors_events;
create table if not exists semantic.supervisors_events(
  supervisor int,
  event_type semantic.supervisor_event,
  event_date date,
  attributes jsonb
);

insert into semantic.supervisors_events(supervisor, event_type, event_date, attributes)
  select * from
  (
    (
      select
      sender::int as supervisor,
      'send feedback response'::semantic.supervisor_event,
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
      sender::int as supervisor,
      'communicate'::semantic.supervisor_event,
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
      recipient::int as supervisor,
      'receive feedback request'::semantic.supervisor_event,
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
      user_id::int as supervisor,
      'add class file'::semantic.supervisor_event,
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
      user_id::int as student,
      'create new experience'::semantic.supervisor_event,
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
      user_id::int as student,
      'create new recipe'::semantic.supervisor_event,
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
      'log in'::semantic.supervisor_event,
      event_date,
      jsonb_build_object('field','empty')::jsonb
      from clean.logins
    )
  ) as events
where events.supervisor in (
  select distinct
  supervisor
  from
    semantic.supervisors
);


create index if not exists semantic_supervisors_events on
semantic.supervisors_events(supervisor);
create index if not exists semantic_supervisors_events on
semantic.supervisors_events(event_type);

select count(*), event_type from semantic.supervisors_events group by event_type ;
