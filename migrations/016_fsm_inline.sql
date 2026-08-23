create function fl.legal_transition(from_status job_status, to_status job_status)
returns boolean language sql immutable as $$
  select (from_status, to_status) in (
    ('scheduled',  'queued'),      ('scheduled',  'cancelled'),
    ('queued',     'claimed'),     ('queued',     'cancelled'),
    ('claimed',    'running'),     ('claimed',    'queued'),
    ('claimed',    'retry_wait'),  ('claimed',    'cancelled'),
    ('claimed',    'dead_letter'),
    ('running',    'completed'),   ('running',    'retry_wait'),
    ('running',    'dead_letter'), ('running',    'cancelled'),
    ('running',    'queued'),      ('running',    'scheduled'),
    ('retry_wait', 'queued'),      ('retry_wait', 'cancelled'),
    ('retry_wait', 'dead_letter'),
    ('dead_letter','queued')
  )
$$;

do $$
declare extra int; missing int;
begin
  select count(*) into extra
    from unnest(enum_range(null::job_status)) a,
         unnest(enum_range(null::job_status)) b
   where fl.legal_transition(a, b)
     and not exists (select 1 from job_transitions where from_status = a and to_status = b);

  select count(*) into missing
    from job_transitions t where not fl.legal_transition(t.from_status, t.to_status);

  if extra > 0 or missing > 0 then
    raise exception 'transition function and table disagree: % extra, % missing', extra, missing;
  end if;
end
$$;

create or replace function jobs_fsm() returns trigger language plpgsql as $$
begin
  if new.fence < old.fence then
    raise exception 'fence went backwards on job %: % -> %', old.id, old.fence, new.fence;
  end if;

  if old.status is distinct from new.status
     and not fl.legal_transition(old.status, new.status) then
    raise exception 'illegal transition % -> % on job %', old.status, new.status, old.id;
  end if;

  return new;
end
$$;
