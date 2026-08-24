alter table queues add column shards smallint not null default 1
  check (shards between 1 and 64);

create table queue_shards (
  queue_id       uuid not null references queues(id) on delete cascade,
  shard          smallint not null check (shard >= 0),
  in_flight      int not null default 0 check (in_flight >= 0),
  rl_tokens      numeric not null default 0 check (rl_tokens >= 0),
  rl_refilled_at timestamptz not null default fl.now(),
  primary key (queue_id, shard)
);

insert into queue_shards (queue_id, shard, in_flight, rl_tokens, rl_refilled_at)
select id, 0, in_flight, rl_tokens, rl_refilled_at from queues;

alter table jobs add column shard smallint not null default 0;

drop index idx_jobs_inflight;
create index idx_jobs_inflight on jobs (queue_id, shard)
  where status in ('claimed', 'running');

alter table queues drop column in_flight;
alter table queues drop column rl_tokens;
alter table queues drop column rl_refilled_at;

create function fl.in_flight(q uuid) returns int
language sql stable as $$
  select coalesce(sum(in_flight), 0)::int from queue_shards where queue_id = q
$$;

create function fl.shard_slots(q uuid, s smallint) returns int
language sql stable as $$
  select max_concurrency / shards + case when s < max_concurrency % shards then 1 else 0 end
    from queues where id = q
$$;

create function fl.spread() returns trigger
language plpgsql as $$
begin
  if tg_op = 'UPDATE' and new.shards < old.shards then
    if exists (select 1 from queue_shards
                where queue_id = new.id and shard >= new.shards and in_flight > 0)
       or exists (select 1 from jobs
                   where queue_id = new.id and shard >= new.shards
                     and status in ('claimed', 'running')) then
      raise exception 'shards still hold work' using errcode = '55006';
    end if;
    delete from queue_shards where queue_id = new.id and shard >= new.shards;
  end if;

  insert into queue_shards (queue_id, shard, rl_tokens, rl_refilled_at)
  select new.id, g, new.rl_burst / new.shards, fl.now()
    from generate_series(0, new.shards - 1) g
  on conflict (queue_id, shard) do nothing;

  update queue_shards set rl_tokens = least(rl_tokens, new.rl_burst / new.shards)
   where queue_id = new.id;

  return null;
end
$$;

create trigger queues_spread after insert or update of shards, rl_burst on queues
  for each row execute function fl.spread();

drop function fl.queue_release(uuid, int);

create function fl.queue_release(q uuid, s smallint, n int) returns void
language sql volatile as $$
  update queue_shards set in_flight = greatest(in_flight - n, 0)
   where queue_id = q and shard = s
$$;

drop function fl.queue_admit(uuid, int);

create function fl.queue_admit(q uuid, want int, out slots int, out sh smallint)
language plpgsql as $$
declare
  qr  record;
  br  record;
  sr  record;
  cap int;
  rot int := pg_backend_pid();
begin
  slots := 0;
  sh := 0;

  select shards, paused, max_concurrency, rl_limit_per_sec, rl_burst, breaker_state
    into qr
    from queues where id = q;

  if not found or qr.paused then
    return;
  end if;

  if qr.breaker_state <> 'closed' then
    select breaker_state, breaker_open_until, breaker_probe_budget into br
      from queues where id = q for no key update;

    if br.breaker_state = 'open' then
      if coalesce(br.breaker_open_until, fl.now()) > fl.now() then
        return;
      end if;
      update queues set breaker_state = 'half_open', breaker_probe_budget = 1 where id = q;
      br.breaker_probe_budget := 1;
    end if;

    if br.breaker_probe_budget <= 0 then
      return;
    end if;
    want := 1;
  end if;

  select s.shard into sh
    from queue_shards s
   where s.queue_id = q
     and s.in_flight < qr.max_concurrency / qr.shards
                       + case when s.shard < qr.max_concurrency % qr.shards then 1 else 0 end
   order by (s.shard + rot) % qr.shards
   limit 1;

  if not found then
    sh := 0;
    return;
  end if;

  select s.in_flight, s.rl_tokens, s.rl_refilled_at into sr
    from queue_shards s
   where s.queue_id = q and s.shard = sh
   for no key update;

  cap := qr.max_concurrency / qr.shards
         + case when sh < qr.max_concurrency % qr.shards then 1 else 0 end;

  slots := least(
    want,
    greatest(cap - sr.in_flight, 0),
    case when qr.rl_limit_per_sec = 0 then want
         else floor(least(qr.rl_burst / qr.shards,
                sr.rl_tokens + extract(epoch from (fl.now() - sr.rl_refilled_at))
                  * (qr.rl_limit_per_sec / qr.shards)))::int
    end);

  if slots is null or slots <= 0 then
    slots := 0;
    return;
  end if;

  update queue_shards s set
    in_flight = s.in_flight + slots,
    rl_tokens = case when qr.rl_limit_per_sec = 0 then s.rl_tokens
                     else least(qr.rl_burst / qr.shards,
                                s.rl_tokens + extract(epoch from (fl.now() - s.rl_refilled_at))
                                  * (qr.rl_limit_per_sec / qr.shards)) - slots
                end,
    rl_refilled_at = case when qr.rl_limit_per_sec = 0 then s.rl_refilled_at else fl.now() end
   where s.queue_id = q and s.shard = sh;

  if qr.breaker_state <> 'closed' then
    update queues set breaker_probe_budget = breaker_probe_budget - slots where id = q;
  end if;
end
$$;

create or replace function fl.reconcile_in_flight() returns int
language plpgsql as $$
declare
  r     record;
  real  int;
  fixed int := 0;
begin
  for r in select queue_id, shard from queue_shards order by queue_id, shard loop
    perform 1 from queue_shards
     where queue_id = r.queue_id and shard = r.shard for no key update;

    select count(*) into real from jobs
     where queue_id = r.queue_id and shard = r.shard and status in ('claimed', 'running');

    update queue_shards set in_flight = real
     where queue_id = r.queue_id and shard = r.shard and in_flight <> real;
    if found then
      fixed := fixed + 1;
    end if;
  end loop;
  return fixed;
end
$$;
