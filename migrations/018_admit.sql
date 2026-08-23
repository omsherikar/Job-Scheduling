create function fl.queue_admit(q uuid, want int) returns int
language plpgsql as $$
declare
  n int;
begin
  select least(
           want,
           greatest(max_concurrency - in_flight, 0),
           case when rl_limit_per_sec = 0 then want
                else floor(least(rl_burst,
                       rl_tokens + extract(epoch from (fl.now() - rl_refilled_at)) * rl_limit_per_sec))::int
           end,
           case when breaker_state = 'closed' then want else 1 end
         )
    into n
    from queues
   where id = q
     and not paused
     and (breaker_state = 'closed'
          or (breaker_state = 'half_open' and breaker_probe_budget > 0)
          or (breaker_state = 'open' and coalesce(breaker_open_until, fl.now()) <= fl.now()))
   for no key update;

  if n is null or n <= 0 then
    return 0;
  end if;

  update queues set
    in_flight = in_flight + n,
    rl_tokens = case when rl_limit_per_sec = 0 then rl_tokens
                     else least(rl_burst,
                                rl_tokens + extract(epoch from (fl.now() - rl_refilled_at)) * rl_limit_per_sec)
                          - n
                end,
    rl_refilled_at = case when rl_limit_per_sec = 0 then rl_refilled_at else fl.now() end,
    breaker_state = case when breaker_state = 'open'
                          and coalesce(breaker_open_until, fl.now()) <= fl.now()
                         then 'half_open' else breaker_state end,
    breaker_probe_budget = case
      when breaker_state = 'open' and coalesce(breaker_open_until, fl.now()) <= fl.now() then 1 - n
      when breaker_state = 'half_open' then breaker_probe_budget - n
      else breaker_probe_budget
    end
   where id = q;

  return n;
end
$$;

create or replace function fl.reconcile_in_flight() returns int
language plpgsql as $$
declare
  r     record;
  real  int;
  fixed int := 0;
begin
  for r in select id from queues order by id loop
    perform 1 from queues where id = r.id for no key update;

    select count(*) into real
      from jobs where queue_id = r.id and status in ('claimed', 'running');

    update queues set in_flight = real where id = r.id and in_flight <> real;
    if found then
      fixed := fixed + 1;
    end if;
  end loop;
  return fixed;
end
$$;
