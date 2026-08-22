create function fl.backoff(kind backoff_kind, base_ms int, max_ms int,
                           jitter boolean, attempt int)
returns interval language sql volatile as $$
  select make_interval(secs => (
    case when jitter and coalesce(current_setting('fl.jitter', true), 'on') <> 'off'
         then random() * d else d end) / 1000.0)
  from (
    select least(max_ms::numeric, base_ms::numeric * case kind
      when 'fixed'       then 1
      when 'linear'      then greatest(attempt, 1)
      when 'exponential' then power(2, greatest(attempt - 1, 0))
    end) as d
  ) x
$$;

create function fl.queue_release(q uuid, n int) returns void
language sql volatile as $$
  update queues set in_flight = greatest(in_flight - n, 0) where id = q
$$;

create function fl.reconcile_in_flight() returns int
language plpgsql as $$
declare
  fixed int;
begin
  with real as (
    select q.id, count(j.id) as n
      from queues q
      left join jobs j on j.queue_id = q.id and j.status in ('claimed', 'running')
     group by q.id
  )
  update queues q set in_flight = real.n
    from real
   where q.id = real.id and q.in_flight <> real.n;
  get diagnostics fixed = row_count;
  return fixed;
end
$$;
