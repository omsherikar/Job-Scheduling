create or replace function fl.spread() returns trigger
language plpgsql as $$
begin
  if tg_op = 'UPDATE' and new.shards <> old.shards then
    if exists (select 1 from queue_shards where queue_id = new.id and in_flight > 0)
       or exists (select 1 from jobs
                   where queue_id = new.id and status in ('claimed', 'running')) then
      raise exception 'queue still holds work' using errcode = '55006';
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
