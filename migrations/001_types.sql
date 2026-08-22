create type job_status as enum (
  'scheduled', 'queued', 'claimed', 'running',
  'completed', 'retry_wait', 'dead_letter', 'cancelled'
);

create type exec_outcome as enum (
  'success', 'retryable_error', 'permanent_error',
  'timeout', 'lost', 'fenced', 'cancelled'
);

create type backoff_kind as enum ('fixed', 'linear', 'exponential');

create type breaker_state as enum ('closed', 'open', 'half_open');

create schema fl;

create function fl.now() returns timestamptz language sql stable as $$
  select coalesce(
    case when current_setting('fl.testing', true) = 'on'
         then nullif(current_setting('fl.now', true), '')::timestamptz end,
    clock_timestamp())
$$;

create function fl.uuidv7() returns uuid language sql volatile as $$
  select encode(
    set_bit(
      set_bit(
        overlay(uuid_send(gen_random_uuid())
                placing substring(int8send(
                  floor(extract(epoch from clock_timestamp()) * 1000)::bigint) from 3)
                from 1 for 6),
        52, 1),
      53, 1), 'hex')::uuid
$$;
