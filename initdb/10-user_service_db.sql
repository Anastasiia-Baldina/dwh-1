\connect user_service_db;

CREATE TABLE IF NOT EXISTS public.users (
  user_id           serial PRIMARY KEY,
  user_external_id  uuid UNIQUE,
  email             varchar,
  first_name        varchar,
  last_name         varchar,
  phone             varchar,
  date_of_birth     date,
  registration_date timestamp,
  status            varchar,
  effective_from    timestamp,
  effective_to      timestamp,
  is_current        boolean,
  created_at        timestamp,
  updated_at        timestamp,
  created_by        varchar,
  updated_by        varchar
);

CREATE TABLE IF NOT EXISTS public.user_addresses (
  address_id          serial PRIMARY KEY,
  address_external_id uuid UNIQUE,
  user_external_id    uuid,
  address_type        varchar,
  country             varchar,
  region              varchar,
  city                varchar,
  street_address      varchar,
  postal_code         varchar,
  apartment           varchar,
  is_default          boolean,
  effective_from      timestamp,
  effective_to        timestamp,
  is_current          boolean,
  created_at          timestamp,
  updated_at          timestamp,
  created_by          varchar,
  updated_by          varchar,
  CONSTRAINT fk_user_addresses_user_external_id
    FOREIGN KEY (user_external_id) REFERENCES public.users(user_external_id)
);

CREATE TABLE IF NOT EXISTS public.user_status_history (
  history_id      serial PRIMARY KEY,
  user_external_id uuid,
  old_status      varchar,
  new_status      varchar,
  change_reason   varchar,
  changed_at      timestamp,
  changed_by      varchar,
  session_id      varchar,
  ip_address      inet,
  user_agent      text,
  CONSTRAINT fk_user_status_history_user_external_id
    FOREIGN KEY (user_external_id) REFERENCES public.users(user_external_id)
);