\connect logistics_service_db;

CREATE TABLE IF NOT EXISTS public.warehouses (
  warehouse_id              serial PRIMARY KEY,
  warehouse_code            varchar UNIQUE,
  warehouse_name            varchar,
  warehouse_type            varchar,
  country                   varchar,
  region                    varchar,
  city                      varchar,
  street_address            varchar,
  postal_code               varchar,
  is_active                 boolean,
  max_capacity_cubic_meters decimal,
  operating_hours           varchar,
  contact_phone             varchar,
  manager_name              varchar,
  effective_from            timestamp,
  effective_to              timestamp,
  is_current                boolean,
  created_at                timestamp,
  updated_at                timestamp,
  created_by                varchar,
  updated_by                varchar
);

CREATE TABLE IF NOT EXISTS public.pickup_points (
  pickup_point_id         serial PRIMARY KEY,
  pickup_point_code       varchar UNIQUE,
  pickup_point_name       varchar,
  pickup_point_type       varchar,
  country                 varchar,
  region                  varchar,
  city                    varchar,
  street_address          varchar,
  postal_code             varchar,
  is_active               boolean,
  max_capacity_packages   integer,
  operating_hours         varchar,
  contact_phone           varchar,
  partner_name            varchar,
  effective_from          timestamp,
  effective_to            timestamp,
  is_current              boolean,
  created_at              timestamp,
  updated_at              timestamp,
  created_by              varchar,
  updated_by              varchar
);

CREATE TABLE IF NOT EXISTS public.shipments (
  shipment_id                    serial PRIMARY KEY,
  shipment_external_id           uuid UNIQUE,
  order_external_id              uuid,          -- cross-db (FK НЕ делаем)
  tracking_number                varchar UNIQUE,
  status                         varchar,
  weight_grams                   integer,
  volume_cubic_cm                integer,
  package_count                  integer,
  origin_warehouse_code          varchar,
  destination_type               varchar,
  destination_pickup_point_code  varchar,
  destination_address_external_id uuid,         -- cross-db (FK НЕ делаем)
  created_date                   timestamp,
  dispatched_date                timestamp,
  estimated_delivery_date        timestamp,
  actual_delivery_date           timestamp,
  delivery_notes                 text,
  recipient_name                 varchar,
  delivery_signature             varchar,
  effective_from                 timestamp,
  effective_to                   timestamp,
  is_current                     boolean,
  created_at                     timestamp,
  updated_at                     timestamp,
  created_by                     varchar,
  updated_by                     varchar,
  CONSTRAINT fk_shipments_origin_warehouse_code
    FOREIGN KEY (origin_warehouse_code) REFERENCES public.warehouses(warehouse_code),
  CONSTRAINT fk_shipments_destination_pickup_point_code
    FOREIGN KEY (destination_pickup_point_code) REFERENCES public.pickup_points(pickup_point_code)
);

CREATE TABLE IF NOT EXISTS public.shipment_movements (
  movement_id        serial PRIMARY KEY,
  shipment_external_id uuid,
  movement_type      varchar,
  location_type      varchar,
  location_code      varchar,
  movement_datetime  timestamp,
  operator_name      varchar,
  notes              text,
  latitude           decimal,
  longitude          decimal,
  created_at         timestamp,
  created_by         varchar,
  CONSTRAINT fk_shipment_movements_shipment_external_id
    FOREIGN KEY (shipment_external_id) REFERENCES public.shipments(shipment_external_id)
);

CREATE TABLE IF NOT EXISTS public.shipment_status_history (
  history_id         serial PRIMARY KEY,
  shipment_external_id uuid,
  old_status         varchar,
  new_status         varchar,
  change_reason      varchar,
  changed_at         timestamp,
  changed_by         varchar,
  location_type      varchar,
  location_code      varchar,
  notes              text,
  customer_notified  boolean,
  CONSTRAINT fk_shipment_status_history_shipment_external_id
    FOREIGN KEY (shipment_external_id) REFERENCES public.shipments(shipment_external_id)
);