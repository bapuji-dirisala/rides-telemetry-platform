-- Unity Catalog bootstrap for the rides telemetry lakehouse.
-- Three-level namespace: <catalog>.<schema>.<table>.
-- One catalog per environment; schemas mirror the medallion.

CREATE CATALOG IF NOT EXISTS rides_dev
    MANAGED LOCATION 's3://rides-dev-lakehouse/warehouse/_managed';

CREATE SCHEMA IF NOT EXISTS rides_dev.bronze
    MANAGED LOCATION 's3://rides-dev-lakehouse/warehouse/bronze';

CREATE SCHEMA IF NOT EXISTS rides_dev.silver
    MANAGED LOCATION 's3://rides-dev-lakehouse/warehouse/silver';

CREATE SCHEMA IF NOT EXISTS rides_dev.gold
    MANAGED LOCATION 's3://rides-dev-lakehouse/warehouse/gold';

ALTER CATALOG rides_dev SET TAGS ('env' = 'dev', 'owner' = 'rides-platform');
