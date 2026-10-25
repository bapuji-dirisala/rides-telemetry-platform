-- Grants: platform engineers own bronze+silver; analysts read gold only.

GRANT USE CATALOG ON CATALOG rides_dev TO `platform_engineers`;
GRANT USE CATALOG ON CATALOG rides_dev TO `rides_analysts`;

GRANT ALL PRIVILEGES ON SCHEMA rides_dev.bronze TO `platform_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA rides_dev.silver TO `platform_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA rides_dev.gold   TO `platform_engineers`;

GRANT USE SCHEMA ON SCHEMA rides_dev.gold TO `rides_analysts`;
GRANT SELECT ON SCHEMA rides_dev.gold TO `rides_analysts`;

-- Row-filter: analysts only see completed, non-fraud trips.
CREATE OR REPLACE FUNCTION rides_dev.gold.rows_safe_for_analysts(trip_status STRING, fraud_flag BOOLEAN)
    RETURNS BOOLEAN
    RETURN trip_status = 'completed' AND NOT fraud_flag;
