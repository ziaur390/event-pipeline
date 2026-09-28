"""Oracle XE as the legacy reference-data source.

The NADRA-style environment keeps reference data in an Oracle database alongside
modern stores. This connector reads device-to-zone mappings from Oracle. It is
optional: without ORACLE_DSN the consumer uses simulated lookups, and Redis
caches the result either way so Oracle is hit at most once per CACHE_TTL.
"""

import logging
import os

log = logging.getLogger(__name__)


def lookup_zone_from_oracle(device_id):
    """Read zone_name for a device from the legacy Oracle source, or None."""
    import oracledb      # imported lazily so the dep is only needed when used

    with oracledb.connect(
        user=os.getenv("ORACLE_USER", "pipeline"),
        password=os.getenv("ORACLE_PASSWORD", "pipeline"),
        dsn=os.environ["ORACLE_DSN"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zone_name FROM reference_devices WHERE device_id = :1",
                [device_id],
            )
            row = cur.fetchone()
            return row[0] if row else None
