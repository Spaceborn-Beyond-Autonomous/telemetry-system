"""
TimescaleDB connection and table management for the raw-writer service.

Provides:
* Async connection pool via ``asyncpg``
* ``telemetry_raw`` hypertable creation (idempotent)
* Batch insert with ``ON CONFLICT DO NOTHING`` for idempotency
* Graceful reconnect and pool lifecycle
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Sequence

import asyncpg

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from shared.utils.logger import get_logger

if TYPE_CHECKING:
    from app.config import Settings

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# SQL Statements
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS telemetry_raw (
    -- Provenance / routing
    ingestion_id   TEXT        NOT NULL,
    mission_id     TEXT        NOT NULL,
    drone_profile  TEXT        NOT NULL,
    schema_version TEXT        NOT NULL,
    exported_at    TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL,

    -- Timing
    t              DOUBLE PRECISION NOT NULL,

    -- 3-D Position
    px             DOUBLE PRECISION NOT NULL,
    py             DOUBLE PRECISION NOT NULL,
    pz             DOUBLE PRECISION NOT NULL,

    -- Orientation
    roll           DOUBLE PRECISION NOT NULL,
    pitch          DOUBLE PRECISION NOT NULL,
    yaw            DOUBLE PRECISION NOT NULL,

    -- Gyroscope
    gx             DOUBLE PRECISION NOT NULL,
    gy             DOUBLE PRECISION NOT NULL,
    gz             DOUBLE PRECISION NOT NULL,

    -- Accelerometer
    acc_x          DOUBLE PRECISION NOT NULL,
    acc_y          DOUBLE PRECISION NOT NULL,
    acc_z          DOUBLE PRECISION NOT NULL,

    -- Velocity
    vx             DOUBLE PRECISION NOT NULL,
    vy             DOUBLE PRECISION NOT NULL,
    vz             DOUBLE PRECISION NOT NULL,

    -- Motor commands
    m0             DOUBLE PRECISION NOT NULL,
    m1             DOUBLE PRECISION NOT NULL,
    m2             DOUBLE PRECISION NOT NULL,
    m3             DOUBLE PRECISION NOT NULL,

    -- Motor RPM
    rpm0           DOUBLE PRECISION NOT NULL,
    rpm1           DOUBLE PRECISION NOT NULL,
    rpm2           DOUBLE PRECISION NOT NULL,
    rpm3           DOUBLE PRECISION NOT NULL,

    -- Battery
    batt           DOUBLE PRECISION NOT NULL,
    curr           DOUBLE PRECISION NOT NULL,
    batt_pct       DOUBLE PRECISION NOT NULL,

    -- Barometer
    baro_raw       DOUBLE PRECISION NOT NULL,
    baro_filtered  DOUBLE PRECISION NOT NULL,

    -- Wind
    wind_x         DOUBLE PRECISION NOT NULL,
    wind_z         DOUBLE PRECISION NOT NULL,

    -- Dryden turbulence
    dryden_x       DOUBLE PRECISION NOT NULL,
    dryden_y       DOUBLE PRECISION NOT NULL,
    dryden_z       DOUBLE PRECISION NOT NULL,

    -- GPS
    gps_lat        BIGINT           NOT NULL,
    gps_lon        BIGINT           NOT NULL,
    gps_fix        INTEGER          NOT NULL,
    gps_sat        INTEGER          NOT NULL,
    gps_eph        INTEGER          NOT NULL,
    gps_epv        INTEGER          NOT NULL,

    -- Obstacle sensors
    obs_fwd        DOUBLE PRECISION NOT NULL,
    obs_right      DOUBLE PRECISION NOT NULL,
    obs_back       DOUBLE PRECISION NOT NULL,
    obs_left       DOUBLE PRECISION NOT NULL,
    obs_up         DOUBLE PRECISION NOT NULL,

    -- Flight controller state
    mode           TEXT             NOT NULL,
    armed          BOOLEAN          NOT NULL,
    crashed        SMALLINT         NOT NULL,
    grounded       SMALLINT         NOT NULL,
    ground_y       DOUBLE PRECISION NOT NULL,

    -- Motor damage
    dmg0           DOUBLE PRECISION NOT NULL,
    dmg1           DOUBLE PRECISION NOT NULL,
    dmg2           DOUBLE PRECISION NOT NULL,
    dmg3           DOUBLE PRECISION NOT NULL,

    -- Pilot inputs
    input_throttle DOUBLE PRECISION NOT NULL,
    input_pitch    DOUBLE PRECISION NOT NULL,
    input_roll     DOUBLE PRECISION NOT NULL,
    input_yaw      DOUBLE PRECISION NOT NULL,

    -- PID controller
    pid_roll_err   DOUBLE PRECISION NOT NULL,
    pid_roll_out   DOUBLE PRECISION NOT NULL,
    pid_pitch_err  DOUBLE PRECISION NOT NULL,
    pid_pitch_out  DOUBLE PRECISION NOT NULL,
    pid_yaw_err    DOUBLE PRECISION NOT NULL,
    pid_yaw_out    DOUBLE PRECISION NOT NULL,
    pid_alt_err    DOUBLE PRECISION NOT NULL,
    pid_alt_out    DOUBLE PRECISION NOT NULL,

    -- Constraints
    PRIMARY KEY (ingestion_id)
);
"""

CREATE_HYPERTABLE_SQL = """
SELECT create_hypertable(
    'telemetry_raw',
    by_range('ingested_at'),
    if_not_exists => TRUE
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_telemetry_raw_mission ON telemetry_raw (mission_id, ingested_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_raw_drone ON telemetry_raw (drone_profile, ingested_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_raw_time ON telemetry_raw (mission_id, t);",
]

# The 72-field ordered column list for batch inserts
COLUMNS = [
    "ingestion_id", "mission_id", "drone_profile", "schema_version",
    "exported_at", "ingested_at",
    "t",
    "px", "py", "pz",
    "roll", "pitch", "yaw",
    "gx", "gy", "gz",
    "acc_x", "acc_y", "acc_z",
    "vx", "vy", "vz",
    "m0", "m1", "m2", "m3",
    "rpm0", "rpm1", "rpm2", "rpm3",
    "batt", "curr", "batt_pct",
    "baro_raw", "baro_filtered",
    "wind_x", "wind_z",
    "dryden_x", "dryden_y", "dryden_z",
    "gps_lat", "gps_lon", "gps_fix", "gps_sat", "gps_eph", "gps_epv",
    "obs_fwd", "obs_right", "obs_back", "obs_left", "obs_up",
    "mode", "armed", "crashed", "grounded", "ground_y",
    "dmg0", "dmg1", "dmg2", "dmg3",
    "input_throttle", "input_pitch", "input_roll", "input_yaw",
    "pid_roll_err", "pid_roll_out",
    "pid_pitch_err", "pid_pitch_out",
    "pid_yaw_err", "pid_yaw_out",
    "pid_alt_err", "pid_alt_out",
]

# Build parameterised INSERT with ON CONFLICT DO NOTHING for idempotency
_placeholders = ", ".join(f"${i+1}" for i in range(len(COLUMNS)))
_col_list = ", ".join(COLUMNS)
INSERT_BATCH_SQL = (
    f"INSERT INTO telemetry_raw ({_col_list}) "
    f"VALUES ({_placeholders}) "
    f"ON CONFLICT (ingestion_id) DO NOTHING"
)


# ---------------------------------------------------------------------------
# Database Manager
# ---------------------------------------------------------------------------

class DatabaseManager:
    """
    Async TimescaleDB connection manager with pool lifecycle.

    Usage::

        db = DatabaseManager(settings)
        await db.connect()
        await db.insert_batch(records)
        await db.close()
    """

    def __init__(self, settings: "Settings") -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Create the connection pool and ensure the schema exists.

        Raises ``RuntimeError`` if the pool is already initialised.
        """
        if self._pool is not None:
            raise RuntimeError("Database pool is already initialised")

        logger.info(
            "db_connecting",
            host=self._settings.db_host,
            port=self._settings.db_port,
            database=self._settings.db_name,
            pool_min=self._settings.db_pool_min_size,
            pool_max=self._settings.db_pool_max_size,
        )

        self._pool = await asyncpg.create_pool(
            host=self._settings.db_host,
            port=self._settings.db_port,
            database=self._settings.db_name,
            user=self._settings.db_user,
            password=self._settings.db_password,
            min_size=self._settings.db_pool_min_size,
            max_size=self._settings.db_pool_max_size,
            command_timeout=self._settings.db_command_timeout,
            ssl=self._settings.db_ssl if self._settings.db_ssl else None,
        )

        logger.info("db_connected")

        # Ensure schema exists
        await self._ensure_schema()

    async def close(self) -> None:
        """Drain and close all connections in the pool."""
        if self._pool is not None:
            logger.info("db_closing")
            await self._pool.close()
            self._pool = None
            logger.info("db_closed")

    @property
    def is_connected(self) -> bool:
        """Return ``True`` if the pool is initialised."""
        return self._pool is not None

    # ------------------------------------------------------------------
    # Schema Management
    # ------------------------------------------------------------------

    async def _ensure_schema(self) -> None:
        """
        Create the ``telemetry_raw`` table and TimescaleDB hypertable
        if they do not already exist.  Fully idempotent.
        """
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            # Create base table
            await conn.execute(CREATE_TABLE_SQL)
            logger.info("db_table_ensured", table="telemetry_raw")

            # Convert to hypertable (idempotent via if_not_exists)
            try:
                await conn.execute(CREATE_HYPERTABLE_SQL)
                logger.info("db_hypertable_ensured", table="telemetry_raw")
            except asyncpg.UndefinedFunctionError:
                # TimescaleDB extension not installed — run as plain Postgres
                logger.warning(
                    "db_timescaledb_not_available",
                    msg="create_hypertable not found; running as plain PostgreSQL",
                )

            # Create indexes
            for idx_sql in CREATE_INDEXES_SQL:
                await conn.execute(idx_sql)
            logger.info("db_indexes_ensured", count=len(CREATE_INDEXES_SQL))

    # ------------------------------------------------------------------
    # Batch Insert
    # ------------------------------------------------------------------

    async def insert_batch(
        self, records: Sequence[tuple[Any, ...]]
    ) -> int:
        """
        Insert a batch of telemetry records into ``telemetry_raw``.

        Uses ``executemany`` within a transaction for atomicity.
        ``ON CONFLICT (ingestion_id) DO NOTHING`` ensures idempotency —
        duplicate ingestion IDs are silently skipped.

        Parameters
        ----------
        records:
            Sequence of tuples, each with values in ``COLUMNS`` order.

        Returns
        -------
        int
            Number of records in the batch (duplicates are silently skipped).
        """
        if self._pool is None:
            raise RuntimeError("Database pool is not initialised — call connect() first")

        if not records:
            return 0

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(INSERT_BATCH_SQL, records)

        logger.debug(
            "db_batch_inserted",
            batch_size=len(records),
        )
        return len(records)

    # ------------------------------------------------------------------
    # Health Check
    # ------------------------------------------------------------------

    async def health_check(self) -> bool:
        """Run a simple ``SELECT 1`` to verify database connectivity."""
        if self._pool is None:
            return False
        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception:
            logger.exception("db_health_check_failed")
            return False
