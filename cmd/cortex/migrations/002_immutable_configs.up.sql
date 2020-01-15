-- NB: Incompatible schema change. Normally we would follow a more incremental
-- process (as exemplified in users/db/migrations/006 to 010), but currently
-- there are no data in production and only one row in dev.

-- https://github.com/mattes/migrate/tree/master/database/postgres#upgrading-from-v1
-- Wrap all commands in BEGIN and COMMIT to accommodate upgrade
BEGIN;

-- The existing id, type columns are the id & type of the entity that owns the
-- config.
ALTER TABLE configs RENAME COLUMN id TO owner_id;
ALTER TABLE configs RENAME COLUMN type TO owner_type;

-- Add a new auto-incrementing id.
ALTER TABLE configs ADD COLUMN id SERIAL;

ALTER TABLE configs DROP CONSTRAINT configs_pkey;
ALTER TABLE configs ADD PRIMARY KEY (id, owner_id, owner_type, subsystem);

COMMIT;
