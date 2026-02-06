-- Migration: align price_history_unified with db_cars_scrap source columns
-- Target DB: DB_NAME (e.g. db_fastapi_scrap_new)
--
-- Changes:
-- - Add listing_id (TEXT)
-- - Ensure changed_at is TIMESTAMPTZ
--
-- Notes:
-- - Re-runnable (IF NOT EXISTS) for the column add.

BEGIN;

ALTER TABLE IF EXISTS public.price_history_unified
  ADD COLUMN IF NOT EXISTS listing_id TEXT NULL;

ALTER TABLE IF EXISTS public.price_history_unified
  ALTER COLUMN changed_at TYPE TIMESTAMPTZ;

COMMIT;

