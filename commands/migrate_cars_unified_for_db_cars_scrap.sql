-- Migration: align cars_unified with db_cars_scrap source columns
-- Target DB: DB_NAME (e.g. db_fastapi_scrap_new)
--
-- Changes:
-- - Drop legacy columns sourced from old db_scrap_new: model_group, ads_tag
-- - Add new source columns from db_cars_scrap: listing_id, series, type, created_at
--
-- Notes:
-- - Uses IF EXISTS / IF NOT EXISTS to be re-runnable.
-- - Review before running in production.

BEGIN;

ALTER TABLE IF EXISTS public.cars_unified
  ADD COLUMN IF NOT EXISTS listing_id TEXT NULL;

ALTER TABLE IF EXISTS public.cars_unified
  ADD COLUMN IF NOT EXISTS series VARCHAR(100) NULL;

ALTER TABLE IF EXISTS public.cars_unified
  ADD COLUMN IF NOT EXISTS type VARCHAR(100) NULL;

ALTER TABLE IF EXISTS public.cars_unified
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NULL;

-- Align images with db_cars_scrap (TEXT[]).
-- If images was previously a TEXT column, this coerces existing values into a
-- single-element array; review if you need a smarter conversion.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'cars_unified'
      AND column_name = 'images'
  ) THEN
    EXECUTE $sql$
      ALTER TABLE public.cars_unified
        ALTER COLUMN images TYPE TEXT[]
        USING CASE
          WHEN images IS NULL THEN NULL
          ELSE ARRAY[images::text]
        END
    $sql$;
  END IF;
END $$;

-- Legacy columns no longer present in db_cars_scrap source tables
ALTER TABLE IF EXISTS public.cars_unified
  DROP COLUMN IF EXISTS model_group;

ALTER TABLE IF EXISTS public.cars_unified
  DROP COLUMN IF EXISTS ads_tag;

COMMIT;
