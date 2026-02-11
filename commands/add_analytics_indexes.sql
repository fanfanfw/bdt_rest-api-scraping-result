-- Index suggestions for speeding up /analytics/price_vs_mileage
-- Apply on the same database used by FastAPI (e.g. db_fastapi_scrap).
--
-- Notes:
-- - Use CONCURRENTLY to avoid blocking writes (but it can take longer).
-- - Run during low-traffic window if possible.
-- - After creating indexes, run ANALYZE to refresh planner stats.

-- 1) Speed up joins to cars_standard (filters are on cs.brand_norm/cs.model_norm/cs.variant_norm)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_unified_cars_standard_id
    ON public.cars_unified (cars_standard_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_carsome_cars_standard_id
    ON public.carsome (cars_standard_id);

-- 2) Speed up ordering + pagination by ads_date (information_ads_date) for unified sources
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_unified_std_src_year_ads_date_desc
    ON public.cars_unified (cars_standard_id, year, source, information_ads_date DESC, id DESC)
    WHERE status IN ('active', 'sold');

-- 3) Speed up ordering + pagination by scraped_at (last_scraped_at) for unified sources
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_unified_std_src_year_last_scraped_desc
    ON public.cars_unified (cars_standard_id, year, source, last_scraped_at DESC, id DESC)
    WHERE status IN ('active', 'sold');

-- 4) Carsome ordering indexes (choose one/both depending on which sort you use most)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_carsome_std_year_last_updated_desc
    ON public.carsome (cars_standard_id, year, last_updated_at DESC, id DESC)
    WHERE status IN ('active', 'sold');

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_carsome_std_year_created_desc
    ON public.carsome (cars_standard_id, year, created_at DESC, id DESC)
    WHERE status IN ('active', 'sold');

ANALYZE public.cars_unified;
ANALYZE public.carsome;
ANALYZE public.cars_standard;
