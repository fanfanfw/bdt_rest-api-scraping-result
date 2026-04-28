-- Migration: add MudahMY-specific normalize prediction reference
-- Target database: db_fastapi_scrap

CREATE TABLE IF NOT EXISTS public.normalize_predict_mudahmy (
    id BIGINT PRIMARY KEY,
    brand_norm VARCHAR(100) NOT NULL,
    brand_raw VARCHAR(100) NULL,
    brand_raw2 VARCHAR(100) NULL,
    model_norm VARCHAR(100) NOT NULL,
    model_raw VARCHAR(100) NULL,
    model_raw2 VARCHAR(100) NULL,
    model_raw3 VARCHAR(100) NULL,
    variant_norm VARCHAR(100) NOT NULL,
    variant_raw VARCHAR(100) NULL,
    variant_raw2 VARCHAR(100) NULL,
    variant_raw3 VARCHAR(100) NULL,
    variant_raw4 VARCHAR(100) NULL
);

COMMENT ON TABLE public.normalize_predict_mudahmy IS
    'MudahMY-specific reference table for normalize prediction mapping';

ALTER TABLE IF EXISTS public.cars_unified
    ADD COLUMN IF NOT EXISTS normalize_predict_mudahmy_id BIGINT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'normalize_predict_mudahmy'
          AND column_name = 'model_nowm'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'normalize_predict_mudahmy'
          AND column_name = 'model_norm'
    ) THEN
        ALTER TABLE public.normalize_predict_mudahmy
            ADD COLUMN model_norm VARCHAR(100);

        UPDATE public.normalize_predict_mudahmy
        SET model_norm = model_nowm
        WHERE model_norm IS NULL;

        ALTER TABLE public.normalize_predict_mudahmy
            ALTER COLUMN model_norm SET NOT NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_cars_unified_normalize_predict_mudahmy'
    ) THEN
        ALTER TABLE public.cars_unified
            ADD CONSTRAINT fk_cars_unified_normalize_predict_mudahmy
            FOREIGN KEY (normalize_predict_mudahmy_id)
            REFERENCES public.normalize_predict_mudahmy(id)
            ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_cars_unified_normalize_predict_mudahmy_id
    ON public.cars_unified (normalize_predict_mudahmy_id);

CREATE INDEX IF NOT EXISTS idx_cars_unified_mudahmy_normalize_predict_null
    ON public.cars_unified (id)
    WHERE source = 'mudahmy'
      AND normalize_predict_mudahmy_id IS NULL
      AND brand IS NOT NULL
      AND model IS NOT NULL
      AND variant IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_normalize_predict_mudahmy_brand_norm_upper
    ON public.normalize_predict_mudahmy (UPPER(TRIM(brand_norm)));

CREATE INDEX IF NOT EXISTS idx_normalize_predict_mudahmy_brand_raw_upper
    ON public.normalize_predict_mudahmy (UPPER(TRIM(brand_raw)));

CREATE INDEX IF NOT EXISTS idx_normalize_predict_mudahmy_brand_raw2_upper
    ON public.normalize_predict_mudahmy (UPPER(TRIM(brand_raw2)));

CREATE INDEX IF NOT EXISTS idx_normalize_predict_mudahmy_norm_combo
    ON public.normalize_predict_mudahmy (brand_norm, model_norm, variant_norm);

ANALYZE public.normalize_predict_mudahmy;
ANALYZE public.cars_unified;
